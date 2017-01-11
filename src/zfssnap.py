#!/usr/bin/env python3

import argparse
import logging
import sys
import subprocess
import re
import os
from datetime import datetime
from operator import attrgetter
import fcntl
import time
import fnmatch
from distutils.version import StrictVersion
import hashlib
import json
import contextlib

import yaml

try:
    from os import scandir
except ImportError:
    from scandir import scandir


VERSION = '3.5.0'
PROPERTY_PREFIX = 'zfssnap'
ZFSSNAP_LABEL = '%s:label' % PROPERTY_PREFIX
ZFSSNAP_REPL_STATUS = '%s:repl_status' % PROPERTY_PREFIX
ZFSSNAP_VERSION = '%s:version' % PROPERTY_PREFIX


def autotype(value):
    for fn in [int]:
        try:
            return fn(value)
        except ValueError:
            pass

    return value


class ReplicationException(Exception):
    pass

class SnapshotException(Exception):
    pass

class ZFSSnapException(Exception):
    pass

class SegmentMissingException(Exception):
    pass


class MetadataFile(object):
    def __init__(self, path):
        self.path = path
        self.logger = logging.getLogger(__name__)
        self.version = None
        self.timestamp = None
        self.label = None
        self.snapshot = None
        self.depends_on = None
        self.segments = []

    @staticmethod
    def _get_checksum(metadata):
        checksum = hashlib.md5()
        checksum.update(json.dumps(metadata, sort_keys=True).encode('utf-8'))
        return checksum.hexdigest()

    def read(self):
        self.logger.debug('Reading metadata from %s', self.path)

        with open(self.path) as f:
            metadata = json.load(f)

        checksum = metadata.pop('checksum')
        self.logger.debug('Validating metadata checksum')

        if checksum != self._get_checksum(metadata):
            raise ZFSSnapException('Invalid metadata checksum')

        self.version = metadata['version']
        self.timestamp = metadata['timestamp']
        self.label = metadata['label']
        self.snapshot = metadata['snapshot']
        self.depends_on = metadata['depends_on']
        self.segments = metadata['segments']

    def write(self):
        self.logger.info('Writing metadata to %s', self.path)
        metadata = {}

        with open(self.path, 'w') as f:
            metadata['label'] = self.label
            metadata['snapshot'] = self.snapshot
            metadata['version'] = self.version
            metadata['timestamp'] = self.timestamp
            metadata['depends_on'] = self.depends_on
            metadata['segments'] = self.segments
            metadata['checksum'] = self._get_checksum(metadata)
            f.write(json.dumps(metadata, sort_keys=True, indent=4))

    @property
    def datetime(self):
        strptime_name = re.sub(r'Z$', '+0000', self.timestamp)
        return datetime.strptime(strptime_name, '%Y%m%dT%H%M%S%z')


class Config(object):
    def __init__(self, config_file):
        if config_file is None:
            config_file = '/etc/zfssnap/zfssnap.yml'

        with open(config_file) as f:
            self._config = yaml.load(f)

    def get_policy(self, policy):
        try:
            return self._config['policies'][policy]
        except KeyError:
            raise ZFSSnapException(
                'The policy \'%s\' is not defined' % policy)

    def get_cmd(self, cmd):
        return self._config['cmds'][cmd]

    def get_cmds(self):
        return self._config.get('cmds', {})


class Snapshot(object):
    def __init__(self, host, name, properties=None):
        self.logger = logging.getLogger(__name__)
        self.name = name
        self.dataset_name, self.snapshot_name = name.split('@')
        self.host = host
        self._snapshot_name = None
        self._version = None
        self._properties = {}

        if properties:
            self._properties = properties

    @property
    def location(self):
        if self.host.name:
            return '%s: %s' % (self.host.name, self.name)
        else:
            return self.name

    def destroy(self, recursive=False):
        self.host.destroy_snapshot(self, recursive)

    @property
    def timestamp(self):
        _, timestamp = self.snapshot_name.split('_')
        return timestamp

    @property
    def datetime(self):
        strptime_name = re.sub(r'Z$', '+0000', self.snapshot_name)
        return datetime.strptime(strptime_name, 'zfssnap_%Y%m%dT%H%M%S%z')

    @property
    def repl_status(self):
        return self.get_property(ZFSSNAP_REPL_STATUS)

    @repl_status.setter
    def repl_status(self, value):
        self.set_property(ZFSSNAP_REPL_STATUS, value)

    @property
    def version(self):
        if not self._version:
            version = self.get_property(ZFSSNAP_VERSION)

            if version is None:
                if self.get_property('zfssnap:label'):
                    version = '3.0.0'
                elif self.get_property('zol:zfssnap:label'):
                    version = '2.0.0'

            self._version = version

        if not self._version:
            raise SnapshotException('Snapshot version not found')

        return self._version

    @version.setter
    def version(self, value):
        self.set_property(ZFSSNAP_VERSION, value)

    @property
    def label(self):
        zfs_property = ZFSSNAP_LABEL

        if StrictVersion('3.0.0') > StrictVersion(self.version) >= StrictVersion('2.0.0'):
            zfs_property = 'zol:zfssnap:label'

        return self.get_property(zfs_property)

    @label.setter
    def label(self, value):
        self.set_property(ZFSSNAP_LABEL, value)

    def _refresh_properties(self):
        self.logger.debug('Refreshing zfs properties cache for %s', self.name)
        self._properties = {}
        args = [
            'get', 'all',
            '-H',
            '-p',
            '-o', 'property,value',
            self.name
        ]
        cmd = self.host.get_cmd('zfs', args)
        output = subprocess.check_output(cmd)

        for line in output.decode('utf8').split('\n'):
            if not line.strip():
                continue

            name, value = line.split('\t')
            self._properties[name] = autotype(value)

    def get_properties(self, refresh=False):
        if refresh:
            self._refresh_properties()

        return self._properties

    def get_property(self, name):
        value = self.get_properties().get(name, None)

        if not value:
            self.logger.debug('The zfs property \'%s\' was not found in cache '
                              'for %s. Trying to refresh', name, self.name)
            value = self.get_properties(refresh=True).get(name, None)

        if not value:
            self.logger.debug('The zfs property \'%s\' does not exist for %s',
                              name, self.name)

        return value

    def set_property(self, name, value):
        args = [
            'set',
            '%s=%s' % (name, value),
            self.name
        ]
        cmd = self.host.get_cmd('zfs', args)
        subprocess.check_call(cmd)
        self._properties[name] = value


class Dataset(object):
    def __init__(self, host, name):
        self.name = name
        self.host = host
        self.logger = logging.getLogger(__name__)

    @property
    def location(self):
        if self.host.name:
            return '%s: %s' % (self.host.name, self.name)
        else:
            return self.name

    def get_latest_repl_snapshot(self, label=None, status='success',
                                 refresh=False):
        snapshots = sorted(self.get_snapshots(label, refresh=refresh),
                           key=attrgetter('datetime'),
                           reverse=True)

        for snapshot in snapshots:
            if snapshot.repl_status != status:
                continue

            return snapshot

    def destroy(self, recursive=False):
        self.logger.info('Destroying dataset %s', self.name)
        args = ['destroy']

        if recursive:
            args.append('-r')

        args.append(self.name)
        cmd = self.host.get_cmd('zfs', args)
        subprocess.check_call(cmd)

    @property
    def exists(self):
        return bool(self.host.get_filesystem(self.name))

    def get_snapshots(self, label=None, refresh=False):
        return self.host.get_snapshots(label=label, dataset=self,
                                       refresh=refresh)

    def get_snapshot(self, name):
        full_name = '%s@%s' % (self.name, name)
        snapshot = self.host.get_snapshot(dataset=self, name=full_name)

        if not snapshot:
            self.logger.debug('The snapshot \'%s\' was not found in cache. '
                              'Trying to refresh', full_name)
            snapshot = self.host.get_snapshot(dataset=self, name=full_name,
                                              refresh=True)

        return snapshot

    @staticmethod
    def _write_metadata_file(name, segments, snapshot, previous_snapshot=None):
        metadata = MetadataFile(name)
        metadata.segments = segments
        metadata.label = snapshot.label
        metadata.snapshot = snapshot.snapshot_name
        metadata.version = snapshot.version
        metadata.timestamp = snapshot.timestamp

        if previous_snapshot:
            metadata.depends_on = previous_snapshot.snapshot_name

        metadata.write()

    @staticmethod
    def cleanup_sync_files(metadata, src_dir):
        with contextlib.suppress(FileNotFoundError):
            for segment in metadata.segments:
                segment_path = os.path.join(src_dir, segment)
                os.remove(segment_path)

            os.remove(os.path.join(src_dir, metadata.path))

    def receive_from_file(self, label, src_dir, file_prefix=None):
        if file_prefix is None:
            file_prefix = 'zfssnap'

        src_files = scandir(src_dir)
        metadata_pattern = r'^%s_[0-9]{8}T[0-9]{6}Z.json$' % file_prefix
        metadata_re = re.compile(metadata_pattern)
        metadata_files = []

        for f in src_files:
            if re.match(metadata_re, f.name):
                metadata = MetadataFile(f.path)
                metadata.read()

                if metadata.label != label:
                    self.logger.warning(
                        'Wrong snapshot label (%s). Do you have other metadata '
                        'files in %s sharing the prefix \'%s\'? Skipping.',
                        metadata.label, src_dir, file_prefix)
                    continue

                if metadata.version != VERSION:
                    raise ReplicationException(
                        'The incoming snapshot was generated using zfssnap '
                        'v%s, while this receiver is using zfssnap v%s. '
                        'Only snapshots generated by the same version of '
                        'zfssnap is accepted in case of incompatibilites. '
                        'Please ensure this receiver use the same version as '
                        'the sender and try again.' % (metadata.version, VERSION))

                metadata_files.append(metadata)


        for metadata in sorted(metadata_files, key=attrgetter('datetime')):
            self.logger.info('Selecting %s', metadata.path)

            # Make sure the cache is refreshed as the snapshot count might have
            # changed if multiple metadata files are processed in one run
            previous_snapshot = self.get_latest_repl_snapshot(label,
                                                              refresh=True)

            if previous_snapshot and previous_snapshot.datetime >= metadata.datetime:
                self.logger.warning('Ignoring %s as it is already applied or '
                                    'older than the current snapshot', metadata.path)
                self.cleanup_sync_files(metadata, src_dir)
                continue


            if metadata.depends_on and not self.get_snapshot(metadata.depends_on):
                raise ReplicationException(
                    'The dependant snapshot %s does not exist on destination dataset %s' %
                    (metadata.depends_on, self.name))

            segment_files = []

            # Refresh file list for each run as each sync can potentially take
            # a long time
            src_file_names = {f.name for f in scandir(src_dir)}

            for segment in sorted(metadata.segments):
                if segment not in src_file_names:
                    raise SegmentMissingException('Segment %s is missing in %s', segment, src_dir)

                segment_files.append(os.path.join(src_dir, segment))

            cat_args = segment_files
            cat_cmd = self.host.get_cmd('cat', cat_args)
            receive_args = ['receive', '-F', '-v', self.name]
            receive_cmd = self.host.get_cmd('zfs', receive_args)

            self.logger.debug('Replicate cmd: \'%s | %s\'', ' '.join(cat_cmd),
                              ' '.join(receive_cmd))
            cat = subprocess.Popen(cat_cmd, stdout=subprocess.PIPE)
            receive = subprocess.Popen(receive_cmd, stdin=cat.stdout,
                                       stdout=subprocess.PIPE)
            cat.stdout.close()

            # See comment in replicate()
            stdout_data, _ = receive.communicate()

            for line in stdout_data.decode('utf8').split('\n'):
                if not line.strip():
                    continue

                self.logger.info(line)

            if receive.returncode == 0:
                # See comment in replicate()
                # Workaround for ZoL bug in initial replication fixed in 0.7.0?
                dst_snapshot = Snapshot(self.host, '%s@%s' % (self.name, metadata.snapshot))
                dst_snapshot.label = metadata.label
                dst_snapshot.version = metadata.version

                #dst_snapshot = self.get_snapshot(metadata.snapshot)
                dst_snapshot.repl_status = 'success'

                # Cleanup files after marking the sync as success as we don't
                # really care if this goes well for the sake of sync integrity
                self.cleanup_sync_files(metadata, src_dir)
            else:
                raise ReplicationException('Replication failed!')

    def send_to_file(self, label, dst_dir, file_prefix=None, suffix_length=None,
                     split_size=None, source_snapshot=None):
        if file_prefix is None:
            file_prefix = 'zfssnap'

        if suffix_length is None:
            suffix_length = 4

        if split_size is None:
            split_size = '1G'

        previous_snapshot = None

        if source_snapshot:
            previous_snapshot = self.get_snapshot(source_snapshot)

            if not previous_snapshot:
                raise ReplicationException('The source snapshot %s was not found',
                                           source_snapshot)
        else:
            previous_snapshot = self.get_latest_repl_snapshot(label)

        snapshot = self.create_snapshot(label=label, recursive=True)

        send_args = ['send', '-R']

        if previous_snapshot:
            send_args.extend(['-I', '@%s' % previous_snapshot.snapshot_name])

        send_args.append(snapshot.name)
        send_cmd = self.host.get_cmd('zfs', send_args)

        segments = []
        self.logger.info('Splitting at segment size %s', split_size)
        prefix = os.path.join(dst_dir, '%s_%s-' % (file_prefix, snapshot.timestamp))
        metadata_file = os.path.join(dst_dir, '%s_%s.json' % (file_prefix, snapshot.timestamp))
        segments_log_pattern = r'^creating\sfile\s.*(%s[a-z]{%s}).*$' % (prefix, suffix_length)
        segments_log_re = re.compile(segments_log_pattern)
        split_args = [
            '--bytes=%s' % split_size,
            '--suffix-length=%s' % suffix_length,
            '--verbose',
            '-',
            prefix
        ]
        split_cmd = self.host.get_cmd('split', split_args)

        self.logger.debug('Replicate cmd: \'%s | %s\'', ' '.join(send_cmd),
                          ' '.join(split_cmd))
        send = subprocess.Popen(send_cmd, stdout=subprocess.PIPE)
        split = subprocess.Popen(split_cmd, stdin=send.stdout, stdout=subprocess.PIPE)
        send.stdout.close()

        # See comment in replicate()
        stdout_data, _ = split.communicate()

        for line in stdout_data.decode('utf8').split('\n'):
            if not line.strip():
                continue

            match = re.match(segments_log_re, line)

            if match:
                segment = match.group(1)
                self.logger.info('Created replication file segment %s',
                                 os.path.join(dst_dir, segment))
                segments.append(os.path.basename(segment))
                continue

            self.logger.info(line)

        if split.returncode == 0:
            self.logger.info('Total segment count: %s', len(segments))

            # Ensure metadata file are written before repl_status are set to
            # 'success', so we are sure this end does not believe things are
            # ok and uses this snapshot as the base for the next sync while the
            # metadata file for the opposite end might not have been written
            self._write_metadata_file(metadata_file, segments, snapshot,
                                      previous_snapshot)

            # See comment in replicate()
            snapshot.repl_status = 'success'

            self.cleanup_repl_snapshots(label=label)
        else:
            raise ReplicationException('Replication failed!')

    def replicate(self, dst_dataset, label, source_snapshot):
        previous_snapshot = None

        if source_snapshot:
            previous_snapshot = self.get_snapshot(source_snapshot)

            if not previous_snapshot:
                raise ReplicationException('The source snapshot %s was not found',
                                           source_snapshot)
        else:
            previous_snapshot = self.get_latest_repl_snapshot(label)

        snapshot = self.create_snapshot(label=label, recursive=True)

        send_args = ['send', '-R']

        if previous_snapshot:
            send_args.extend(['-I', '@%s' % previous_snapshot.snapshot_name])

        send_args.append(snapshot.name)
        send_cmd = self.host.get_cmd('zfs', send_args)
        receive_args = ['receive', '-F', '-v', dst_dataset.name]
        receive_cmd = dst_dataset.host.get_cmd('zfs', receive_args)

        self.logger.debug('Replicate cmd: \'%s | %s\'', ' '.join(send_cmd),
                          ' '.join(receive_cmd))
        self.logger.info('Replicating %s to %s', self.location,
                         dst_dataset.location)
        send = subprocess.Popen(send_cmd, stdout=subprocess.PIPE)
        receive = subprocess.Popen(receive_cmd, stdin=send.stdout,
                                   stdout=subprocess.PIPE)
        send.stdout.close()

        # Do not capture stderr_data as I have found no way to capture stderr
        # from the send process properly when using pipes without it beeing
        # eaten as bad data for the receiving end when sending it through
        # the pipe to be captured in the receiving process as normally
        # suggested. Instead of having the send stderr go directly to output
        # and receive printed using logging I just leave both untouched for
        # now.
        stdout_data, _ = receive.communicate()

        for line in stdout_data.decode('utf8').split('\n'):
            if not line.strip():
                continue

            self.logger.info(line)

        if receive.returncode == 0:
            # CAUTION!
            # There is potential for a race condition here. To ensure only
            # successfully replicated snapshots are replicated on the next run,
            # repl_status is set immediately after replication.
            # However, if the script fails in that short time period,
            # then the script would not be able to find this snapshot on the
            # next run as it only looks for success in repl_status for
            # potential snapshots to use for incremental replication, even
            # though it exists on both sides.
            # It is therefore important to ensure that at least one replication
            # snapshot with repl_status success exists at all times.
            snapshot.repl_status = 'success'

            # For completeness also set repl_status to success on destination.
            dst_snapshot = dst_dataset.get_snapshot(snapshot.snapshot_name)
            dst_snapshot.repl_status = snapshot.repl_status

            self.cleanup_repl_snapshots(label=label)
        else:
            raise ReplicationException('Replication failed!')

    def create_snapshot(self, label, recursive=False, ts=None):
        if ts is None:
            ts = datetime.utcnow()

        timestamp = ts.strftime('%Y%m%dT%H%M%SZ')
        name = '%s@zfssnap_%s' % (self.name, timestamp)
        snapshot = self.host.create_snapshot(name=name, label=label,
                                             recursive=recursive)
        return snapshot

    def cleanup_repl_snapshots(self, label=None, keep=1):
        snapshots = self.get_snapshots(label)
        keep_snapshots = []

        for snapshot in sorted(snapshots, key=attrgetter('datetime'), reverse=True):
            if len(keep_snapshots) < keep:
                if snapshot.repl_status == 'success':
                    keep_snapshots.append(snapshot)
            else:
                snapshot.destroy(recursive=True)

    def cleanup_snapshots(self, keep, label=None, recursive=False):
        snapshots = sorted(self.get_snapshots(label),
                           key=attrgetter('datetime'),
                           reverse=True)[keep:]

        for snapshot in sorted(snapshots, key=attrgetter('datetime')):
            snapshot.destroy(recursive)


class Host(object):
    def __init__(self, ssh_user=None, name=None, cmds=None):
        if cmds is None:
            cmds = {}

        self.logger = logging.getLogger(__name__)
        self.cmds = self._validate_cmds(cmds)
        self.ssh_user = ssh_user
        self.name = name
        self._snapshots = []
        self._snapshots_refreshed = False

    @staticmethod
    def _validate_cmds(cmds):
        valid_cmds = {
            'zfs': 'zfs',
            'ssh': 'ssh',
            'split': 'split',
            'cat': 'cat'
        }

        valid_cmds.update({k: v for k, v in cmds.items() if v is not None})
        return valid_cmds

    def get_cmd(self, name, args=None):
        cmd_path = self.cmds.get(name, None)

        if cmd_path is None:
            raise ZFSSnapException(
                '\'%s\' does not have a path defined.' % name)

        if args is None:
            args = []

        ssh_cmd = self.cmds.get('ssh', None)

        if ssh_cmd and self.name:
            if self.ssh_user:
                cmd = [ssh_cmd, '%s@%s' % (self.ssh_user, self.name), cmd_path]
            else:
                cmd = [ssh_cmd, self.name, cmd_path]
        else:
            cmd = [cmd_path]

        cmd.extend(args)
        self.logger.debug('Command: %s', ' '.join(cmd))
        return cmd

    def get_filesystems(self, include=None, exclude=None):
        if include is None:
            include = []

        if exclude is None:
            exclude = []

        args = [
            'list',
            '-H',
            '-p',
            '-o', 'name',
            '-t', 'filesystem'
        ]
        cmd = self.get_cmd('zfs', args)
        output = subprocess.check_output(cmd)

        for name in output.decode('utf8').split('\n'):
            exclude_filesystem = False

            if not name.strip():
                continue

            for pattern in exclude:
                if fnmatch.fnmatch(name, pattern):
                    self.logger.debug('\'%s\' is excluded by pattern \'%s\'',
                                      name, pattern)
                    exclude_filesystem = True
                    break

            if exclude_filesystem:
                continue

            if include:
                for pattern in include:
                    if fnmatch.fnmatch(name, pattern):
                        yield Dataset(host=self, name=name)
                        break
            else:
                yield Dataset(host=self, name=name)

    def create_snapshot(self, name, label, recursive=False):
        self.logger.info('Creating snapshot %s (label: %s)', name, label)

        if label == '-':
            raise SnapshotException('\'%s\' is not a valid label' % label)

        properties = {
            ZFSSNAP_LABEL: label,
            ZFSSNAP_VERSION: VERSION
        }

        args = [
            'snapshot',
        ]

        for key, value in properties.items():
            args.extend([
                '-o', '%s=%s' % (key, value),
            ])

        if recursive:
            args.append('-r')

        args.append(name)
        cmd = self.get_cmd('zfs', args)
        subprocess.check_call(cmd)
        snapshot = Snapshot(self, name, properties=properties)
        self._snapshots.append(snapshot)

        return snapshot

    def destroy_snapshot(self, snapshot, recursive=False):
        self.logger.info('Destroying dataset %s', snapshot.name)
        args = ['destroy']

        if recursive:
            args.append('-r')

        args.append(snapshot.name)
        cmd = self.get_cmd('zfs', args)
        subprocess.check_call(cmd)
        self._snapshots.remove(snapshot)

    def _refresh_snapshots(self):
        self.logger.debug('Refreshing snapshot cache')
        self._snapshots = []
        snapshots = {}
        name_pattern = r'^.+@zfssnap_[0-9]{8}T[0-9]{6}Z$'
        name_re = re.compile(name_pattern)

        args = [
            'get', 'all',
            '-H',
            '-p',
            '-o', 'name,property,value',
            '-t', 'snapshot',
        ]

        cmd = self.get_cmd('zfs', args)
        output = subprocess.check_output(cmd)
        fast_skip = None

        for line in output.decode('utf8').split('\n'):
            if not line.strip():
                continue

            name, zfs_property, value = line.split('\t')

            # Avoid having to regex check name for every ZFS property
            # if we know it's not a zfssnap snapshot.
            if name == fast_skip:
                continue

            fast_skip = None

            # There is no point looking at snapshots not taken by zfssnap
            if not re.match(name_re, name):
                self.logger.debug('%s is not a zfssnap snapshot. Skipping.', name)
                fast_skip = name
                continue

            if name not in snapshots:
                snapshots[name] = {}

            snapshots[name][zfs_property] = autotype(value)

        for name, properties in snapshots.items():
            snapshot = Snapshot(self, name, properties=properties)
            self._snapshots.append(snapshot)

        self._snapshots_refreshed = True


    def get_snapshot(self, name, dataset, refresh=False):
        for snapshot in self.get_snapshots(dataset=dataset, refresh=refresh):
            if snapshot.name == name:
                return snapshot

    def get_snapshots(self, dataset=None, label=None, refresh=False):
        if refresh or not self._snapshots_refreshed:
            self._refresh_snapshots()

        for snapshot in self._snapshots:
            if dataset and snapshot.dataset_name != dataset.name:
                continue

            if label and snapshot.label != label:
                continue

            yield snapshot

    def get_filesystem(self, fs_name):
        first_fs = None

        # This slightly convoluted way to return the first filesystem tries to
        # err out early without having to fetch the entire filesystem list if
        # e.g. '*' is provided as fs_name.
        for fs in self.get_filesystems(include=[fs_name]):
            if first_fs:
                raise ZFSSnapException('More than one dataset matches %s' % fs_name)

            first_fs = fs

        return first_fs


class ZFSSnap(object):
    def __init__(self, config=None, lockfile=None):
        self.logger = logging.getLogger(__name__)
        self.lockfile = '/run/lock/zfssnap.lock'

        if lockfile:
            self.lockfile = lockfile

        # The lock file object needs to be at class level for not to be
        # garbage collected after the _aquire_lock function has finished.
        self._lock = None

        self.config = Config(config)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is KeyboardInterrupt:
            self.logger.error('zfssnap aborted!')
        elif exc_type is not None:
            self.logger.error(exc_value)

    def _aquire_lock(self, lockfile=None):
        if lockfile is None:
            lockfile = self.lockfile

        self._lock = open(lockfile, 'w')
        wait = 3
        timeout = 60

        while timeout > 0:
            try:
                fcntl.lockf(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                self.logger.debug('Lock aquired')
                return
            except OSError:
                self.logger.info('zfssnap is already running. Waiting for '
                                 'lock release... (timeout: %ss)', timeout)
                timeout = timeout - wait
                time.sleep(wait)

        raise ZFSSnapException('Timeout reached. Could not aquire lock.')

    def _release_lock(self):
        fcntl.flock(self._lock, fcntl.LOCK_UN)
        self.logger.debug('Lock released')

    def _run_snapshot_policy(self, policy, reset=False):
        if not reset:
            sleep = 1
            self.logger.debug('Sleeping %ss to avoid potential snapshot name '
                              'collisions due to matching timestamps', sleep)
            time.sleep(sleep)

        policy_config = self.config.get_policy(policy)
        label = policy_config.get('label', policy)
        host = Host(cmds=self.config.get_cmds())
        datasets = host.get_filesystems(
            include=policy_config.get('include', None),
            exclude=policy_config.get('exclude', None))
        keep = policy_config['keep']
        recursive = policy_config.get('recursive', False)

        if reset:
            self.logger.warning('Reset is enabled. Removing all snapshots '
                                'for this policy')
            keep = 0

        self._aquire_lock()

        for dataset in datasets:
            if keep > 0:
                dataset.create_snapshot(label=label, recursive=recursive)

            dataset.cleanup_snapshots(keep=keep, label=label, recursive=recursive)

        self._release_lock()

    def _run_replicate_policy(self, policy, reset=False, source_snapshot=None):
        if not reset:
            sleep = 1
            self.logger.debug('Sleeping %ss to avoid potential snapshot name '
                              'collisions due to matching timestamps', sleep)
            time.sleep(sleep)

        policy_config = self.config.get_policy(policy)
        label = policy_config.get('label', policy)
        src_host = Host(cmds=self.config.get_cmds())
        src_dataset = src_host.get_filesystem(policy_config['source']['dataset'])
        dst_host = Host(
            ssh_user=policy_config['destination'].get('ssh_user', None),
            name=policy_config['destination'].get('host', None),
            cmds=policy_config['destination'].get('cmds', None))
        dst_dataset = Dataset(dst_host, policy_config['destination']['dataset'])

        self._aquire_lock()

        if reset:
            self.logger.warning('Reset is enabled. Reinitializing replication.')
            self.logger.warning('Cleaning up source replication snapshots')
            src_dataset.cleanup_repl_snapshots(label=label, keep=0)

            if dst_dataset.exists:
                self.logger.warning('Destroying destination dataset')
                dst_dataset.destroy(recursive=True)
        else:
            src_dataset.replicate(dst_dataset, label, source_snapshot)

        self._release_lock()

    def _run_receive_from_file_policy(self, policy, reset=False):
        policy_config = self.config.get_policy(policy)
        label = policy_config.get('label', policy)
        dst_host = Host(cmds=self.config.get_cmds())
        dst_dataset = Dataset(dst_host, policy_config['destination']['dataset'])
        src_dir = policy_config['source']['dir']
        file_prefix = policy_config.get('file_prefix', None)

        self._aquire_lock()

        if reset:
            self.logger.warning('Reset is enabled. Reinitializing replication.')

            if dst_dataset.exists:
                self.logger.warning('Destroying destination dataset')
                dst_dataset.destroy(recursive=True)
        else:
            try:
                dst_dataset.receive_from_file(label, src_dir, file_prefix)
            except SegmentMissingException as e:
                self.logger.info(e)

        self._release_lock()

    def _run_send_to_file_policy(self, policy, reset=False, source_snapshot=None):
        if not reset:
            sleep = 1
            self.logger.debug('Sleeping %ss to avoid potential snapshot name '
                              'collisions due to matching timestamps', sleep)
            time.sleep(sleep)

        policy_config = self.config.get_policy(policy)
        label = policy_config.get('label', policy)
        src_host = Host(cmds=self.config.get_cmds())
        src_dataset = src_host.get_filesystem(policy_config['source']['dataset'])
        dst_dir = policy_config['destination']['dir']
        file_prefix = policy_config.get('file_prefix', None)
        suffix_length = policy_config['source'].get('suffix_length', None)
        split_size = policy_config['source'].get('split_size', None)

        self._aquire_lock()

        if reset:
            self.logger.warning('Reset is enabled. Reinitializing replication.')
            self.logger.warning('Cleaning up source replication snapshots')
            src_dataset.cleanup_repl_snapshots(label=label, keep=0)
        else:
            src_dataset.send_to_file(label, dst_dir, file_prefix, suffix_length,
                                     split_size, source_snapshot)

        self._release_lock()

    def _list_snapshot_policy(self, policy):
        policy_config = self.config.get_policy(policy)
        host = Host(cmds=self.config.get_cmds())
        label = policy_config.get('label', policy)

        # Store the dataset in a list as the iterator is consumed several
        # times below.
        datasets = [
            d for d in host.get_filesystems(
                include=policy_config.get('include', None),
                exclude=policy_config.get('exclude', None))
        ]

        print('DATASETS')
        self._print_datasets(datasets)

        print('\nSNAPSHOTS')
        snapshots = self.get_snapshots(label=label, datasets=datasets)
        self._print_snapshots(snapshots)

    def _list_replicate_policy(self, policy):
        policy_config = self.config.get_policy(policy)
        label = policy_config.get('label', policy)

        # Print source datasets
        src_host = Host(cmds=self.config.get_cmds())
        src_dataset = src_host.get_filesystem(policy_config['source']['dataset'])
        print('SOURCE DATASET')
        self._print_datasets([src_dataset])

        # Print destination datasets
        dst_host = Host(
            ssh_user=policy_config['destination'].get('ssh_user', None),
            name=policy_config['destination'].get('host', None),
            cmds=policy_config['destination'].get('cmds', None))
        dst_dataset = Dataset(dst_host, policy_config['destination']['dataset'])
        print('\nDESTINATION DATASET')
        self._print_datasets([dst_dataset])

        # Print source snapshots
        src_snapshots = self.get_snapshots(label=label, datasets=[src_dataset])
        print('\nSOURCE SNAPSHOTS')
        self._print_snapshots(src_snapshots)

        # Print destination snapshots
        if dst_dataset.exists:
            dst_snapshots = self.get_snapshots(label=label, datasets=[dst_dataset])
        else:
            dst_snapshots = iter([])

        print('\nDESTINATION SNAPSHOTS')
        self._print_snapshots(dst_snapshots)

    def execute_policy(self, policy, mode, reset=False, source_snapshot=None):
        exec_mode = 'exec'
        list_mode = 'list'
        policy_type = self.config.get_policy(policy)['type']

        if policy_type == 'snapshot':
            if mode == exec_mode:
                self._run_snapshot_policy(policy, reset)
            elif mode == list_mode:
                self._list_snapshot_policy(policy)
            else:
                raise ZFSSnapException('%s is not a valid mode for policy type %s' %
                                       (mode, policy_type))
        elif policy_type == 'replicate':
            if mode == exec_mode:
                self._run_replicate_policy(policy, reset, source_snapshot)
            elif mode == list_mode:
                self._list_replicate_policy(policy)
            else:
                raise ZFSSnapException('%s is not a valid mode for policy type %s' %
                                       (mode, policy_type))
        elif policy_type == 'send_to_file':
            if mode == exec_mode:
                self._run_send_to_file_policy(policy, reset, source_snapshot)
            elif mode == list_mode:
                self._list_snapshot_policy(policy)
            else:
                raise ZFSSnapException('%s is not a valid mode for policy type %s' %
                                       (mode, policy_type))
        elif policy_type == 'receive_from_file':
            if mode == exec_mode:
                self._run_receive_from_file_policy(policy, reset)
            elif mode == list_mode:
                self._list_snapshot_policy(policy)
            else:
                raise ZFSSnapException('%s is not a valid mode for policy type %s' %
                                       (mode, policy_type))
        else:
            raise ZFSSnapException('%s is not a valid policy type' % policy_type)

    @staticmethod
    def get_snapshots(label=None, datasets=None):
        if datasets is None:
            datasets = []

        for dataset in datasets:
            for snapshot in dataset.get_snapshots(label=label):
                yield snapshot

    @staticmethod
    def _print_snapshots(snapshots):
        for snapshot in snapshots:
            print(snapshot.location)

    @staticmethod
    def _print_datasets(datasets):
        for dataset in datasets:
            print(dataset.location)


def main():
    parser = argparse.ArgumentParser(
        description='Automatic snapshotting and replication for ZFS on Linux')

    mutex_group = parser.add_mutually_exclusive_group(required=True)
    mutex_group.add_argument('--version', action='store_true',
                             help='Print version and exit')
    mutex_group.add_argument('--policy', help='Select policy')

    mutex_group2 = parser.add_mutually_exclusive_group()
    mutex_group2.add_argument(
        '--reset', action='store_true',
        help='Remove all policy snapshots or reinitialize replication')
    mutex_group2.add_argument('--list', action='store_true',
                              help='List all policy snapshots')
    mutex_group2.add_argument(
        '--source-snapshot', metavar='NAME',
        help='Override the source snapshot used for replication')

    parser.add_argument('--quiet', action='store_true',
                        help='Suppress output from script')
    parser.add_argument(
        '--log-level',
        choices=[
            'CRITICAL',
            'ERROR',
            'WARNING',
            'INFO',
            'DEBUG'
        ],
        default='INFO', help='Set log level for console output. Default: INFO')
    parser.add_argument('--config', metavar='PATH',
                        help='Path to configuration file')
    parser.add_argument('--lockfile', metavar='PATH',
                        help='Override path to lockfile')
    args = parser.parse_args()

    if args.version:
        print('zfssnap v%s' % VERSION)
        return 0

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    if not args.quiet:
        fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
        ch = logging.StreamHandler()
        ch.setLevel(args.log_level)
        ch.setFormatter(fmt)
        logger.addHandler(ch)

    if args.list:
        mode = 'list'
    else:
        mode = 'exec'

    try:
        with ZFSSnap(config=args.config, lockfile=args.lockfile) as z:
            z.execute_policy(args.policy, mode, args.reset, args.source_snapshot)
    except ZFSSnapException:
        return 10
    except ReplicationException:
        return 11
    except SnapshotException:
        return 12
    except KeyboardInterrupt:
        return 130

if __name__ == '__main__':
    sys.exit(main())
