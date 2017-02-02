#!/usr/bin/env python3

import argparse
import logging
import sys
import subprocess
import re
import os
from datetime import datetime, timedelta, timezone
from operator import attrgetter
import fcntl
import time
import fnmatch
from distutils.version import StrictVersion
import hashlib
import json
import contextlib

import yaml
from dateutil.relativedelta import relativedelta

try:
    from os import scandir
except ImportError:
    from scandir import scandir


VERSION = '3.6.2'
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


class MetadataFileException(Exception):
    pass

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
        self._version = None
        self._timestamp = None
        self._label = None
        self._snapshot = None
        self._depends_on = None
        self._segments = []

    @staticmethod
    def _get_checksum(metadata):
        checksum = hashlib.md5()
        checksum.update(json.dumps(metadata, sort_keys=True).encode('utf-8'))
        return checksum.hexdigest()

    def _read_file(self):
        self.logger.debug('Reading metadata from %s', self.path)
        with open(self.path) as f:
            return json.load(f)

    def _write_file(self, metadata):
        self.logger.info('Writing metadata to %s', self.path)
        with open(self.path, 'w') as f:
            f.write(json.dumps(metadata, sort_keys=True, indent=4))

    def read(self):
        metadata = self._read_file()
        checksum = metadata.pop('checksum')
        self.logger.debug('Validating metadata checksum')

        if checksum != self._get_checksum(metadata):
            raise MetadataFileException('Invalid metadata checksum')

        self.version = metadata['version']
        self.timestamp = metadata['timestamp']
        self.label = metadata['label']
        self.snapshot = metadata['snapshot']
        self.depends_on = metadata['depends_on']
        self.segments = metadata['segments']

    def write(self):
        metadata = {}
        metadata['label'] = self.label
        metadata['snapshot'] = self.snapshot
        metadata['version'] = self.version
        metadata['timestamp'] = self.timestamp
        metadata['depends_on'] = self.depends_on
        metadata['segments'] = self.segments
        metadata['checksum'] = self._get_checksum(metadata)

        for key, value in metadata.items():
            if key == 'depends_on':
                continue
            if not value:
                raise MetadataFileException('\'%s\' attribute is not set' % key)

        self._write_file(metadata)

    @staticmethod
    def _validate_snapshot_name(name):
        pattern = r'^zfssnap_[0-9]{8}T[0-9]{6}Z$'
        if not re.match(pattern, name):
            raise MetadataFileException('Invalid snapshot name \'%s\'' % name)
        return name

    @property
    def label(self):
        return self._label

    @label.setter
    def label(self, label):
        if not label:
            raise MetadataFileException('empty label value')
        if not isinstance(label, str):
            raise MetadataFileException('label must be a str object')
        self._label = label

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, version):
        if not version:
            raise MetadataFileException('empty version value')
        if not isinstance(version, str):
            raise MetadataFileException('version must be a str object')
        self._version = version

    @property
    def segments(self):
        return self._segments

    @segments.setter
    def segments(self, segments):
        if not segments:
            raise MetadataFileException('empty segment list')
        if not isinstance(segments, list):
            raise MetadataFileException('segments must be a list object')
        self._segments = segments

    @property
    def snapshot(self):
        return self._snapshot

    @snapshot.setter
    def snapshot(self, name):
        self._snapshot = self._validate_snapshot_name(name)

    @property
    def depends_on(self):
        return self._depends_on

    @depends_on.setter
    def depends_on(self, name):
        if name is not None:
            self._depends_on = self._validate_snapshot_name(name)

    @property
    def timestamp(self):
        return self._timestamp

    @timestamp.setter
    def timestamp(self, timestamp):
        pattern = r'^[0-9]{8}T[0-9]{6}Z$'
        if not re.match(pattern, timestamp):
            raise MetadataFileException('Invalid timestamp \'%s\'' % timestamp)
        self._timestamp = timestamp

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
        self._datetime = None
        self._version = None
        self._properties = {}
        self.keep_reasons = []

        if properties:
            self._properties = properties

    @property
    def location(self):
        if self.host.name:
            return '%s: %s' % (self.host.name, self.name)
        else:
            return self.name

    def destroy(self, recursive=False):
        self.logger.info('Destroying snapshot %s', self.name)
        args = ['destroy']

        if recursive:
            args.append('-r')

        args.append(self.name)
        cmd = self.host.get_cmd('zfs', args)
        subprocess.check_call(cmd)
        self.host.snapshot_cache_remove(self)

    @property
    def timestamp(self):
        _, timestamp = self.snapshot_name.split('_')
        return timestamp

    @property
    def datetime(self):
        if not self._datetime:
            strptime_name = re.sub(r'Z$', '+0000', self.snapshot_name)
            self._datetime = datetime.strptime(strptime_name, 'zfssnap_%Y%m%dT%H%M%S%z')
        return self._datetime

    @property
    def repl_status(self):
        return self.get_property(ZFSSNAP_REPL_STATUS)

    @repl_status.setter
    def repl_status(self, value):
        self.set_property(ZFSSNAP_REPL_STATUS, value)

    @property
    def version(self):
        return self.get_property(ZFSSNAP_VERSION)

    @version.setter
    def version(self, value):
        self.set_property(ZFSSNAP_VERSION, value)

    @property
    def label(self):
        return self.get_property(ZFSSNAP_LABEL)

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

    def add_keep_reason(self, value):
        self.keep_reasons.append(value)


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
        snapshots = sorted(self.get_snapshots(label=label, refresh=refresh),
                           key=attrgetter('datetime'),
                           reverse=True)

        for snapshot in snapshots:
            if snapshot.repl_status == status:
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
        for snapshot in self.host.get_snapshots_cached(refresh):
            if snapshot.dataset_name != self.name:
                continue
            if label and snapshot.label != label:
                continue
            yield snapshot

    def get_snapshot(self, name, refresh=False):
        for snapshot in self.get_snapshots(refresh=refresh):
            if snapshot.snapshot_name == name:
                return snapshot

    @staticmethod
    def _write_metadata_file(name, segments, snapshot, base_snapshot=None):
        metadata = MetadataFile(name)
        metadata.segments = segments
        metadata.label = snapshot.label
        metadata.snapshot = snapshot.snapshot_name
        metadata.version = snapshot.version
        metadata.timestamp = snapshot.timestamp

        if base_snapshot:
            metadata.depends_on = base_snapshot.snapshot_name

        metadata.write()

    @staticmethod
    def cleanup_sync_files(metadata, src_dir):
        with contextlib.suppress(FileNotFoundError):
            for segment in metadata.segments:
                segment_path = os.path.join(src_dir, segment)
                os.remove(segment_path)

            os.remove(os.path.join(src_dir, metadata.path))

    @staticmethod
    def _get_segments(src_dir, metadata_segments):
        # Refresh file list for each run as each sync can potentially take
        # a long time
        src_file_names = {f.name for f in scandir(src_dir)}

        for segment in sorted(metadata_segments):
            if segment not in src_file_names:
                raise SegmentMissingException('Segment %s is missing in %s', segment, src_dir)

            yield os.path.join(src_dir, segment)

    def _get_cat_cmd(self, segments):
        return self.host.get_cmd('cat', segments)

    @staticmethod
    def _get_receive_cmd(dataset):
        receive_args = ['receive', '-F', '-v', dataset.name]
        return dataset.host.get_cmd('zfs', receive_args)

    def _get_base_snapshot(self, label=None, base_snapshot=None):
        if base_snapshot:
            snapshot = self.get_snapshot(base_snapshot)

            if not snapshot:
                raise ReplicationException(
                    'The base snapshot %s was not found' % base_snapshot)
        else:
            snapshot = self.get_latest_repl_snapshot(label)

        return snapshot

    def _get_send_cmd(self, snapshot, base_snapshot):
        send_args = ['send', '-R']

        if base_snapshot:
            send_args.extend(['-I', '@%s' % base_snapshot.snapshot_name])

        send_args.append(snapshot.name)
        return self.host.get_cmd('zfs', send_args)

    def _get_split_cmd(self, prefix, split_size, suffix_length):
        if suffix_length is None:
            suffix_length = 4

        if split_size is None:
            split_size = '1G'

        self.logger.info('Splitting at segment size %s', split_size)
        split_args = [
            '--bytes=%s' % split_size,
            '--suffix-length=%s' % suffix_length,
            '--verbose',
            '-',
            prefix
        ]
        return self.host.get_cmd('split', split_args)

    @staticmethod
    def _get_segment_name(line, segments_log_re):
        match = re.match(segments_log_re, line)
        if match:
            segment = match.group(1)
            return os.path.basename(segment)

    def _run_replication_cmd(self, in_cmd, out_cmd):
        self.logger.debug('Replication command: \'%s | %s\'',
                          ' '.join(in_cmd), ' '.join(out_cmd))

        in_p = subprocess.Popen(in_cmd, stdout=subprocess.PIPE)
        out_p = subprocess.Popen(out_cmd, stdin=in_p.stdout,
                                 stdout=subprocess.PIPE)
        in_p.stdout.close()

        # Do not capture stderr_data as I have found no way to capture stderr
        # from the send process properly when using pipes without it beeing
        # eaten as bad data for the receiving end when sending it through
        # the pipe to be captured in the receiving process as normally
        # suggested. Instead of having the send stderr go directly to output
        # and receive printed using logging I just leave both untouched for
        # now.
        lines = []

        while out_p.poll() is None:
            for line in iter(out_p.stdout.readline, b''):
                line = line.strip().decode('utf8')
                self.logger.info(line)
                lines.append(line)

        if out_p.returncode != 0:
            raise ReplicationException('Replication failed')

        return lines

    def receive_from_file(self, label, src_dir, metadata):
        self.logger.info('Selecting %s', metadata.path)

        # Make sure the cache is refreshed as the snapshot count might have
        # changed if multiple metadata files are processed in one run
        previous_snapshot = self.get_latest_repl_snapshot(label, refresh=True)

        if previous_snapshot and previous_snapshot.datetime >= metadata.datetime:
            self.logger.warning('Ignoring %s as it is already applied or '
                                'older than the current snapshot', metadata.path)
            self.cleanup_sync_files(metadata, src_dir)
            return

        if metadata.depends_on and not self.get_snapshot(metadata.depends_on):
            raise ReplicationException(
                'The dependant snapshot %s does not exist on destination dataset %s' %
                (metadata.depends_on, self.name))

        segments = self._get_segments(src_dir, metadata.segments)

        cat_cmd = self._get_cat_cmd(segments)
        receive_cmd = self._get_receive_cmd(self)
        self._run_replication_cmd(cat_cmd, receive_cmd)

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

    def send_to_file(self, label, dst_dir, file_prefix=None, suffix_length=None,
                     split_size=None, base_snapshot=None):
        if file_prefix is None:
            file_prefix = 'zfssnap'

        _base_snapshot = self._get_base_snapshot(label, base_snapshot)
        snapshot = self.snapshot(label, recursive=True)
        prefix = os.path.join(dst_dir, '%s_%s-' % (file_prefix, snapshot.timestamp))

        segments_log_pattern = r'^creating\sfile\s.*(%s[a-z]{%s}).*$' % (prefix, suffix_length)
        segments_log_re = re.compile(segments_log_pattern)

        send_cmd = self._get_send_cmd(snapshot, _base_snapshot)
        split_cmd = self._get_split_cmd(prefix, split_size, suffix_length)
        output = self._run_replication_cmd(send_cmd, split_cmd)
        segments = []

        for line in output:
            segment = self._get_segment_name(line, segments_log_re)

            if segment:
                segments.append(segment)

        self.logger.info('Total segment count: %s', len(segments))

        # Ensure metadata file are written before repl_status are set to
        # 'success', so we are sure this end does not believe things are
        # ok and uses this snapshot as the base for the next sync while the
        # metadata file for the opposite end might not have been written
        metadata_file = os.path.join(dst_dir,
                                     '%s_%s.json' % (file_prefix, snapshot.timestamp))
        self._write_metadata_file(metadata_file, segments, snapshot, _base_snapshot)

        # See comment in replicate()
        snapshot.repl_status = 'success'

    def replicate(self, dst_dataset, label, base_snapshot):
        _base_snapshot = self._get_base_snapshot(label, base_snapshot)
        snapshot = self.snapshot(label, recursive=True)

        self.logger.info('Replicating %s to %s', self.location,
                         dst_dataset.location)

        send_cmd = self._get_send_cmd(snapshot, _base_snapshot)
        receive_cmd = self._get_receive_cmd(dst_dataset)
        self._run_replication_cmd(send_cmd, receive_cmd)

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

    def snapshot(self, label, recursive=False, ts=None):
        if ts is None:
            ts = datetime.utcnow()

        if label == '-':
            raise SnapshotException('\'%s\' is not a valid label' % label)

        timestamp = ts.strftime('%Y%m%dT%H%M%SZ')
        name = '%s@zfssnap_%s' % (self.name, timestamp)
        self.logger.info('Creating snapshot %s (label: %s)', name, label)
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
        cmd = self.host.get_cmd('zfs', args)
        subprocess.check_call(cmd)
        snapshot = Snapshot(self.host, name, properties=properties)
        self.host.snapshot_cache_add(snapshot)
        return snapshot

    @staticmethod
    def _get_delta_datetimes(start, end, delta):
        current = start
        while current > end:
            yield current
            current -= delta

    def _get_interval_snapshots(self, snapshots, start, end, delta):
        _snapshots = sorted(snapshots, key=attrgetter('datetime'),
                            reverse=True)

        for dt in self._get_delta_datetimes(start, end, delta):
            for snapshot in _snapshots:
                if dt <= snapshot.datetime < dt + delta:
                    yield snapshot
                    break

    def _get_hourly_snapshots(self, snapshots, keep):
        start = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        delta = timedelta(hours=1)
        end = start - (delta * keep)
        for snapshot in self._get_interval_snapshots(snapshots, start, end, delta):
            snapshot.add_keep_reason('hourly')
            yield snapshot

    def _get_daily_snapshots(self, snapshots, keep):
        start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        delta = timedelta(days=1)
        end = start - (delta * keep)
        for snapshot in self._get_interval_snapshots(snapshots, start, end, delta):
            snapshot.add_keep_reason('daily')
            yield snapshot

    def _get_weekly_snapshots(self, snapshots, keep):
        start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        delta = timedelta(weeks=1)
        end = start - (delta * keep)
        for snapshot in self._get_interval_snapshots(snapshots, start, end, delta):
            snapshot.add_keep_reason('weekly')
            yield snapshot

    def _get_monthly_snapshots(self, snapshots, keep):
        start = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        delta = relativedelta(months=1)
        end = start - (delta * keep)
        for snapshot in self._get_interval_snapshots(snapshots, start, end, delta):
            snapshot.add_keep_reason('monthly')
            yield snapshot

    def _get_yearly_snapshots(self, snapshots, keep):
        start = datetime.now(timezone.utc).replace(month=1, day=1, hour=0, minute=0, second=0,
                                                   microsecond=0)
        delta = relativedelta(years=1)
        end = start - (delta * keep)
        for snapshot in self._get_interval_snapshots(snapshots, start, end, delta):
            snapshot.add_keep_reason('yearly')
            yield snapshot

    @staticmethod
    def _get_latest_snapshots(snapshots, keep):
        _snapshots = sorted(snapshots, key=attrgetter('datetime'), reverse=True)
        for num, snapshot in enumerate(_snapshots):
            if num >= keep:
                break
            snapshot.add_keep_reason('latest')
            yield snapshot

    def enforce_retention(self, keep, label=None, recursive=False, reset=False,
                          replication=False):
        # Make the list of snapshots an immutable tuple to ensure it's not
        # getting changed while it's passed around.
        snapshots = tuple(s for s in self.get_snapshots(label))
        keep_snapshots = set()

        if not reset:
            if replication:
                repl_snapshot = self.get_latest_repl_snapshot(label)
                if repl_snapshot:
                    repl_snapshot.add_keep_reason('replication')
                    keep_snapshots.add(repl_snapshot)

            if keep['latest'] > 0:
                keep_snapshots.update(
                    {s for s in self._get_latest_snapshots(snapshots, keep['latest'])})

            if keep['hourly'] > 0:
                keep_snapshots.update(
                    {s for s in self._get_hourly_snapshots(snapshots, keep['hourly'])})

            if keep['daily'] > 0:
                keep_snapshots.update(
                    {s for s in self._get_daily_snapshots(snapshots, keep['daily'])})

            if keep['weekly'] > 0:
                keep_snapshots.update(
                    {s for s in self._get_weekly_snapshots(snapshots, keep['weekly'])})

            if keep['monthly'] > 0:
                keep_snapshots.update(
                    {s for s in self._get_monthly_snapshots(snapshots, keep['monthly'])})

            if keep['yearly'] > 0:
                keep_snapshots.update(
                    {s for s in self._get_yearly_snapshots(snapshots, keep['yearly'])})

        for snapshot in snapshots:
            if snapshot in keep_snapshots:
                self.logger.debug('Keeping snapshot %s (reasons: %s)',
                                  snapshot.name, ', '.join(snapshot.keep_reasons))
            else:
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

    def snapshot_cache_add(self, snapshot):
        self._snapshots.append(snapshot)

    def snapshot_cache_remove(self, snapshot):
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

    def get_snapshots_cached(self, refresh=False):
        if refresh or not self._snapshots_refreshed:
            self._refresh_snapshots()

        for snapshot in self._snapshots:
            yield snapshot


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

    def _validate_keep(self, keep):
        valid_keep = {
            'latest': 0,
            'hourly': 0,
            'daily': 0,
            'weekly': 0,
            'monthly': 0,
            'yearly': 0
        }

        for key, value in valid_keep.items():
            in_value = keep.get(key, value)
            if in_value > 0:
                valid_keep[key] = in_value

        self.logger.debug('Keep values: %s', valid_keep)
        return valid_keep

    def _get_metadata_files(self, src_dir, label, file_prefix=None):
        if file_prefix is None:
            file_prefix = 'zfssnap'

        metadata_pattern = r'^%s_[0-9]{8}T[0-9]{6}Z.json$' % file_prefix
        metadata_re = re.compile(metadata_pattern)

        for f in scandir(src_dir):
            if re.match(metadata_re, f.name):
                metadata = MetadataFile(f.path)
                metadata.read()

                if metadata.label != label:
                    self.logger.warning(
                        'Wrong snapshot label (%s). Do you have other metadata '
                        'files in %s sharing the prefix \'%s\'? Skipping.',
                        metadata.label, src_dir, file_prefix)
                    continue

                if StrictVersion(metadata.version) > StrictVersion(VERSION):
                    raise ReplicationException(
                        'The incoming snapshot was generated using zfssnap '
                        'v%s, while this receiver is using the older zfssnap v%s. '
                        'Please ensure this receiver use the same or newer version '
                        'as the sender and try again.' % (metadata.version, VERSION))

                yield metadata

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
        recursive = policy_config.get('recursive', False)
        keep = self._validate_keep(policy_config.get('keep', {}))
        self._aquire_lock()

        if reset:
            self.logger.warning('Reset is enabled. Removing all snapshots for '
                                'this policy')

        for dataset in datasets:
            if not reset:
                dataset.snapshot(label, recursive)

            dataset.enforce_retention(keep, label, recursive, reset)

        self._release_lock()

    def _run_replicate_policy(self, policy, reset=False, base_snapshot=None):
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
        keep = self._validate_keep(policy_config.get('keep', {}))

        self._aquire_lock()

        if reset:
            self.logger.warning('Reset is enabled. Reinitializing replication.')

            if dst_dataset.exists:
                self.logger.warning('Destroying destination dataset')
                dst_dataset.destroy(recursive=True)
        else:
            src_dataset.replicate(dst_dataset, label, base_snapshot)

        src_dataset.enforce_retention(keep, label, recursive=True, reset=reset,
                                      replication=True)
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
                metadata_files = self._get_metadata_files(src_dir, label, file_prefix)

                for metadata in sorted(metadata_files, key=attrgetter('datetime')):
                    dst_dataset.receive_from_file(label, src_dir, metadata)
            except SegmentMissingException as e:
                self.logger.info(e)

        self._release_lock()

    def _run_send_to_file_policy(self, policy, reset=False, base_snapshot=None):
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
        keep = self._validate_keep(policy_config.get('keep', {}))

        self._aquire_lock()

        if reset:
            self.logger.warning('Reset is enabled. Reinitializing replication.')
            self.logger.warning('Cleaning up source replication snapshots')
        else:
            src_dataset.send_to_file(label, dst_dir, file_prefix, suffix_length,
                                     split_size, base_snapshot)

        src_dataset.enforce_retention(keep, label, recursive=True, reset=reset,
                                      replication=True)
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
        for dataset in datasets:
            snapshots = dataset.get_snapshots(label)
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
        src_snapshots = src_dataset.get_snapshots(label)
        print('\nSOURCE SNAPSHOTS')
        self._print_snapshots(src_snapshots)

        # Print destination snapshots
        if dst_dataset.exists:
            dst_snapshots = dst_dataset.get_snapshots(label)
        else:
            dst_snapshots = iter([])

        print('\nDESTINATION SNAPSHOTS')
        self._print_snapshots(dst_snapshots)

    def execute_policy(self, policy, mode, reset=False, base_snapshot=None):
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
                self._run_replicate_policy(policy, reset, base_snapshot)
            elif mode == list_mode:
                self._list_replicate_policy(policy)
            else:
                raise ZFSSnapException('%s is not a valid mode for policy type %s' %
                                       (mode, policy_type))
        elif policy_type == 'send_to_file':
            if mode == exec_mode:
                self._run_send_to_file_policy(policy, reset, base_snapshot)
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
        '--base-snapshot', metavar='NAME',
        help='Override the base snapshot used for replication')

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
            z.execute_policy(args.policy, mode, args.reset, args.base_snapshot)
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
