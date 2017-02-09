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
from collections import defaultdict

import yaml
from dateutil.relativedelta import relativedelta

try:
    from os import scandir
except ImportError:
    from scandir import scandir


VERSION = '3.7.1'
PROPERTY_PREFIX = 'zfssnap'
ZFSSNAP_LABEL = '%s:label' % PROPERTY_PREFIX
ZFSSNAP_REPL_STATUS = '%s:repl_status' % PROPERTY_PREFIX
ZFSSNAP_VERSION = '%s:version' % PROPERTY_PREFIX
LOGGER = logging.getLogger(__name__)


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

class ConfigException(Exception):
    pass

class SegmentMissingException(Exception):
    pass


class MetadataFile(object):
    def __init__(self, path):
        self.path = path
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
        LOGGER.debug('Reading metadata from %s', self.path)
        with open(self.path) as f:
            return json.load(f)

    def _write_file(self, metadata):
        LOGGER.info('Writing metadata to %s', self.path)
        with open(self.path, 'w') as f:
            f.write(json.dumps(metadata, sort_keys=True, indent=4))

    def read(self):
        metadata = self._read_file()
        checksum = metadata.pop('checksum')
        LOGGER.debug('Validating metadata checksum')

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
            self.config = yaml.load(f)

        self.global_defaults = self._get_global_defaults()

    def _merge(self, d1, d2):
        """Merges dictionary d2 into d1. Modifies d1 inplace"""
        for k in d2:
            if k in d1 and isinstance(d1[k], dict) and isinstance(d2[k], dict):
                self._merge(d1[k], d2[k])
            else:
                d1[k] = d2[k]

        return d1

    def _get_global_defaults(self):
        user_defaults = self.config.get('defaults', {})
        defaults = {
            'cmds': {
                'ssh': '/usr/bin/ssh',
                'zfs': '/sbin/zfs',
                'split': '/usr/bin/split',
                'cat': '/bin/cat'
            },
            'keep': {
                'latest': 0,
                'hourly': 0,
                'daily': 0,
                'weekly': 0,
                'monthly': 0,
                'yearly': 0
            }
        }

        return self._merge(defaults, user_defaults)

    def get_policy(self, policy):
        try:
            user_config = self.config['policies'][policy]
        except KeyError:
            raise ConfigException(
                'The policy \'%s\' is not defined' % policy)

        policy_type = user_config['type']
        defaults = {
            'keep': self.global_defaults['keep'],
            'label': user_config.get('label', policy)
        }

        if policy_type == 'snapshot':
            defaults.update({
                'cmds': {
                    'zfs': self.global_defaults['cmds']['zfs']
                },
                'recursive': False
            })
        elif policy_type == 'replicate':
            defaults.update({
                'source': {
                    'cmds': {
                        'zfs': self.global_defaults['cmds']['zfs'],
                        'ssh': self.global_defaults['cmds']['ssh']
                    }
                },
                'destination': {
                    'host': None,
                    'ssh_user': None,
                    'read_only': True,
                    'cmds': {
                        'zfs': self.global_defaults['cmds']['zfs'],
                    }
                }
            })
        elif policy_type == 'send_to_file':
            defaults.update({
                'cmds': {
                    'zfs': self.global_defaults['cmds']['zfs'],
                    'split': self.global_defaults['cmds']['split']
                },
                'file_prefix': 'zfssnap',
                'suffix_length': 4,
                'split_size': '1G'
            })
        elif policy_type == 'receive_from_file':
            defaults.update({
                'cmds': {
                    'zfs': self.global_defaults['cmds']['zfs'],
                    'cat': self.global_defaults['cmds']['cat']
                },
                'file_prefix': 'zfssnap',
                'destination': {
                    'read_only': True
                }
            })

        self._validate_keep(user_config.get('keep', {}))
        return self._merge(defaults, user_config)

    def _validate_keep(self, keep):
        for key, value in keep.items():
            if key not in self.global_defaults['keep']:
                raise ConfigException('%s is not a valid keep interval' % key)
            elif value < 0:
                raise ConfigException(
                    '%s is set to a negative value (%s)' % (key, value))


class Dataset(object):
    def __init__(self, host, name, properties=None):
        self.name = name
        self.host = host

        if properties:
            for name, value in properties.items():
                self.host.cache_add_property(self.name, name, value)

    def _destroy(self, recursive=False, defer=False):
        args = ['destroy']

        if recursive:
            args.append('-r')

        if defer:
            args.append('-d')

        args.append(self.name)
        cmd = self.host.get_cmd('zfs', args)
        subprocess.check_call(cmd)

    def set_property(self, name, value):
        if value is None:
            self.unset_property(name)
            return

        args = [
            'set',
            '%s=%s' % (name, value),
            self.name
        ]
        cmd = self.host.get_cmd('zfs', args)
        subprocess.check_call(cmd)
        self.host.cache_add_property(self.name, name, value)

    def unset_property(self, name):
        args = [
            'inherit',
            name,
            self.name
        ]
        cmd = self.host.get_cmd('zfs', args)
        subprocess.check_call(cmd)
        self.host.cache_remove_property(self.name, name)

    def get_properties(self, refresh=False):
        return self.host.get_properties_cached(refresh)[self.name]

    def get_property(self, name):
        value = self.get_properties().get(name, None)
        if not value:
            LOGGER.debug('The zfs property \'%s\' was not found in cache '
                         'for %s. Trying to refresh', name, self.name)
            value = self.get_properties(refresh=True).get(name, None)
        if not value:
            LOGGER.debug('The zfs property \'%s\' does not exist for %s',
                         name, self.name)
        return value


class Snapshot(Dataset):
    def __init__(self, host, name, properties=None):
        if properties is None:
            properties = {}

        properties['type'] = 'snapshot'
        super(Snapshot, self).__init__(host, name, properties)

        self.dataset_name, self.snapshot_name = name.split('@')
        self._datetime = None
        self._version = None
        self.keep_reasons = []

    def destroy(self, recursive=False, defer=True):
        LOGGER.info('Destroying snapshot %s', self.name)
        self._destroy(recursive, defer)
        self.host.cache_remove_snapshot(self)

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

    def add_keep_reason(self, value):
        self.keep_reasons.append(value)


class Filesystem(Dataset):
    def __init__(self, host, name, properties=None):
        if properties is None:
            properties = {}

        properties['type'] = 'filesystem'
        super(Filesystem, self).__init__(host, name, properties)

    @property
    def read_only(self):
        return self.get_property('readonly')

    @read_only.setter
    def read_only(self, value):
        self.set_property('readonly', value)

    def get_latest_repl_snapshot(self, label=None, status='success',
                                 refresh=False):
        snapshots = sorted(self.get_snapshots(label=label, refresh=refresh),
                           key=attrgetter('datetime'),
                           reverse=True)

        for snapshot in snapshots:
            if snapshot.repl_status == status:
                return snapshot

    def destroy(self, recursive=False):
        LOGGER.info('Destroying filesystem %s', self.name)
        self._destroy(recursive)
        self.host.cache_remove_filesystem(self)

    def get_snapshots(self, label=None, refresh=False):
        for snapshot in self.host.cache_get_snapshots(refresh):
            if snapshot.dataset_name != self.name:
                continue
            if label and snapshot.label != label:
                continue
            yield snapshot

    def get_snapshot(self, name, refresh=False):
        for snapshot in self.get_snapshots(refresh=refresh):
            if snapshot.snapshot_name == name:
                return snapshot

    def get_base_snapshot(self, label=None, base_snapshot=None):
        if base_snapshot:
            snapshot = self.get_snapshot(base_snapshot)

            if not snapshot:
                raise ReplicationException(
                    'The base snapshot %s was not found' % base_snapshot)
        else:
            snapshot = self.get_latest_repl_snapshot(label)
        return snapshot

    def get_send_cmd(self, snapshot, base_snapshot):
        send_args = ['send', '-R']

        if base_snapshot:
            send_args.extend(['-I', '@%s' % base_snapshot.snapshot_name])

        send_args.append(snapshot.name)
        return self.host.get_cmd('zfs', send_args)

    def get_cat_cmd(self, segments):
        return self.host.get_cmd('cat', segments)

    def get_receive_cmd(self):
        receive_args = ['receive', '-F', '-v', self.name]
        return self.host.get_cmd('zfs', receive_args)

    def get_split_cmd(self, prefix, split_size='1G', suffix_length=4):
        LOGGER.info('Splitting at segment size %s', split_size)
        split_args = [
            '--bytes=%s' % split_size,
            '--suffix-length=%s' % suffix_length,
            '--verbose',
            '-',
            prefix
        ]
        return self.host.get_cmd('split', split_args)

    def snapshot(self, label, recursive=False, ts=None):
        if ts is None:
            ts = datetime.utcnow()

        if label == '-':
            raise SnapshotException('\'%s\' is not a valid label' % label)

        timestamp = ts.strftime('%Y%m%dT%H%M%SZ')
        name = '%s@zfssnap_%s' % (self.name, timestamp)
        LOGGER.info('Creating snapshot %s (label: %s)', name, label)
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
        self.host.cache_add_snapshot(snapshot)
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
        # Make the list of snapshots a tuple to ensure it's not getting
        # changed while it's passed around.
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

        # Sort snapshots for less messy log output
        for snapshot in sorted(snapshots, key=attrgetter('datetime'), reverse=True):
            # There is no point in keeping failed replication snapshots
            if replication and snapshot.repl_status != 'success':
                keep_snapshots.discard(snapshot)

            if snapshot not in keep_snapshots:
                snapshot.destroy(recursive)
                continue

            LOGGER.debug('Keeping snapshot %s (reasons: %s)', snapshot.name,
                         ', '.join(snapshot.keep_reasons))


class Host(object):
    def __init__(self, cmds, ssh_params=None):
        self.cmds = cmds
        self.ssh_params = ssh_params
        self._filesystems = []
        self._snapshots = []
        self._dataset_properties = defaultdict(dict)
        self._refresh_snapshots_cache = True
        self._refresh_filesystems_cache = True
        self._refresh_properties_cache = True

    def _get_ssh_cmd(self):
        user = self.ssh_params['user']
        host = self.ssh_params.get('host', None)
        cmd = self.ssh_params['ssh']
        ssh_cmd = []

        if not host:
            return ssh_cmd

        if user:
            ssh_cmd = [cmd, '%s@%s' % (user, host)]
        else:
            ssh_cmd = [cmd, host]
        return ssh_cmd

    def get_cmd(self, name, args=None):
        if args is None:
            args = []

        try:
            cmd_path = self.cmds[name]
        except KeyError:
            raise ZFSSnapException(
                '\'%s\' is not defined.' % name)

        cmd = []
        if self.ssh_params:
            cmd.extend(self._get_ssh_cmd())

        cmd.append(cmd_path)
        cmd.extend(args)
        LOGGER.debug('Command: %s', ' '.join(cmd))
        return cmd

    def cache_refresh(self):
        self._refresh_properties_cache = True
        self._refresh_snapshots_cache = True
        self._refresh_filesystems_cache = True

    def _cache_refresh_properties(self):
        LOGGER.debug('Refreshing dataset properties cache')
        dataset_properties = defaultdict(dict)
        args = [
            'get', 'all',
            '-H',
            '-p',
            '-o', 'name,property,value',
        ]
        cmd = self.get_cmd('zfs', args)
        output = subprocess.check_output(cmd)

        for line in output.decode('utf8').split('\n'):
            if not line.strip():
                continue
            name, zfs_property, value = line.split('\t')
            dataset_properties[name][zfs_property] = autotype(value)

        self._dataset_properties = dataset_properties
        self._refresh_properties_cache = False

    def _cache_refresh_snapshots(self):
        LOGGER.debug('Refreshing snapshots cache')
        snapshots = []
        snapshot_pattern = r'^.+@zfssnap_[0-9]{8}T[0-9]{6}Z$'
        snapshot_re = re.compile(snapshot_pattern)
        all_datasets = self.get_properties_cached()

        for name, properties in all_datasets.items():
            if properties['type'] != 'snapshot':
                continue
            # Only keep zfssnap snapshots
            if not re.match(snapshot_re, name):
                continue
            snapshot = Snapshot(self, name)
            snapshots.append(snapshot)

        self._snapshots = snapshots
        self._refresh_snapshots_cache = False

    def _cache_refresh_filesystems(self):
        LOGGER.debug('Refreshing filesystems cache')
        filesystems = []
        all_datasets = self.get_properties_cached()

        for name, properties in all_datasets.items():
            if properties['type'] != 'filesystem':
                continue
            fs = Filesystem(self, name)
            filesystems.append(fs)

        self._filesystems = filesystems
        self._refresh_filesystems_cache = False

    def get_properties_cached(self, refresh=False):
        if refresh:
            self.cache_refresh()
        if refresh or self._refresh_properties_cache:
            self._cache_refresh_properties()
        return self._dataset_properties

    def cache_add_property(self, dataset, name, value):
        self._dataset_properties[dataset][name] = value

    def cache_remove_property(self, dataset, name):
        self._dataset_properties[dataset].pop(name, None)

    def cache_get_snapshots(self, refresh=False):
        if refresh:
            self.cache_refresh()
        if refresh or self._refresh_snapshots_cache:
            self._cache_refresh_snapshots()
        for snapshot in self._snapshots:
            yield snapshot

    def cache_add_snapshot(self, snapshot):
        LOGGER.debug('Adding %s to snapshot cache', snapshot.name)
        self._snapshots.append(snapshot)

    def cache_remove_snapshot(self, snapshot):
        LOGGER.debug('Removing %s from snapshot cache', snapshot.name)
        self._snapshots.remove(snapshot)
        self._dataset_properties.pop(snapshot.name)

    def cache_get_filesystems(self, refresh=False):
        if refresh:
            self.cache_refresh()
        if refresh or self._refresh_filesystems_cache:
            self._cache_refresh_filesystems()
        for filesystem in self._filesystems:
            yield filesystem

    def cache_remove_filesystem(self, fs):
        LOGGER.debug('Removing %s from filesystem cache', fs.name)
        self._filesystems.remove(fs)
        self._dataset_properties.pop(fs.name)

    def get_filesystems(self, include=None, exclude=None, recursive=False, refresh=False):
        if include is None:
            include = []
        if exclude is None:
            exclude = []
        if recursive:
            exclude.extend(['%s/*' % p for p in include])

        for fs in self.cache_get_filesystems(refresh):
            if any((fnmatch.fnmatch(fs.name, p) for p in exclude)):
                continue
            if include and not any((fnmatch.fnmatch(fs.name, p) for p in include)):
                continue
            yield fs

    def get_filesystem(self, name, refresh=False):
        for fs in self.cache_get_filesystems(refresh):
            if fs.name == name:
                return fs


class ZFSSnap(object):
    def __init__(self, config=None, lockfile=None):
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
            LOGGER.error('zfssnap aborted!')
        elif exc_type is not None:
            LOGGER.error(exc_value)

    def _aquire_lock(self, lockfile=None):
        if lockfile is None:
            lockfile = self.lockfile

        self._lock = open(lockfile, 'w')
        wait = 3
        timeout = 60

        while timeout > 0:
            try:
                fcntl.lockf(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                LOGGER.debug('Lock aquired')
                return
            except OSError:
                LOGGER.info('zfssnap is already running. Waiting for '
                            'lock release... (timeout: %ss)', timeout)
                timeout = timeout - wait
                time.sleep(wait)

        raise ZFSSnapException('Timeout reached. Could not aquire lock.')

    def _release_lock(self):
        fcntl.flock(self._lock, fcntl.LOCK_UN)
        LOGGER.debug('Lock released')

    @staticmethod
    def _get_metadata_files(src_dir, label, file_prefix=None):
        if file_prefix is None:
            file_prefix = 'zfssnap'

        metadata_pattern = r'^%s_[0-9]{8}T[0-9]{6}Z.json$' % file_prefix
        metadata_re = re.compile(metadata_pattern)

        for f in scandir(src_dir):
            if re.match(metadata_re, f.name):
                metadata = MetadataFile(f.path)
                metadata.read()

                if metadata.label != label:
                    LOGGER.warning(
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

    @staticmethod
    def _run_replication_cmd(in_cmd, out_cmd):
        LOGGER.debug('Replication command: \'%s | %s\'',
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
                LOGGER.info(line)
                lines.append(line)

        if out_p.returncode != 0:
            raise ReplicationException('Replication failed')

        return lines

    @staticmethod
    def _enforce_read_only(fs, read_only):
        if fs.read_only == 'on':
            LOGGER.info('%s is set to read only', fs.name)
            if not read_only:
                LOGGER.info('Removing read only from %s', fs.name)
                fs.read_only = None
        elif read_only:
            LOGGER.info('Setting %s to read only', fs.name)
            fs.read_only = 'on'

    def replicate(self, src_fs, dst_fs, label, base_snapshot, read_only=False):
        _base_snapshot = src_fs.get_base_snapshot(label, base_snapshot)
        snapshot = src_fs.snapshot(label, recursive=True)
        LOGGER.info('Replicating %s to %s', src_fs.name, dst_fs.name)
        send_cmd = src_fs.get_send_cmd(snapshot, _base_snapshot)
        receive_cmd = dst_fs.get_receive_cmd()
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
        # The snapshot list must be refreshed as the dst_fs snapshot cache
        # does not know that a new snapshot has arrived
        dst_snapshot = dst_fs.get_snapshot(snapshot.snapshot_name, refresh=True)
        dst_snapshot.repl_status = snapshot.repl_status
        self._enforce_read_only(dst_fs, read_only)

    @staticmethod
    def _cleanup_sync_files(metadata, src_dir):
        with contextlib.suppress(FileNotFoundError):
            for segment in metadata.segments:
                segment_path = os.path.join(src_dir, segment)
                os.remove(segment_path)

            os.remove(os.path.join(src_dir, metadata.path))

    @staticmethod
    def _get_segment_name(line, segments_log_re):
        match = re.match(segments_log_re, line)
        if match:
            segment = match.group(1)
            return os.path.basename(segment)

    @staticmethod
    def _get_segments(src_dir, metadata_segments):
        # Refresh file list for each run as each sync can potentially take
        # a long time
        src_file_names = {f.name for f in scandir(src_dir)}

        for segment in sorted(metadata_segments):
            if segment not in src_file_names:
                raise SegmentMissingException('Segment %s is missing in %s', segment, src_dir)

            yield os.path.join(src_dir, segment)

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

    def receive_from_file(self, dst_fs, label, src_dir, metadata, read_only=False):
        LOGGER.info('Selecting %s', metadata.path)

        # Make sure the cache is refreshed as the snapshot count might have
        # changed if multiple metadata files are processed in one run
        previous_snapshot = dst_fs.get_latest_repl_snapshot(label, refresh=True)

        if previous_snapshot and previous_snapshot.datetime >= metadata.datetime:
            LOGGER.warning('Ignoring %s as it is already applied or '
                           'older than the current snapshot', metadata.path)
            self._cleanup_sync_files(metadata, src_dir)
            return

        if metadata.depends_on and not dst_fs.get_snapshot(metadata.depends_on):
            raise ReplicationException(
                'The dependant snapshot %s does not exist on destination dataset %s' %
                (metadata.depends_on, dst_fs.name))

        segments = self._get_segments(src_dir, metadata.segments)
        cat_cmd = dst_fs.get_cat_cmd(segments)
        receive_cmd = dst_fs.get_receive_cmd()
        self._run_replication_cmd(cat_cmd, receive_cmd)

        # See comment in replicate()
        # Workaround for ZoL bug in initial replication fixed in 0.7.0?
        dst_snapshot = Snapshot(dst_fs.host, '%s@%s' % (dst_fs.name, metadata.snapshot))
        dst_snapshot.label = metadata.label
        dst_snapshot.version = metadata.version

        #dst_snapshot = self.get_snapshot(metadata.snapshot)
        dst_snapshot.repl_status = 'success'
        self._enforce_read_only(dst_fs, read_only)

        # Cleanup files after marking the sync as success as we don't
        # really care if this goes well for the sake of sync integrity
        self._cleanup_sync_files(metadata, src_dir)

    def send_to_file(self, src_fs, label, dst_dir, file_prefix='zfssnap', suffix_length=None,
                     split_size=None, base_snapshot=None):
        _base_snapshot = src_fs.get_base_snapshot(label, base_snapshot)
        snapshot = src_fs.snapshot(label, recursive=True)
        prefix = os.path.join(dst_dir, '%s_%s-' % (file_prefix, snapshot.timestamp))

        segments_log_pattern = r'^creating\sfile\s.*(%s[a-z]{%s}).*$' % (prefix, suffix_length)
        segments_log_re = re.compile(segments_log_pattern)

        send_cmd = src_fs.get_send_cmd(snapshot, _base_snapshot)
        split_cmd = src_fs.get_split_cmd(prefix, split_size, suffix_length)
        output = self._run_replication_cmd(send_cmd, split_cmd)
        segments = []

        for line in output:
            segment = self._get_segment_name(line, segments_log_re)

            if segment:
                segments.append(segment)

        LOGGER.info('Total segment count: %s', len(segments))

        # Ensure metadata file are written before repl_status are set to
        # 'success', so we are sure this end does not believe things are
        # ok and uses this snapshot as the base for the next sync while the
        # metadata file for the opposite end might not have been written
        metadata_file = os.path.join(dst_dir,
                                     '%s_%s.json' % (file_prefix, snapshot.timestamp))
        self._write_metadata_file(metadata_file, segments, snapshot, _base_snapshot)

        # See comment in replicate()
        snapshot.repl_status = 'success'

    def _run_snapshot_policy(self, policy, reset=False):
        if not reset:
            sleep = 1
            LOGGER.debug('Sleeping %ss to avoid potential snapshot name '
                         'collisions due to matching timestamps', sleep)
            time.sleep(sleep)

        policy_config = self.config.get_policy(policy)
        label = policy_config['label']
        host = Host(cmds=policy_config['cmds'])
        recursive = policy_config['recursive']
        datasets = host.get_filesystems(
            policy_config.get('include', None),
            policy_config.get('exclude', None),
            recursive)
        keep = policy_config['keep']
        self._aquire_lock()

        if reset:
            LOGGER.warning('Reset is enabled. Removing all snapshots for this policy')

        for dataset in datasets:
            if not reset:
                dataset.snapshot(label, recursive)

            dataset.enforce_retention(keep, label, recursive, reset)

        self._release_lock()

    def _run_replicate_policy(self, policy, reset=False, base_snapshot=None):
        if not reset:
            sleep = 1
            LOGGER.debug('Sleeping %ss to avoid potential snapshot name '
                         'collisions due to matching timestamps', sleep)
            time.sleep(sleep)

        policy_config = self.config.get_policy(policy)
        src_host = Host(policy_config['source']['cmds'])
        src_fs = src_host.get_filesystem(policy_config['source']['dataset'])

        ssh_params = dict()
        ssh_params['ssh'] = policy_config['source']['cmds']['ssh']
        ssh_params['user'] = policy_config['destination']['ssh_user']
        ssh_params['host'] = policy_config['destination']['host']

        dst_host = Host(policy_config['destination']['cmds'], ssh_params)
        dst_fs = dst_host.get_filesystem(policy_config['destination']['dataset'])

        label = policy_config['label']
        self._aquire_lock()

        if reset:
            LOGGER.warning('Reset is enabled. Reinitializing replication.')
            if dst_fs:
                LOGGER.warning('Destroying destination dataset')
                dst_fs.destroy(recursive=True)
        else:
            # If this is the first replication run the destination file system
            # might not exist
            if not dst_fs:
                dst_fs = Filesystem(dst_host, policy_config['destination']['dataset'])

            read_only = policy_config['destination']['read_only']
            self.replicate(src_fs, dst_fs, label, base_snapshot, read_only)

        keep = policy_config['keep']
        src_fs.enforce_retention(keep, label, recursive=True, reset=reset,
                                 replication=True)
        self._release_lock()

    def _run_receive_from_file_policy(self, policy, reset=False):
        policy_config = self.config.get_policy(policy)

        if not reset:
            src_dir = policy_config['source']['dir']
            label = policy_config['label']
            file_prefix = policy_config.get('file_prefix', None)
            metadata_files = [
                f for f in self._get_metadata_files(src_dir, label, file_prefix)]

            # Return early if no metadata files are found to avoid triggering
            # unnecessary cache refreshes against the host
            if not metadata_files:
                LOGGER.debug('No metadata files found in %s', src_dir)
                return

        dst_host = Host(policy_config['cmds'])
        dst_fs = dst_host.get_filesystem(policy_config['destination']['dataset'])

        self._aquire_lock()

        if reset:
            LOGGER.warning('Reset is enabled. Reinitializing replication.')
            if dst_fs:
                LOGGER.warning('Destroying destination dataset')
                dst_fs.destroy(recursive=True)
        else:
            # If this is the first replication run the destination file system
            # might not exist
            if not dst_fs:
                dst_fs = Filesystem(dst_host, policy_config['destination']['dataset'])

            read_only = policy_config['destination']['read_only']

            try:
                for metadata in sorted(metadata_files, key=attrgetter('datetime')):
                    self.receive_from_file(dst_fs, label, src_dir, metadata, read_only)
            except SegmentMissingException as e:
                LOGGER.info(e)

        self._release_lock()

    def _run_send_to_file_policy(self, policy, reset=False, base_snapshot=None):
        if not reset:
            sleep = 1
            LOGGER.debug('Sleeping %ss to avoid potential snapshot name '
                         'collisions due to matching timestamps', sleep)
            time.sleep(sleep)

        policy_config = self.config.get_policy(policy)
        label = policy_config['label']
        src_host = Host(policy_config['cmds'])
        src_fs = src_host.get_filesystem(policy_config['source']['dataset'])
        dst_dir = policy_config['destination']['dir']
        file_prefix = policy_config['file_prefix']
        suffix_length = policy_config['suffix_length']
        split_size = policy_config['split_size']
        keep = policy_config['keep']

        self._aquire_lock()

        if reset:
            LOGGER.warning('Reset is enabled. Reinitializing replication.')
            LOGGER.warning('Cleaning up source replication snapshots')
        else:
            self.send_to_file(src_fs, label, dst_dir, file_prefix, suffix_length,
                              split_size, base_snapshot)

        src_fs.enforce_retention(keep, label, recursive=True, reset=reset,
                                 replication=True)
        self._release_lock()

    @staticmethod
    def _print_header(text):
        print('%s' % text)
        print('-' * len(text))

    def _print_datasets(self, datasets, header='DATASETS'):
        self._print_header(header)
        for dataset in sorted(datasets, key=attrgetter('name')):
            print(dataset.name)

    def _print_snapshots(self, datasets, label, header='SNAPSHOTS'):
        self._print_header(header)
        for dataset in sorted(datasets, key=attrgetter('name')):
            snapshots = dataset.get_snapshots(label)
            for snapshot in sorted(snapshots, key=attrgetter('name')):
                print(snapshot.name)

    def _print_config(self, config):
        self._print_header('POLICY CONFIG')
        print(yaml.dump(config, default_flow_style=False))

    def _list_snapshot_policy(self, policy, list_mode):
        policy_config = self.config.get_policy(policy)
        label = policy_config['label']
        host = Host(policy_config['cmds'])
        recursive = policy_config['recursive']
        datasets = [
            d for d in host.get_filesystems(
                policy_config.get('include', None),
                policy_config.get('exclude', None),
                recursive)
        ]

        if list_mode == 'config':
            self._print_config(policy_config)
        if list_mode == 'datasets':
            self._print_datasets(datasets)
        if list_mode == 'snapshots':
            self._print_snapshots(datasets, label)

    def _list_send_to_file_policy(self, policy, list_mode):
        policy_config = self.config.get_policy(policy)
        label = policy_config['label']
        src_host = Host(policy_config['cmds'])
        src_dataset = src_host.get_filesystem(policy_config['source']['dataset'])

        if list_mode == 'config':
            self._print_config(policy_config)
        if list_mode == 'datasets':
            self._print_datasets([src_dataset], 'SOURCE DATASET')
        if list_mode == 'snapshots':
            self._print_snapshots([src_dataset], label, 'SOURCE SNAPSHOTS')

    def _list_receive_from_file_policy(self, policy, list_mode):
        policy_config = self.config.get_policy(policy)
        label = policy_config['label']
        dst_host = Host(policy_config['cmds'])
        dst_dataset = dst_host.get_filesystem(policy_config['destination']['dataset'])

        if dst_dataset:
            dst_datasets = [dst_dataset]
        else:
            dst_datasets = []

        if list_mode == 'config':
            self._print_config(policy_config)
        if list_mode == 'datasets':
            self._print_datasets(dst_datasets, 'DESTINATION DATASET')
        if list_mode == 'snapshots':
            self._print_snapshots(dst_datasets, label, 'DESTINATION SNAPSHOTS')


    def _list_replicate_policy(self, policy, list_mode):
        policy_config = self.config.get_policy(policy)
        label = policy_config['label']
        src_host = Host(policy_config['source']['cmds'])
        src_dataset = src_host.get_filesystem(policy_config['source']['dataset'])

        ssh_params = dict()
        ssh_params['ssh'] = policy_config['source']['cmds']['ssh']
        ssh_params['user'] = policy_config['destination']['ssh_user']
        ssh_params['host'] = policy_config['destination']['host']

        dst_host = Host(policy_config['destination']['cmds'], ssh_params)
        dst_dataset = dst_host.get_filesystem(policy_config['destination']['dataset'])

        if dst_dataset:
            dst_datasets = [dst_dataset]
        else:
            dst_datasets = []

        if list_mode == 'config':
            self._print_config(policy_config)
        if list_mode == 'datasets':
            self._print_datasets([src_dataset], 'SOURCE DATASET')
            self._print_datasets(dst_datasets, '\nDESTINATION DATASET')
        if list_mode == 'snapshots':
            self._print_snapshots([src_dataset], label, 'SOURCE SNAPSHOTS')
            self._print_snapshots(dst_datasets, label, '\nDESTINATION SNAPSHOTS')

    def execute_policy(self, policy, list_mode=None, reset=False, base_snapshot=None):
        policy_type = self.config.get_policy(policy)['type']

        if policy_type == 'snapshot':
            if list_mode:
                self._list_snapshot_policy(policy, list_mode)
            else:
                self._run_snapshot_policy(policy, reset)
        elif policy_type == 'replicate':
            if list_mode:
                self._list_replicate_policy(policy, list_mode)
            else:
                self._run_replicate_policy(policy, reset, base_snapshot)
        elif policy_type == 'send_to_file':
            if list_mode:
                self._list_send_to_file_policy(policy, list_mode)
            else:
                self._run_send_to_file_policy(policy, reset, base_snapshot)
        elif policy_type == 'receive_from_file':
            if list_mode:
                self._list_receive_from_file_policy(policy, list_mode)
            else:
                self._run_receive_from_file_policy(policy, reset)
        else:
            raise ZFSSnapException('%s is not a valid policy type' % policy_type)


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
    mutex_group2.add_argument('--list', help='List policy information',
                              choices=[
                                  'snapshots',
                                  'datasets',
                                  'config',
                              ])
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

    try:
        with ZFSSnap(config=args.config, lockfile=args.lockfile) as z:
            z.execute_policy(args.policy, args.list, args.reset, args.base_snapshot)
    except ZFSSnapException:
        return 10
    except ReplicationException:
        return 11
    except SnapshotException:
        return 12
    except SegmentMissingException:
        return 13
    except ConfigException:
        return 14
    except MetadataFileException:
        return 15
    except KeyboardInterrupt:
        return 130

if __name__ == '__main__':
    sys.exit(main())
