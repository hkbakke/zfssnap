#!/usr/bin/env python3

import argparse
import logging
import sys
import subprocess
import re
from datetime import datetime
from operator import attrgetter


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.DEBUG)
ZFS = '/sbin/zfs'


class ZFSSnapshot(object):
    def __init__(self, name):
        self.name = name

    def create(self, label):
        LOGGER.info('Creating snapshot %s', self.name)

        subprocess.check_call([ZFS, 'snapshot', '-o',
                               'zol:zfs-snap:label=%s' % label, self.name])

    def destroy(self):
        LOGGER.info('Destroying snapshot %s', self.name)
        subprocess.check_call([ZFS, 'destroy', self.name])

    @property
    def datetime(self):
        _, name = self.name.split('@')
        name = re.sub(r'Z$', '+0000', name)
        return datetime.strptime(name, 'zfs-snap_%Y%m%dT%H%M%S%z')


class ZFSFileSystem(object):
    def __init__(self, name):
        self.name = name
        self._properties = dict()

    @staticmethod
    def _autoconvert(value):
        for fn in [int]:
            try:
                return fn(value)
            except ValueError:
                pass

        return value

    def snapshots_enabled(self, label):
        properties = self.get_properties()
        enabled = True

        if 'zol:zfs-snap:%s' % label in properties:
            value = properties['zol:zfs-snap:%s' % label].lower()

            if value == 'on':
                enabled = True
            elif value == 'off':
                enabled = False
        elif 'zol:zfs-snap' in properties:
            value = properties['zol:zfs-snap'].lower()

            if value == 'on':
                enabled = True
            elif value == 'off':
                enabled = False

        return enabled

    def get_keep(self, label):
        properties = self.get_properties()

        if 'zol:zfs-snap:%s:keep' % label in properties:
            keep = properties['zol:zfs-snap:%s:keep' % label]
        elif 'zol:zfs-snap:keep' in properties:
            keep = properties['zol:zfs-snap:keep' % label]
        else:
            keep = None

        return keep

    def get_properties(self, refresh=False):
        if refresh or not self._properties:
            cmd = [ZFS, 'get', 'all', '-H', '-p', '-o', 'property,value',
                   self.name]
            output = subprocess.check_output(cmd)
            properties = dict()

            for line in output.decode('utf8').split('\n'):
                if line.strip():
                    zfs_property, value = line.split('\t')
                    properties[zfs_property] = self._autoconvert(value)

            self._properties = properties

        return self._properties

    @property
    def percent_free(self):
        properties = self.get_properties(refresh=True)

        return (properties['available']
                / (properties['available'] + properties['used'])
                * 100)

    def get_snapshots(self, label):
        output = subprocess.check_output([
            ZFS, 'list', '-H', '-o', 'name,zol:zfs-snap:label', '-d 1', '-t',
            'snapshot', self.name
        ])

        for line in output.decode('utf8').split('\n'):
            if line.strip():
                name, snapshot_label = line.split('\t')

                if snapshot_label == label:
                    yield ZFSSnapshot(name)

    def create_snapshot(self, label, min_free, min_keep):
        if not self.snapshots_enabled(label):
            return None

        if self.percent_free < min_free:
            LOGGER.warning('There is only %s%% free space on %s '
                           '[min-free: %s%%]. Trying to delete old '
                           'snapshots to free space.',
                           round(self.percent_free, 1), self.name, min_free)

            while self.percent_free < min_free:
                if not self.destroy_old_snapshots(label, min_keep, limit=1):
                    LOGGER.error('Could not free enough space. Aborting.')
                    return None

        timestamp = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        name = '%s@zfs-snap_%s' % (self.name, timestamp)
        s = ZFSSnapshot(name)
        s.create(label)

        return s

    def destroy_old_snapshots(self, label, keep, limit=None):
        # Delete all snapshots if snapshots are disabled
        if not self.snapshots_enabled(label):
            keep = 0

        snapshots = sorted(self.get_snapshots(label),
                           key=attrgetter('datetime'),
                           reverse=True)[keep:]
        destroyed_snapshots = list()

        for snapshot in sorted(snapshots, key=attrgetter('datetime'),
                               reverse=False):
            if limit and len(destroyed_snapshots) >= limit:
                break

            snapshot.destroy()
            destroyed_snapshots.append(snapshot)

        return destroyed_snapshots


class ZFSSnap(object):
    def __init__(self, label):
        self.label = label

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is KeyboardInterrupt:
            LOGGER.error('zfs-snap aborted!')
        elif exc_type is not None:
            LOGGER.error(exc_value)

    @staticmethod
    def get_all_fs(file_system=None):
        cmd = [ZFS, 'list', '-H', '-p', '-o', 'name', '-t', 'filesystem']

        if file_system:
            cmd.append(file_system)

        output = subprocess.check_output(cmd)

        for name in output.decode('utf8').split('\n'):
            name = name.strip()

            if name:
                yield ZFSFileSystem(name)

    def _get_keep(self, fs, keep, force):
        # Use the keep value given by command line, unless overriden
        # either globally or per label by ZFS properties.
        # Per label is prioritized over the global setting. If --force
        # is given by command line the command line value will be used,
        # regardless of ZFS properties
        fs_keep = fs.get_keep(self.label)

        if force:
            runtime_keep = keep
        elif fs_keep:
            runtime_keep = fs_keep
        else:
            runtime_keep = keep

        return runtime_keep

    def run(self, keep, min_free, min_keep, file_system=None, force=None):
        for fs in self.get_all_fs(file_system):
            runtime_keep = self._get_keep(fs, keep, force)

            if runtime_keep > 0:
                fs.create_snapshot(self.label, min_free, min_keep)

            fs.destroy_old_snapshots(self.label, runtime_keep)


def main():
    parser = argparse.ArgumentParser(
        description='Automatic snapshotting for ZFS on Linux')
    parser.add_argument('-f', '--force',
                        help='Override ZFS property keep value if set',
                        action='store_true')
    parser.add_argument('-k', '--keep', help='Number of snapshots to keep.',
                        type=int, required=True)
    parser.add_argument('-l', '--label', help='Snapshot label.',
                        required=True)
    parser.add_argument('-m', '--min-free',
                        help='Minimum free space in percent required to create '
                             'new snapshots. (default: %(default)s)',
                        type=int, default=0)
    parser.add_argument('-e', '--min-keep',
                        help='Minimum number of old snapshots to keep if '
                             '--min-free is exceeded. (default: %(default)s)',
                        type=int, default=1)
    parser.add_argument('-q', '--quiet', help='Suppress output from script.',
                        action='store_true')
    parser.add_argument('-v', '--verbosity',
                        choices=[
                            'CRITICAL',
                            'ERROR',
                            'WARNING',
                            'INFO',
                            'DEBUG'
                        ],
                        default='DEBUG',
                        help='Set log level for console output.')
    parser.add_argument('-z', '--file-system',
                        help='Select specific file system.')
    args = parser.parse_args()

    if not args.quiet:
        fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
        ch = logging.StreamHandler()
        ch.setLevel(args.verbosity)
        ch.setFormatter(fmt)
        LOGGER.addHandler(ch)

    try:
        with ZFSSnap(args.label) as z:
            z.run(args.keep, args.min_free, args.min_keep, args.file_system,
                  args.force)
    except KeyboardInterrupt:
        sys.exit(2)

if __name__ == '__main__':
    main()
