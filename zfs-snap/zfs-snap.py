#!/usr/bin/env python3

import argparse
import logging
import sys
import subprocess
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

class ZFSSnap(object):
    def __init__(self, label, keep, force):
        self.label = label
        self.keep = keep
        self.force = force
        self.zfs = '/sbin/zfs'

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is KeyboardInterrupt:
            logger.error('zfs-snap aborted!')
        elif exc_type is not None:
            logger.error(exc_value)

    def _get_all_fs(self, file_system=None):
        zfs_fs_cmd = [
            self.zfs, 'list', '-H', '-p',
            '-o', 'name,used,avail,zol:zfs-snap,zol:zfs-snap:%s,'
                  'zol:zfs-snap:keep,zol:zfs-snap:%s:keep' %
                      (self.label, self.label),
            '-t', 'filesystem'
        ]

        if file_system:
            zfs_fs_cmd.append(file_system)

        output = subprocess.check_output(zfs_fs_cmd)

        for line in output.decode('utf8').split('\n'):
            line = line.strip()

            if line:
                zfs_values = line.split('\t')
                zfs_info = dict()
                zfs_info['name'] = zfs_values[0]
                zfs_info['used'] = int(zfs_values[1])
                zfs_info['avail'] = int(zfs_values[2])
                zfs_info['fs_enable'] = zfs_values[3]
                zfs_info['label_enable'] = zfs_values[4]
                zfs_info['fs_keep'] = zfs_values[5]
                zfs_info['label_keep'] = zfs_values[6]

                # The label property toggling snapshots have priority over
                # the global file system property if they are different.
                if zfs_info['label_enable'].lower() == 'true':
                    enable_snapshots = True
                elif zfs_info['label_enable'].lower() == 'false':
                    enable_snapshots = False
                elif zfs_info['fs_enable'].lower() == 'true':
                    enable_snapshots = True
                elif zfs_info['fs_enable'].lower() == 'false':
                    enable_snapshots = False
                else:
                    enable_snapshots = True

                # Use the keep value given by command line, unless overriden
                # either globally or per label by ZFS properties.
                # Per label is prioritized over the global setting. If --force
                # is given by command line the command line value will be used.
                if self.force:
                    keep = self.keep
                elif zfs_info['label_keep'] != '-':
                    keep = zfs_info['label_keep']
                elif zfs_info['fs_keep'] != '-':
                    keep = zfs_info['fs_keep']
                else:
                    keep = self.keep

                percent_free = (zfs_info['avail'] /
                                (zfs_info['avail'] + zfs_info['used']) * 100)

                yield {
                    'name': zfs_info['name'],
                    'percent_free': percent_free,
                    'enable_snapshots': enable_snapshots,
                    'keep': int(keep),
                }

    def _get_all_snapshots(self):
        output = subprocess.check_output([
            self.zfs, 'list', '-H',
            '-o', 'name,zol:zfs-snap:label', '-t', 'snapshot'
        ])

        for line in output.decode('utf8').split('\n'):
            line = line.strip()

            if line:
                name, label = line.split('\t')
                fs, _ = name.split('@')

                if label == self.label:
                    yield {
                        'name': name,
                        'fs': fs
                    }

    def _create_snapshot(self, fs):
        timestamp = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        name = '%s@zfs-snap_%s' % (fs, timestamp)
        logger.info('Creating snapshot %s', name)

        subprocess.check_call([
            self.zfs, 'snapshot', '-o', 'zol:zfs-snap:label=%s' %
                self.label, name])

    def create_snapshots(self, file_system, min_free, min_keep):
        for fs in self._get_all_fs(file_system):
            if not fs['enable_snapshots']:
                continue

            if fs['keep'] < 1:
                continue

            create_snapshot=True

            if fs['percent_free'] < min_free:
                logger.warning('There is only %s%% free space on %s '
                               '[min-free: %s%%]. Trying to delete old '
                               'snapshots to free space.',
                               round(fs['percent_free'], 1), fs['name'],
                               min_free)

                while fs['percent_free'] < min_free:
                    if not self._destroy_oldest_snapshot(fs['name'], min_keep):
                        logger.error('Could not free enough space. Aborting.')
                        create_snapshot=False
                        break

            if create_snapshot:
                self._create_snapshot(fs['name'])

    def _destroy_snapshot(self, name):
        logger.info('Destroying snapshot %s', name)
        subprocess.check_call([self.zfs, 'destroy', name])

    def _destroy_oldest_snapshot(self, fs, min_keep):
        snapshots = sorted([s['name'] for s in self._get_all_snapshots()
                           if s['fs'] == fs], reverse=True)[min_keep:]

        if not snapshots:
            return False

        # Return after the first element
        for snapshot in sorted(snapshots, reverse=False):
            self._destroy_snapshot(snapshot)
            return True

    def destroy_old_snapshots(self, file_system):
        for fs in self._get_all_fs(file_system):
            if not fs['enable_snapshots']:
                keep = 0
            else:
                keep = fs['keep']

            snapshots = [s['name'] for s in self._get_all_snapshots()
                            if s['fs'] == fs['name']]

            for snapshot in sorted(snapshots, reverse=True)[keep:]:
                self._destroy_snapshot(snapshot)


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
        logger.addHandler(ch)

    try:
        with ZFSSnap(args.label, args.keep, args.force) as z:
            z.create_snapshots(args.file_system, args.min_free, args.min_keep)
            z.destroy_old_snapshots(args.file_system)
    except KeyboardInterrupt:
        sys.exit(2)

if __name__ == '__main__':
    main()
