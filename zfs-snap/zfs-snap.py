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

    def _get_all_fs(self):
        output = subprocess.check_output([
            self.zfs, 'list', '-H',
            '-o', 'name,zol:zfs-snap,zol:zfs-snap:%s,'
                  'zol:zfs-snap:keep,zol:zfs-snap:%s:keep' %
                      (self.label, self.label),
            '-t', 'filesystem'
        ])

        for line in output.decode('utf8').split('\n'):
            line = line.strip()

            if line:
                name, fs_enable, label_enable, fs_keep, label_keep = line.split('\t')

                # The label property toggling snapshots have priority over
                # the global file system property if they are different.
                enable_snapshots = True

                if fs_enable.lower() == 'true':
                    enable_snapshots = True
                elif fs_enable.lower() == 'false':
                    enable_snapshots = False

                if label_enable.lower() == 'true':
                    enable_snapshots = True
                elif label_enable.lower() == 'false':
                    enable_snapshots = False

                # Use the keep value given by command line, unless overriden
                # either globally or per label by ZFS properties.
                # Per label is prioritized over the global setting. If --force
                # is given by command line the command line value will be used.
                keep = self.keep

                if fs_keep != '-':
                    keep = fs_keep

                if label_keep != '-':
                    keep = label_keep

                if self.force:
                    keep = self.keep

                yield {
                    'name': name,
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
                fs, id = name.split('@')

                if label == self.label:
                    yield {
                        'name': name,
                        'fs': fs,
                        'id': id
                    }

    def _create_snapshot(self, fs):
        timestamp = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        name = '%s@zfs-snap_%s' % (fs, timestamp)
        logger.info('Creating snapshot %s', name)

        subprocess.check_call([
            self.zfs, 'snapshot', '-o', 'zol:zfs-snap:label=%s' %
                self.label, name])

    def create_snapshots(self):
        for fs in self._get_all_fs():
            if not fs['enable_snapshots']:
                continue

            if fs['keep'] < 1:
                continue

            self._create_snapshot(fs['name'])

    @staticmethod
    def _destroy_snapshot(name):
        logger.info('Destroying snapshot %s', name)
        subprocess.check_call([self.zfs, 'destroy', name])

    def destroy_old_snapshots(self):
        snapshots = [s for s in self._get_all_snapshots()]

        for fs in self._get_all_fs():
            if not fs['enable_snapshots']:
                keep = 0
            else:
                keep = fs['keep']

            fs_snapshots = [s['name'] for s in snapshots if s['fs'] == fs['name']]

            for snapshot in sorted(fs_snapshots, reverse=True)[keep:]:
                self._destroy_snapshot(snapshot)


def main():
    parser = argparse.ArgumentParser(
        description='Automatic snapshotting for ZFS on Linux')
    parser.add_argument('-q', '--quiet', help='Suppress output from script.',
                        action='store_true')
    parser.add_argument('-l', '--label', help='Snapshot label.',
                        required=True)
    parser.add_argument('-k', '--keep', help='Number of snapshots to keep.',
                        required=True)
    parser.add_argument('-f', '--force',
                        help='Override ZFS property keep value if set',
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
    args = parser.parse_args()

    if not args.quiet:
        fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
        ch = logging.StreamHandler()
        ch.setLevel(args.verbosity)
        ch.setFormatter(fmt)
        logger.addHandler(ch)

    try:
        with ZFSSnap(args.label, args.keep, args.force) as z:
            z.create_snapshots()
            z.destroy_old_snapshots()
    except KeyboardInterrupt:
        sys.exit(2)

if __name__ == '__main__':
    main()
