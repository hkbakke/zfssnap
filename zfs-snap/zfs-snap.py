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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is KeyboardInterrupt:
            logger.error('zfs-snap aborted!')
        elif exc_type is not None:
            logger.error(exc_value)

    def _get_all_fs(self):
        output = subprocess.check_output([
            'zfs', 'list', '-H',
            '-o', 'name,zol:zfs-snap,zol:zfs-snap:%s,'
                  'zol:zfs-snap:%s:keep' % (self.label, self.label),
            '-t', 'filesystem'
        ])

        for line in output.decode('utf8').split('\n'):
            line = line.strip()

            if line:
                name, global_enable, label_enable, keep = line.split('\t')
                true_values = set([
                    '-',
                    'true'
                ])

                if global_enable.lower() in true_values:
                    global_enable = True
                else:
                    global_enable = False

                if label_enable.lower() in true_values:
                    label_enable = True
                else:
                    label_enable = False

                if keep == '-' or self.force:
                    keep = self.keep

                yield {
                    'name': name,
                    'global_enable': global_enable,
                    'label_enable': label_enable,
                    'keep': int(keep)
                }

    def _get_all_snapshots(self):
        output = subprocess.check_output([
            'zfs', 'list', '-H',
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
            'zfs', 'snapshot', '-o', 'zol:zfs-snap:label=%s' %
                self.label, name])

    def create_snapshots(self):
        for fs in self._get_all_fs():
            if not fs['global_enable'] or not fs['label_enable']:
                continue

            if fs['keep'] < 1:
                continue

            self._create_snapshot(fs['name'])

    @staticmethod
    def _destroy_snapshot(name):
        logger.info('Destroying snapshot %s', name)
        subprocess.check_call(['zfs', 'destroy', name])

    def destroy_old_snapshots(self):
        snapshots = [s for s in self._get_all_snapshots()]

        for fs in self._get_all_fs():
            if not fs['global_enable'] or not fs['label_enable']:
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
    parser.add_argument('-l', '--label', help='Snapshot label',
                        required=True)
    parser.add_argument('-k', '--keep', help='Number of snapshots to keep',
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
