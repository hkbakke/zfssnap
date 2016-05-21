#!/usr/bin/env python3

import argparse
import logging
import sys
import subprocess
import re
from datetime import datetime
from operator import attrgetter
import fcntl
import time


class ZFSReplicationException(Exception):
    pass


class ZFSSnapException(Exception):
    pass


class ZFSSnapshot(object):
    def __init__(self, host, name):
        self.name = name
        self.host = host
        self.logger = logging.getLogger(__name__)

    def create(self, label):
        self.logger.info('Creating snapshot %s', self.name)
        cmd = self.host.cmd.copy()
        cmd.extend([
            'snapshot',
            '-o', 'zol:zfssnap:label=%s' % label,
            self.name
        ])
        subprocess.check_call(cmd)

    def destroy(self):
        self.logger.info('Destroying snapshot %s', self.name)
        cmd = self.host.cmd.copy()
        cmd.extend([
            'destroy',
            self.name
        ])
        subprocess.check_call(cmd)

    @property
    def snapname(self):
        _, snapname = self.name.split('@')
        return snapname

    @property
    def datetime(self):
        _, snapname = self.name.split('@')
        strptime_name = re.sub(r'Z$', '+0000', snapname)
        return datetime.strptime(strptime_name, 'zfssnap_%Y%m%dT%H%M%S%z')


class ZFSFileSystem(object):
    def __init__(self, host, name):
        self.name = name
        self.host = host
        self._properties = dict()
        self.logger = logging.getLogger(__name__)

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
        value = ''

        if 'zol:zfssnap:%s' % label in properties:
            value = properties['zol:zfssnap:%s' % label]
        elif 'zol:zfssnap' in properties:
            value = properties['zol:zfssnap']

        if value.lower() == 'on':
            return True
        elif value.lower() == 'off':
            self.logger.debug('%s: Snapshots are disabled by ZFS properties. '
                              'Use --override to ignore the properties.',
                              self.name)
            return False

        return None

    def get_keep(self, label):
        properties = self.get_properties()

        if 'zol:zfssnap:%s:keep' % label in properties:
            keep = properties['zol:zfssnap:%s:keep' % label]
        elif 'zol:zfssnap:keep' in properties:
            keep = properties['zol:zfssnap:keep' % label]
        else:
            keep = None

        return keep

    def get_properties(self, refresh=False):
        if refresh or not self._properties:
            cmd = self.host.cmd.copy()
            cmd.extend([
                'get', 'all',
                '-H',
                '-p',
                '-o', 'property,value',
                self.name
            ])
            output = subprocess.check_output(cmd)
            properties = dict()

            for line in output.decode('utf8').split('\n'):
                if line.strip():
                    zfs_property, value = line.split('\t')
                    properties[zfs_property] = self._autoconvert(value)

            self._properties = properties

        return self._properties

    def get_latest_snapshot(self):
        snapshots = sorted(self.get_snapshots(),
                           key=attrgetter('datetime'),
                           reverse=True)

        return next(iter(snapshots), None)

    def snapshot_exists(self, name):
        if '@' in name:
            for s in self.get_snapshots():
                if s.name == name:
                    return True
        else:
            for s in self.get_snapshots():
                if s.snapname == name:
                    return True

        return False

    def get_snapshots(self, label=None):
        cmd = self.host.cmd.copy()
        cmd.extend([
            'list',
            '-H',
            '-o', 'name,zol:zfssnap:label',
            '-d', '1',
            '-t', 'snapshot',
            self.name
        ])
        output = subprocess.check_output(cmd)

        for line in output.decode('utf8').split('\n'):
            if line.strip():
                name, snapshot_label = line.split('\t')

                if not label or snapshot_label == label:
                    yield ZFSSnapshot(self.host, name)

    def replicate(self, snapshot, target_fs):
        try:
            latest_s = target_fs.get_latest_snapshot()
        except subprocess.CalledProcessError:
            latest_s = None

        if latest_s and self.snapshot_exists(latest_s.snapname):
            send_cmd = self.host.cmd.copy()
            send_cmd.extend([
                'send',
                '-R',
                '-i', '@%s' % latest_s.snapname,
                snapshot.name
            ])
        else:
            send_cmd = self.host.cmd.copy()
            send_cmd.extend([
                'send',
                '-R',
                snapshot.name
            ])

        receive_cmd = target_fs.host.cmd.copy()
        receive_cmd.extend([
            'receive',
            '-F',
            '-v',
            target_fs.name
        ])

        self.logger.debug('Replicate cmd: \'%s | %s\'', ' '.join(send_cmd),
                          ' '.join(receive_cmd))
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
            if line:
                self.logger.info(line)

        if receive.returncode != 0:
            raise ZFSReplicationException('Replication failed!')

    def create_snapshot(self, label, ts=None):
        if ts is None:
            ts = datetime.utcnow()

        timestamp = ts.strftime('%Y%m%dT%H%M%SZ')
        name = '%s@zfssnap_%s' % (self.name, timestamp)
        s = ZFSSnapshot(self.host, name)
        s.create(label)

        return s

    def destroy_old_snapshots(self, label, keep, limit=None):
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


class ZFSHost(object):
    def __init__(self, ssh_user=None, ssh_host=None, zfs_cmd=None,
                 ssh_cmd=None):
        if zfs_cmd is None:
            zfs_cmd = '/sbin/zfs'

        if ssh_cmd is None:
            ssh_cmd = '/usr/bin/ssh'

        self.zfs_cmd = zfs_cmd
        self.ssh_cmd = ssh_cmd
        self.ssh_user = ssh_user
        self.ssh_host = ssh_host

    @property
    def cmd(self):
        if self.ssh_cmd and self.ssh_user and self.ssh_host:
            cmd = [
                self.ssh_cmd,
                '%s@%s' % (self.ssh_user, self.ssh_host),
                self.zfs_cmd
            ]
        else:
            cmd = [self.zfs_cmd]

        return cmd

    def get_file_systems(self, file_systems=None):
        cmd = self.cmd.copy()
        cmd.extend([
            'list',
            '-H',
            '-p',
            '-o', 'name',
            '-t', 'filesystem'
        ])
        output = subprocess.check_output(cmd)

        for name in output.decode('utf8').split('\n'):
            if name.strip():
                if not file_systems or name in file_systems:
                    yield ZFSFileSystem(host=self, name=name)

    def get_file_system(self, file_system):
        return next(self.get_file_systems([file_system]), None)


class ZFSSnap(object):
    def __init__(self, lockfile=None):
        self.logger = logging.getLogger(__name__)

        # The lock file object needs to be at class level for not to be
        # garbage collected after the _aquire_lock function has finished.
        self._lock_f = None
        self._aquire_lock(lockfile)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is KeyboardInterrupt:
            self.logger.error('zfssnap aborted!')
        elif exc_type is not None:
            self.logger.error(exc_value)

    def _aquire_lock(self, lockfile=None):
        if lockfile is None:
            lockfile = '/run/lock/zfssnap.lock'

        self._lock_f = open(lockfile, 'w')

        while True:
            try:
                fcntl.lockf(self._lock_f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                self.logger.debug('Lock aquired.')
                return
            except OSError:
                self.logger.info('zfssnap is already running. Waiting for '
                                 'lock release...')
                time.sleep(3)

    @staticmethod
    def _parse_fs_location(name):
        ssh_user = None
        ssh_host = None
        fs_name = None

        if '@' in name:
            ssh_user, tail = name.split('@', 1)
            ssh_host, fs_name = tail.split(':', 1)
        else:
            fs_name = name

        return (fs_name, ssh_user, ssh_host)

    def _get_fs_params(self, fs_location, zfs_cmd=None, ssh_cmd=None):
        fs_name, ssh_user, ssh_host = self._parse_fs_location(fs_location)
        host = ZFSHost(zfs_cmd=zfs_cmd, ssh_cmd=ssh_cmd,
                       ssh_user=ssh_user, ssh_host=ssh_host)

        return (fs_name, host)

    def replicate(self, keep, label, src_fs_location, dst_fs_location,
                  src_zfs_cmd=None, dst_zfs_cmd=None, ssh_cmd=None):
        if keep < 1:
            raise ZFSReplicationException(
                'Replication needs a keep value of at least 1.')

        src_fs_name, src_host = self._get_fs_params(fs_location=src_fs_location,
                                                    zfs_cmd=src_zfs_cmd,
                                                    ssh_cmd=ssh_cmd)
        src_fs = src_host.get_file_system(src_fs_name)

        if src_fs is None:
            raise ZFSReplicationException(
                'The source file system %s does not exist.' % src_fs_location)

        dst_fs_name, dst_host = self._get_fs_params(fs_location=dst_fs_location,
                                                    zfs_cmd=dst_zfs_cmd,
                                                    ssh_cmd=ssh_cmd)
        dst_fs = ZFSFileSystem(host=dst_host, name=dst_fs_name)

        snapshot = src_fs.create_snapshot(label)
        src_fs.replicate(snapshot, dst_fs)
        src_fs.destroy_old_snapshots(label, keep)

    def snapshot(self, keep, label, fs_locations=None, zfs_cmd=None,
                 ssh_cmd=None, override=False, default_exclude=False):
        if fs_locations is None:
            fs_locations = ['_all']

        file_systems = []

        for fs_loc in fs_locations:
            fs_name, host = self._get_fs_params(fs_location=fs_loc,
                                                zfs_cmd=zfs_cmd,
                                                ssh_cmd=ssh_cmd)

            if fs_name == '_all':
                file_systems.extend(host.get_file_systems())
            else:
                file_systems.append(host.get_file_system(fs_name))

        for fs in file_systems:
            if override:
                snapshots_enabled = True
                fs_keep = keep
            else:
                snapshots_enabled = not default_exclude
                fs_snapshots_enabled = fs.snapshots_enabled(label)

                if fs_snapshots_enabled is not None:
                    snapshots_enabled = fs_snapshots_enabled

                fs_keep = fs.get_keep(label)

                if fs_keep is None:
                    fs_keep = keep

            if snapshots_enabled and fs_keep > 0:
                fs.create_snapshot(label)

            if override or snapshots_enabled:
                fs.destroy_old_snapshots(label, fs_keep)


def main():
    parser = argparse.ArgumentParser(
        description='Automatic snapshotting and replication for ZFS on Linux')
    subparsers = parser.add_subparsers(dest='subparser')

    # Replication specific arguments
    replicate_parser = subparsers.add_parser(
        'replicate', help='Replication sub-commands')
    replicate_parser.add_argument(
        '--src-file-system', metavar='FILE_SYSTEM', required=True,
        help='File system to replicate')
    replicate_parser.add_argument(
        '--dst-file-system', metavar='FILE_SYSTEM', required=True,
        help='File system to replicate to')
    replicate_parser.add_argument(
        '--src-zfs-cmd', metavar='PATH',
        help='Override path to source zfs executable')
    replicate_parser.add_argument(
        '--dst-zfs-cmd', metavar='PATH',
        help='Override path to destination zfs executable')
    replicate_parser.add_argument(
        '--keep', metavar='INT', type=int, required=True,
        help='Number of snapshots to keep')
    replicate_parser.add_argument(
        '--label', required=True, help='Snapshot label')
    replicate_parser.add_argument(
        '--ssh-cmd', metavar='PATH', help='Override path to ssh executable')

    # Snapshot specific arguments
    snapshot_parser = subparsers.add_parser(
        'snapshot', help='Snapshot sub-commands')
    snapshot_parser.add_argument(
        '--file-systems', nargs='+', help='Select specific file systems.')
    snapshot_parser.add_argument(
        '--override', action='store_true',
        help='Ignore ZFS properties and use command line arguments')
    snapshot_parser.add_argument(
        '--default-exclude', action='store_true',
        help='Disable snapshots by default, unless enabled by ZFS properties')
    snapshot_parser.add_argument(
        '--zfs-cmd', metavar='PATH',
        help='Override path to zfs executable')
    snapshot_parser.add_argument(
        '--keep', metavar='INT', type=int, required=True,
        help='Number of snapshots to keep')
    snapshot_parser.add_argument(
        '--label', required=True, help='Snapshot label')
    snapshot_parser.add_argument(
        '--ssh-cmd', metavar='PATH', help='Override path to ssh executable')

    # Common arguments
    parser.add_argument(
        '--quiet', action='store_true', help='Suppress output from script')
    parser.add_argument(
        '--verbosity',
        choices=[
            'CRITICAL',
            'ERROR',
            'WARNING',
            'INFO',
            'DEBUG'
        ],
        default='INFO', help='Set log level for console output. Default: INFO')
    parser.add_argument(
        '--lockfile', metavar='PATH', help='Override path to lockfile')
    args = parser.parse_args()

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    if not args.quiet:
        fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
        ch = logging.StreamHandler()
        ch.setLevel(args.verbosity)
        ch.setFormatter(fmt)
        logger.addHandler(ch)

    try:
        with ZFSSnap(lockfile=args.lockfile) as z:
            if args.subparser == 'snapshot':
                z.snapshot(
                    keep=args.keep,
                    label=args.label,
                    fs_locations=args.file_systems,
                    zfs_cmd=args.zfs_cmd,
                    ssh_cmd=args.ssh_cmd,
                    default_exclude=args.default_exclude,
                    override=args.override)
            elif args.subparser == 'replicate':
                z.replicate(
                    keep=args.keep,
                    label=args.label,
                    src_fs_location=args.src_file_system,
                    dst_fs_location=args.dst_file_system,
                    src_zfs_cmd=args.src_zfs_cmd,
                    dst_zfs_cmd=args.dst_zfs_cmd,
                    ssh_cmd=args.ssh_cmd)
    except KeyboardInterrupt:
        sys.exit(130)
    except ZFSReplicationException:
        sys.exit(11)
    except ZFSSnapException:
        sys.exit(10)

if __name__ == '__main__':
    main()
