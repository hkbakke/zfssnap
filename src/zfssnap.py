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
import yaml
import fnmatch


PROPERTY_PREFIX = 'zfssnap'
ZFSSNAP_LABEL = '%s:label' % PROPERTY_PREFIX
ZFSSNAP_REPL_STATUS = '%s:repl_status' % PROPERTY_PREFIX
VERSION = '3.0.0'


class ZFSHostException(Exception):
    pass


class ZFSReplicationException(Exception):
    pass


class ZFSSnapshotException(Exception):
    pass


class ZFSSnapException(Exception):
    pass


class ZFSSnapConfigException(Exception):
    pass


class ZFSSnapConfig(object):
    def __init__(self, config_file):
        if config_file is None:
            config_file = '/etc/zfssnap/zfssnap.yml'

        with open(config_file) as f:
            self._config = yaml.load(f)

    def get_policy(self, policy):
        try:
            return self._config['policies'][policy]
        except KeyError:
            raise ZFSSnapConfigException(
                'The policy \'%s\' is not defined' % policy)

    def get_cmd(self, cmd):
        return self._config['cmds'][cmd]

    def get_cmds(self):
        return self._config.get('cmds', {})


class ZFSSnapshot(object):
    def __init__(self, dataset, name):
        self.name = name
        self.dataset = dataset
        self.full_name = '%s@%s' % (dataset.name, name)
        self.logger = logging.getLogger(__name__)
        self._properties = dict()
        self._label = None
        self._repl_status = None

    def create(self, label, recursive=False, properties=None):
        self.logger.info('Creating snapshot %s', self.full_name)

        if properties is None:
            properties = {}

        if label == '-':
            raise ZFSSnapshotException('\'%s\' is not a valid label' % label)

        args = [
            'snapshot',
            '-o', '%s=%s' % (ZFSSNAP_LABEL, label),
        ]

        for key, value in properties.items():
            args.extend(['-o', '%s=%s' % (key, value)])

        if recursive:
            args.append('-r')

        args.append(self.full_name)
        cmd = self.dataset.host.get_cmd('zfs', args)
        subprocess.check_call(cmd)

    def destroy(self, recursive=False):
        self.logger.info('Destroying snapshot %s', self.full_name)
        args = ['destroy']

        if recursive:
            args.append('-r')

        args.append(self.full_name)
        cmd = self.dataset.host.get_cmd('zfs', args)
        subprocess.check_call(cmd)

    @property
    def label(self):
        if self._label:
            return self._label
        else:
            return self.get_property(ZFSSNAP_LABEL)

    @label.setter
    def label(self, value):
        self._label = value

    @property
    def repl_status(self):
        if self._repl_status:
            return self._repl_status
        else:
            return self.get_property(ZFSSNAP_LABEL)

    @repl_status.setter
    def repl_status(self, value):
        self._repl_status = value

    @property
    def datetime(self):
        strptime_name = re.sub(r'Z$', '+0000', self.name)
        return datetime.strptime(strptime_name, 'zfssnap_%Y%m%dT%H%M%S%z')

    @staticmethod
    def _autoconvert(value):
        for fn in [int]:
            try:
                return fn(value)
            except ValueError:
                pass

        return value

    def get_properties(self, refresh=False):
        if refresh or not self._properties:
            args = [
                'get', 'all',
                '-H',
                '-p',
                '-o', 'property,value',
                self.full_name
            ]
            cmd = self.dataset.host.get_cmd('zfs', args)
            output = subprocess.check_output(cmd)

            for line in output.decode('utf8').split('\n'):
                if not line.strip():
                    continue

                zfs_property, value = line.split('\t')
                self._properties[zfs_property] = self._autoconvert(value)

        return self._properties

    def get_property(self, zfs_property, refresh=False):
        properties = self.get_properties(refresh)
        return properties[zfs_property]

    def set_property(self, name, value):
        args = [
            'set',
            '%s=%s' % (name, value),
            self.full_name
        ]
        cmd = self.dataset.host.get_cmd('zfs', args)
        subprocess.check_call(cmd)


class ZFSDataset(object):
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

    @property
    def location(self):
        if self.host.ssh_user and self.host.ssh_host:
            return '%s@%s:%s' % (self.host.ssh_user, self.host.ssh_host,
                                 self.name)
        else:
            return self.name

    def get_properties(self, refresh=False):
        if refresh or not self._properties:
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

                zfs_property, value = line.split('\t')
                self._properties[zfs_property] = self._autoconvert(value)

        return self._properties

    def get_latest_snapshot(self, label=None):
        snapshots = sorted(self.get_snapshots(label),
                           key=attrgetter('datetime'),
                           reverse=True)

        return next(iter(snapshots), None)

    def get_snapshots(self, label=None):
        args = [
            'list',
            '-H',
            '-o', 'name,%s,%s' % (ZFSSNAP_LABEL, ZFSSNAP_REPL_STATUS),
            '-d', '1',
            '-t', 'snapshot',
            self.name
        ]
        cmd = self.host.get_cmd('zfs', args)

        try:
            output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            msg = e.output.decode('utf8').strip()

            if 'dataset does not exist' in msg:
                output = None
            else:
                self.logger.error(msg)
                raise

        if output:
            for line in output.decode('utf8').split('\n'):
                if not line.strip():
                    continue

                snapshot_name, snapshot_label, repl_status = line.split('\t')

                if not label or snapshot_label == label:
                    _, name = snapshot_name.split('@')
                    snapshot = ZFSSnapshot(self, name)
                    snapshot.label = label
                    snapshot.repl_status = repl_status
                    yield snapshot

    def replicate(self, dst_dataset, label):
        self.logger.info('Cleaning up previously failed replications...')
        self.destroy_failed_snapshots(label)

        self.logger.info('Replicating %s to %s', self.location,
                         dst_dataset.location)
        previous_snapshot = self.get_latest_snapshot(label)
        properties = {
            ZFSSNAP_REPL_STATUS: 'failed'
        }
        snapshot = self.create_snapshot(label=label, properties=properties)

        if previous_snapshot:
            send_args = [
                'send',
                '-R',
                '-I', '@%s' % previous_snapshot.name,
                snapshot.full_name
            ]
        else:
            send_args = [
                'send',
                '-R',
                snapshot.full_name
            ]

        receive_args = [
            'receive',
            '-F',
            '-v',
            dst_dataset.name
        ]

        send_cmd = self.host.get_cmd('zfs', send_args)
        receive_cmd = dst_dataset.host.get_cmd('zfs', receive_args)
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

        if receive.returncode == 0:
            snapshot.set_property(ZFSSNAP_REPL_STATUS, 'success')
        else:
            raise ZFSReplicationException('Replication failed!')

    def create_snapshot(self, label, recursive=False, ts=None, properties=None):
        if ts is None:
            ts = datetime.utcnow()

        timestamp = ts.strftime('%Y%m%dT%H%M%SZ')
        name = 'zfssnap_%s' % timestamp
        snapshot = ZFSSnapshot(self, name)
        snapshot.create(label=label, recursive=recursive, properties=properties)
        return snapshot

    def destroy_failed_snapshots(self, label=None):
        for snapshot in self.get_snapshots(label):
            if snapshot.repl_status != 'success':
                snapshot.destroy()

    def destroy_old_snapshots(self, keep, label=None, limit=None, recursive=False):
        snapshots = sorted(self.get_snapshots(label),
                           key=attrgetter('datetime'),
                           reverse=True)[keep:]

        for snapshot in sorted(snapshots, key=attrgetter('datetime'),
                               reverse=False):
            if limit and len(destroyed_snapshots) >= limit:
                return

            snapshot.destroy(recursive)

class ZFSHost(object):
    def __init__(self, ssh_user=None, ssh_host=None, cmds=None):
        if cmds is None:
            cmds = {}

        self.logger = logging.getLogger(__name__)
        self.cmds = self._validate_cmds(cmds)
        self.ssh_user = ssh_user
        self.ssh_host = ssh_host

    def _validate_cmds(self, cmds):
        valid_cmds = {
            'zfs': 'zfs',
            'ssh': 'ssh'
        }

        valid_cmds.update({k: v for k, v in cmds.items() if v is not None})
        return valid_cmds

    def get_cmd(self, name, args=None):
        cmd_path = self.cmds.get(name, None)

        if cmd_path is None:
            raise ZFSHostException(
                '\'%s\' does not have a path defined.' % name)

        if args is None:
            args = []

        ssh_cmd = self.cmds.get('ssh', None)

        if ssh_cmd and self.ssh_user and self.ssh_host:
            cmd = [ssh_cmd, '%s@%s' % (self.ssh_user, self.ssh_host), cmd_path]
        else:
            cmd = [cmd_path]

        cmd.extend(args)
        self.logger.debug('Command: %s', ' '.join(cmd))
        return cmd

    def get_filesystems(self, include_filters=None, exclude_filters=None):
        args = [
            'list',
            '-H',
            '-p',
            '-o', 'name',
            '-t', 'filesystem'
        ]
        cmd = self.get_cmd('zfs', args)
        output = subprocess.check_output(cmd)

        if include_filters is None:
            include_filters = []

        if exclude_filters is None:
            exclude_filters = []

        for name in output.decode('utf8').split('\n'):
            exclude = False

            if not name.strip():
                continue

            for pattern in exclude_filters:
                if fnmatch.fnmatch(name, pattern):
                    self.logger.info('\'%s\' is excluded by pattern \'%s\'',
                                     name, pattern)
                    exclude = True
                    break

            if exclude:
                continue

            if include_filters:
                for pattern in include_filters:
                    if fnmatch.fnmatch(name, pattern):
                        yield ZFSDataset(host=self, name=name)
                        break
            else:
                yield ZFSDataset(host=self, name=name)

    def get_filesystem(self, fs_name):
        return next(self.get_filesystems([fs_name]), None)


class ZFSSnap(object):
    def __init__(self, config=None, lockfile=None):
        self.logger = logging.getLogger(__name__)

        # The lock file object needs to be at class level for not to be
        # garbage collected after the _aquire_lock function has finished.
        self._lock_f = None
        self._aquire_lock(lockfile)

        self.config = ZFSSnapConfig(config)

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
        wait = 3
        timeout = 60

        while timeout > 0:
            try:
                fcntl.lockf(self._lock_f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                self.logger.debug('Lock aquired.')
                return
            except OSError:
                self.logger.info('zfssnap is already running. Waiting for '
                                 'lock release... (timeout: %ss)', timeout)
                timeout = timeout - wait
                time.sleep(wait)

        raise ZFSSnapException('Timeout reached. Could not get lock.')

    @staticmethod
    def _parse_destination_name(name):
        ssh_user = None
        ssh_host = None
        fs_name = None

        if '@' in name:
            ssh_user, tail = name.split('@', 1)
            ssh_host, fs_name = tail.split(':', 1)
        else:
            fs_name = name

        return (fs_name, ssh_user, ssh_host)

    def execute_policy(self, policy, reset=False):
        policy_config = self.config.get_policy(policy)
        local_host = ZFSHost(cmds=self.config.get_cmds())

        if policy_config['type'] == 'snapshot':
            self.snapshot(
                keep=policy_config['keep'],
                label=policy,
                reset=reset,
                recursive=policy_config.get('recursive', False),
                datasets=local_host.get_filesystems(policy_config.get('include', None),
                                                    policy_config.get('exclude', None)))
        elif policy_config['type'] == 'replication':
            dst_dataset_name, ssh_user, ssh_host = self._parse_destination_name(policy_config['destination'])
            dst_host = ZFSHost(ssh_user=ssh_user, ssh_host=ssh_host,
                               cmds=policy_config.get('destination_cmds', None))
            dst_dataset = dst_host.get_filesystem(dst_dataset_name)

            if not dst_dataset:
                raise ZFSReplicationException('The dataset %s does not exist' %
                                              dst_dataset_name)

            self.replicate(
                keep=policy_config['keep'],
                label=policy,
                reset=reset,
                src_dataset=local_host.get_filesystem(policy_config['source']),
                dst_dataset=dst_dataset)

    def replicate(self, keep, label, src_dataset, dst_dataset, reset=False):
        if keep < 1:
            raise ZFSReplicationException(
                'Replication needs a keep value of at least 1.')

        if reset:
            self.logger.warning('Reset is enabled. Reinitializing replication '
                                'for this policy')
            keep = 0
            dst_dataset.destroy_old_snapshots(keep=keep, label=None)
        else:
            src_dataset.replicate(dst_dataset, label)

        src_dataset.destroy_old_snapshots(keep=keep, label=label)

    def snapshot(self, keep, label, datasets=None, recursive=False, reset=False):
        if datasets is None:
            datasets = []

        if reset:
            self.logger.warning('Reset is enabled. Removing all snapshots '
                                'for this policy')
            keep = 0

        for dataset in datasets:
            if keep > 0:
                dataset.create_snapshot(label=label, recursive=recursive)

            dataset.destroy_old_snapshots(keep=keep, label=label,
                                          recursive=recursive)


def main():
    parser = argparse.ArgumentParser(
        description='Automatic snapshotting and replication for ZFS on Linux')

    mutex_group = parser.add_mutually_exclusive_group(required=True)
    mutex_group.add_argument(
        '--version', action='store_true', help='Print version and exit')
    mutex_group.add_argument(
        '--policy', help='Select policy')
    parser.add_argument(
        '--quiet', action='store_true', help='Suppress output from script')
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
    parser.add_argument(
        '--config', metavar='PATH', help='Path to configuration file')
    parser.add_argument(
        '--reset', action='store_true',
        help='Remove all policy snapshots or reinitialize replication')
    parser.add_argument(
        '--lockfile', metavar='PATH', help='Override path to lockfile')
    args = parser.parse_args()

    if args.version:
        print('zfssnap v%s' % VERSION)
        sys.exit(0)

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
            z.execute_policy(args.policy, args.reset)
    except ZFSSnapException:
        sys.exit(10)
    except ZFSReplicationException:
        sys.exit(11)
    except ZFSHostException:
        sys.exit(12)
    except ZFSSnapshotException:
        sys.exit(13)
    except ZFSSnapConfigException:
        sys.exit(14)
    except KeyboardInterrupt:
        sys.exit(130)

if __name__ == '__main__':
    main()
