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
import fnmatch

import yaml


PROPERTY_PREFIX = 'zfssnap'
ZFSSNAP_LABEL = '%s:label' % PROPERTY_PREFIX
ZFSSNAP_REPL_STATUS = '%s:repl_status' % PROPERTY_PREFIX
VERSION = '3.0.0'


def autotype(value):
    for fn in [int]:
        try:
            return fn(value)
        except ValueError:
            pass

    return value


class HostException(Exception):
    pass


class ReplicationException(Exception):
    pass


class SnapshotException(Exception):
    pass


class ZFSSnapException(Exception):
    pass


class ConfigException(Exception):
    pass


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
            raise ConfigException(
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

        if properties is None:
            self._properties = dict()
        else:
            self._properties = properties

    def create(self, label, recursive=False):
        self.logger.info('Creating snapshot %s', self.name)

        if label == '-':
            raise SnapshotException('\'%s\' is not a valid label' % label)

        args = [
            'snapshot',
            '-o', '%s=%s' % (ZFSSNAP_LABEL, label),
        ]

        if recursive:
            args.append('-r')

        args.append(self.name)
        cmd = self.host.get_cmd('zfs', args)
        subprocess.check_call(cmd)

    def destroy(self, recursive=False):
        self.logger.info('Destroying snapshot %s', self.name)
        args = ['destroy']

        if recursive:
            args.append('-r')

        args.append(self.name)
        cmd = self.host.get_cmd('zfs', args)
        subprocess.check_call(cmd)

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
                self._properties[zfs_property] = autotype(value)

        return self._properties

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
    def label(self):
        return self.get_property(ZFSSNAP_LABEL)

    def get_property(self, zfs_property, refresh=False):
        return self.get_properties(refresh).get(zfs_property, None)

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
        if self.host.ssh_user and self.host.ssh_host:
            return '%s@%s:%s' % (self.host.ssh_user, self.host.ssh_host,
                                 self.name)
        else:
            return self.name

    def get_latest_replication_snapshot(self, label=None):
        snapshots = sorted(self.get_snapshots(label),
                           key=attrgetter('datetime'),
                           reverse=True)

        for snapshot in snapshots:
            if snapshot.repl_status == 'success':
                return snapshot

    def get_snapshots(self, label=None):
        snapshots = {}

        args = [
            'get', 'all',
            '-H',
            '-p',
            '-o', 'name,property,value',
            '-d', '1',
            '-t', 'snapshot',
            self.name
        ]
        cmd = self.host.get_cmd('zfs', args)
        output = subprocess.check_output(cmd)

        for line in output.decode('utf8').split('\n'):
            if not line.strip():
                continue

            name, zfs_property, value = line.split('\t')

            if name not in snapshots:
                snapshots[name] = {}

            snapshots[name][zfs_property] = autotype(value)

        for name, properties in snapshots.items():
            if label and properties.get(ZFSSNAP_LABEL, None) != label:
                continue

            yield Snapshot(self.host, name, properties=properties)

    def replicate(self, dst_dataset, label):
        self.logger.info('Cleaning up previously failed replications...')
        self.destroy_failed_replication_snapshots(label)

        self.logger.info('Replicating %s to %s', self.location,
                         dst_dataset.location)
        previous_snapshot = self.get_latest_replication_snapshot(label)
        snapshot = self.create_snapshot(label=label)

        if previous_snapshot:
            send_args = [
                'send',
                '-R',
                '-I', '@%s' % previous_snapshot.snapshot_name,
                snapshot.name
            ]
        else:
            send_args = [
                'send',
                '-R',
                snapshot.name
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
            snapshot.repl_status = 'success'
        else:
            raise ReplicationException('Replication failed!')

    def create_snapshot(self, label, recursive=False, ts=None):
        if ts is None:
            ts = datetime.utcnow()

        timestamp = ts.strftime('%Y%m%dT%H%M%SZ')
        snapshot_name = '%s@zfssnap_%s' % (self.name, timestamp)
        snapshot = Snapshot(self.host, snapshot_name)
        snapshot.create(label=label, recursive=recursive)
        return snapshot

    def destroy_failed_replication_snapshots(self, label):
        for snapshot in self.get_snapshots(label):
            if snapshot.repl_status != 'success':
                snapshot.destroy()

    def destroy_old_snapshots(self, keep, label=None, recursive=False):
        snapshots = sorted(self.get_snapshots(label),
                           key=attrgetter('datetime'),
                           reverse=True)[keep:]

        for snapshot in sorted(snapshots, key=attrgetter('datetime'),
                               reverse=False):
            snapshot.destroy(recursive)

class Host(object):
    def __init__(self, ssh_user=None, ssh_host=None, cmds=None):
        if cmds is None:
            cmds = {}

        self.logger = logging.getLogger(__name__)
        self.cmds = self._validate_cmds(cmds)
        self.ssh_user = ssh_user
        self.ssh_host = ssh_host

    @staticmethod
    def _validate_cmds(cmds):
        valid_cmds = {
            'zfs': 'zfs',
            'ssh': 'ssh'
        }

        valid_cmds.update({k: v for k, v in cmds.items() if v is not None})
        return valid_cmds

    def get_cmd(self, name, args=None):
        cmd_path = self.cmds.get(name, None)

        if cmd_path is None:
            raise HostException(
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
                        yield Dataset(host=self, name=name)
                        break
            else:
                yield Dataset(host=self, name=name)

    def get_filesystem(self, fs_name):
        return next(self.get_filesystems([fs_name]), None)


class ZFSSnap(object):
    def __init__(self, config=None, lockfile=None):
        self.logger = logging.getLogger(__name__)

        # The lock file object needs to be at class level for not to be
        # garbage collected after the _aquire_lock function has finished.
        self._lock_f = None
        self._aquire_lock(lockfile)

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
        local_host = Host(cmds=self.config.get_cmds())

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
            dst_host = Host(ssh_user=ssh_user, ssh_host=ssh_host,
                            cmds=policy_config.get('destination_cmds', None))
            dst_dataset = dst_host.get_filesystem(dst_dataset_name)

            if not dst_dataset:
                raise ReplicationException('The dataset %s does not exist' %
                                              dst_dataset_name)

            self.replicate(
                keep=policy_config['keep'],
                label=policy,
                reset=reset,
                src_dataset=local_host.get_filesystem(policy_config['source']),
                dst_dataset=dst_dataset)

    def replicate(self, keep, label, src_dataset, dst_dataset, reset=False):
        if keep < 1:
            raise ReplicationException(
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
    except ReplicationException:
        sys.exit(11)
    except HostException:
        sys.exit(12)
    except SnapshotException:
        sys.exit(13)
    except ConfigException:
        sys.exit(14)
    except KeyboardInterrupt:
        sys.exit(130)

if __name__ == '__main__':
    main()
