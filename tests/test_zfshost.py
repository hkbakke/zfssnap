import pytest
from zfssnap import ZFSHost, ZFSSnapException

class TestZFSHost(object):
    @pytest.fixture
    def host(self):
        return ZFSHost()

    @pytest.fixture
    def host_with_cmds(self):
        cmds = {
            'newbin': '/some/path/newbin',
            'zfs': '/some/path/zfs'
        }
        return ZFSHost(cmds=cmds)

    @pytest.fixture
    def ssh_host(self):
        ssh_user = 'root'
        ssh_host = 'host'
        return ZFSHost(ssh_user=ssh_user, ssh_host=ssh_host)

    @pytest.fixture
    def ssh_host_with_cmds(self):
        ssh_user = 'root'
        ssh_host = 'host'
        cmds = {
            'newbin': '/some/path/newbin',
            'ssh': '/some/path/ssh'
        }
        return ZFSHost(ssh_user=ssh_user, ssh_host=ssh_host, cmds=cmds)

    def test_get_cmd_defaults(self, host):
        assert host.get_cmd('zfs') == ['/sbin/zfs']
        assert host.get_cmd('ssh') == ['/usr/bin/ssh']

    def test_get_ssh_cmd(self, ssh_host):
        assert ssh_host.get_cmd('zfs') == [
            '/usr/bin/ssh', 'root@host', '/sbin/zfs'
        ]

    def test_get_cmd_with_args(self, host_with_cmds):
        args = ['-a', 'fileA', '-v']
        assert host_with_cmds.get_cmd('newbin', args) == [
            '/some/path/newbin', '-a', 'fileA', '-v'
        ]

    def test_get_ssh_cmd_with_args_override_ssh(self, ssh_host_with_cmds):
        args = ['-a', 'fileA', '-v']
        assert ssh_host_with_cmds.get_cmd('zfs', args) == [
            '/some/path/ssh', 'root@host', '/sbin/zfs', '-a', 'fileA', '-v'
        ]

    def test_get_invalid_cmd(self, host):
        with pytest.raises(ZFSSnapException):
            host.get_cmd('_invalid')
