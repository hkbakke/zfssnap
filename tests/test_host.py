import pytest
from zfssnap import Host, HostException

class TestHost(object):
    @pytest.fixture
    def host(self):
        return Host()

    @pytest.fixture
    def host_with_cmds(self):
        cmds = {
            'newbin': '/some/path/newbin',
            'zfs': '/some/path/zfs'
        }
        return Host(cmds=cmds)

    @pytest.fixture
    def ssh_host(self):
        ssh_user = 'root'
        ssh_host = 'host'
        return Host(ssh_user=ssh_user, ssh_host=ssh_host)

    @pytest.fixture
    def ssh_host_with_cmds(self):
        ssh_user = 'root'
        ssh_host = 'host'
        cmds = {
            'newbin': '/some/path/newbin',
            'ssh': '/some/path/ssh'
        }
        return Host(ssh_user=ssh_user, ssh_host=ssh_host, cmds=cmds)

    def test_get_cmd_defaults(self, host):
        assert host.get_cmd('zfs') == ['zfs']
        assert host.get_cmd('ssh') == ['ssh']

    def test_get_ssh_cmd(self, ssh_host):
        assert ssh_host.get_cmd('zfs') == [
            'ssh', 'root@host', 'zfs'
        ]

    def test_get_cmd_with_args(self, host_with_cmds):
        args = ['-a', 'fileA', '-v']
        assert host_with_cmds.get_cmd('newbin', args) == [
            '/some/path/newbin', '-a', 'fileA', '-v'
        ]

    def test_get_ssh_cmd_with_args_override_ssh(self, ssh_host_with_cmds):
        args = ['-a', 'fileA', '-v']
        assert ssh_host_with_cmds.get_cmd('zfs', args) == [
            '/some/path/ssh', 'root@host', 'zfs', '-a', 'fileA', '-v'
        ]

    def test_get_invalid_cmd(self, host):
        with pytest.raises(HostException):
            host.get_cmd('_invalid')
