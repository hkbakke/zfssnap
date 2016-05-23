import pytest
from zfssnap import ZFSHost, ZFSFileSystem

class TestZFSFileSystem(object):
    @pytest.fixture
    def fs(self):
        fs_name = 'zpool/dataset'
        host = ZFSHost()
        return ZFSFileSystem(host, fs_name)

    @pytest.fixture
    def ssh_fs(self):
        ssh_user = 'root'
        ssh_host = 'host'
        fs_name = 'zpool/dataset'
        host = ZFSHost(ssh_user=ssh_user, ssh_host=ssh_host)
        return ZFSFileSystem(host, fs_name)

    def test_autoconvert_to_int(self):
        assert isinstance(ZFSFileSystem._autoconvert('123'), int)

    def test_autoconvert_to_str(self):
        assert isinstance(ZFSFileSystem._autoconvert('12f'), str)

    def test_return_local_location(self, fs):
        assert fs.location == 'zpool/dataset'

    def test_return_ssh_location(self, ssh_fs):
        assert ssh_fs.location == 'root@host:zpool/dataset'

    def test_snapshots_enabled_no_properties(self, fs):
        label = 'test'
        properties = {}
        assert fs.snapshots_enabled(label, properties) is None

    def test_snapshots_enabled_zfssnap_off(self, fs):
        label = 'test'
        properties = {
            'zol:zfssnap': 'off'
        }
        assert fs.snapshots_enabled(label, properties) is False

    def test_snapshots_enabled_zfssnap_on(self, fs):
        label = 'test'
        properties = {
            'zol:zfssnap': 'on'
        }
        assert fs.snapshots_enabled(label, properties) is True

    def test_snapshots_enabled_label_off(self, fs):
        label = 'test'
        properties = {
            'zol:zfssnap:%s' % label: 'off'
        }
        assert fs.snapshots_enabled(label, properties) is False

    def test_snapshots_enabled_label_on(self, fs):
        label = 'test'
        properties = {
            'zol:zfssnap:%s' % label: 'on'
        }
        assert fs.snapshots_enabled(label, properties) is True

    def test_snapshots_enabled_label_on_zfssnap_off(self, fs):
        label = 'test'
        properties = {
            'zol:zfssnap': 'off',
            'zol:zfssnap:%s' % label: 'on'
        }
        assert fs.snapshots_enabled(label, properties) is True

    def test_snapshots_enabled_label_off_zfssnap_on(self, fs):
        label = 'test'
        properties = {
            'zol:zfssnap': 'on',
            'zol:zfssnap:%s' % label: 'off'
        }
        assert fs.snapshots_enabled(label, properties) is False

    def test_get_keep(self, fs):
        label = 'test'
        properties = {}
        assert fs.get_keep(label, properties) is None

    def test_get_keep_label_override(self, fs):
        label = 'test'
        properties = {
            'zol:zfssnap:%s:keep' % label: 4
        }
        assert fs.get_keep(label, properties) == 4

    def test_get_keep_fs_override(self, fs):
        label = 'test'
        properties = {
            'zol:zfssnap:keep': 3
        }
        assert fs.get_keep(label, properties) == 3

    def test_get_keep_label_and_fs_override(self, fs):
        label = 'test'
        properties = {
            'zol:zfssnap:keep': 3,
            'zol:zfssnap:%s:keep' % label: 4
        }
        assert fs.get_keep(label, properties) == 4
