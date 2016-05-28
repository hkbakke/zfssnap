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

    def test_snapshots_enabled_no_properties(self, fs, monkeypatch):
        label = 'test'

        def mock_get_properties():
            properties = {}
            return properties

        monkeypatch.setattr(fs, 'get_properties', mock_get_properties)
        assert fs.snapshots_enabled(label) is None

    def test_snapshots_enabled_zfssnap_off(self, fs, monkeypatch):
        label = 'test'

        def mock_get_properties():
            properties = {
                'zol:zfssnap': 'off'
            }
            return properties

        monkeypatch.setattr(fs, 'get_properties', mock_get_properties)
        assert fs.snapshots_enabled(label) is False

    def test_snapshots_enabled_zfssnap_on(self, fs, monkeypatch):
        label = 'test'

        def mock_get_properties():
            properties = {
                'zol:zfssnap': 'on'
            }
            return properties

        monkeypatch.setattr(fs, 'get_properties', mock_get_properties)
        assert fs.snapshots_enabled(label) is True

    def test_snapshots_enabled_label_off(self, fs, monkeypatch):
        label = 'test'

        def mock_get_properties():
            properties = {
                'zol:zfssnap:%s' % label: 'off'
            }
            return properties

        monkeypatch.setattr(fs, 'get_properties', mock_get_properties)
        assert fs.snapshots_enabled(label) is False

    def test_snapshots_enabled_label_on(self, fs, monkeypatch):
        label = 'test'

        def mock_get_properties():
            properties = {
                'zol:zfssnap:%s' % label: 'on'
            }
            return properties

        monkeypatch.setattr(fs, 'get_properties', mock_get_properties)
        assert fs.snapshots_enabled(label) is True

    def test_snapshots_enabled_label_on_zfssnap_off(self, fs, monkeypatch):
        label = 'test'

        def mock_get_properties():
            properties = {
                'zol:zfssnap': 'off',
                'zol:zfssnap:%s' % label: 'on'
            }
            return properties

        monkeypatch.setattr(fs, 'get_properties', mock_get_properties)
        assert fs.snapshots_enabled(label) is True

    def test_snapshots_enabled_label_off_zfssnap_on(self, fs, monkeypatch):
        label = 'test'

        def mock_get_properties():
            properties = {
                'zol:zfssnap': 'on',
                'zol:zfssnap:%s' % label: 'off'
            }
            return properties

        monkeypatch.setattr(fs, 'get_properties', mock_get_properties)
        assert fs.snapshots_enabled(label) is False

    def test_get_keep(self, fs, monkeypatch):
        label = 'test'

        def mock_get_properties():
            properties = {}
            return properties

        monkeypatch.setattr(fs, 'get_properties', mock_get_properties)
        assert fs.get_keep(label) is None

    def test_get_keep_label_override(self, fs, monkeypatch):
        label = 'test'

        def mock_get_properties():
            properties = {
                'zol:zfssnap:%s:keep' % label: 4
            }
            return properties

        monkeypatch.setattr(fs, 'get_properties', mock_get_properties)
        assert fs.get_keep(label) == 4

    def test_get_keep_fs_override(self, fs, monkeypatch):
        label = 'test'

        def mock_get_properties():
            properties = {
                'zol:zfssnap:keep': 3
            }
            return properties

        monkeypatch.setattr(fs, 'get_properties', mock_get_properties)
        assert fs.get_keep(label) == 3

    def test_get_keep_label_and_fs_override(self, fs, monkeypatch):
        label = 'test'

        def mock_get_properties():
            properties = {
                'zol:zfssnap:keep': 3,
                'zol:zfssnap:%s:keep' % label: 4
            }
            return properties

        monkeypatch.setattr(fs, 'get_properties', mock_get_properties)
        assert fs.get_keep(label) == 4
