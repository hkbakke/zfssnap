import pytest
from zfssnap import autotype, Host, Dataset
import subprocess


PROPERTY_PREFIX = 'zfssnap'


class TestDataset(object):
    @pytest.fixture
    def fs(self):
        fs_name = 'zpool/dataset'
        host = Host()
        return Dataset(host, fs_name)

    @pytest.fixture
    def ssh_fs(self):
        ssh_user = 'root'
        ssh_host = 'host'
        fs_name = 'zpool/dataset'
        host = Host(ssh_user=ssh_user, ssh_host=ssh_host)
        return Dataset(host, fs_name)

    def test_autotype_to_int(self):
        assert isinstance(autotype('123'), int)

    def test_autotype_to_str(self):
        assert isinstance(autotype('12f'), str)

    def test_return_local_location(self, fs):
        assert fs.location == 'zpool/dataset'

    def test_return_ssh_location(self, ssh_fs):
        assert ssh_fs.location == 'root@host:zpool/dataset'