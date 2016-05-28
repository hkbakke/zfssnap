import pytest
import datetime
from zfssnap import ZFSHost, ZFSFileSystem, ZFSSnapshot

class TestZFSSnapshot(object):
    @pytest.fixture
    def snapshot(self):
        host = ZFSHost()
        fs = ZFSFileSystem(host, 'zpool/dataset')
        return ZFSSnapshot(fs, 'zfssnap_20160522T201201Z')

    def test_datetime(self, snapshot):
        assert snapshot.datetime == datetime.datetime(
            2016, 5, 22, 20, 12, 1, tzinfo=datetime.timezone.utc)
