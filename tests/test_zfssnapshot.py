import pytest
import datetime
from zfssnap import ZFSHost, ZFSSnapshot

class TestZFSSnapshot(object):
    @pytest.fixture
    def snapshot(self):
        name = 'zpool/dataset@zfssnap_20160522T201201Z'
        host = ZFSHost()
        return ZFSSnapshot(host, name)

    def test_snapname(self, snapshot):
        assert snapshot.snapname == 'zfssnap_20160522T201201Z'

    def test_datetime(self, snapshot):
        assert snapshot.datetime == datetime.datetime(
            2016, 5, 22, 20, 12, 1, tzinfo=datetime.timezone.utc)
