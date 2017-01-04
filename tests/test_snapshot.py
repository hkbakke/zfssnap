import pytest
import datetime
from zfssnap import Host, Dataset, Snapshot

class TestSnapshot(object):
    @pytest.fixture
    def snapshot(self):
        host = Host()
        fs = Dataset(host, 'zpool/dataset')
        return Snapshot(fs, 'zfssnap_20160522T201201Z')

    def test_datetime(self, snapshot):
        assert snapshot.datetime == datetime.datetime(
            2016, 5, 22, 20, 12, 1, tzinfo=datetime.timezone.utc)
