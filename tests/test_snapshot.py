import pytest
import datetime
import subprocess

from zfssnap import Host, Snapshot, ZFSSNAP_REPL_STATUS, ZFSSNAP_LABEL, ZFSSNAP_VERSION

class TestSnapshot(object):
    @pytest.fixture
    def snapshot(self):
        cmds = {
            'zfs': 'zfs'
        }
        host = Host(cmds)
        name = 'dev-1/test-1@zfssnap_20170119T094102Z'
        return Snapshot(host, name)

    def test_timestamp_property(self, snapshot):
        assert snapshot.timestamp == '20170119T094102Z'

    def test_snapshot_datetime(self, snapshot):
        assert snapshot.datetime == datetime.datetime(
            2017, 1, 19, 9, 41, 2, tzinfo=datetime.timezone.utc)
