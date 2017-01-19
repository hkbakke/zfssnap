import pytest
import datetime
import subprocess

from zfssnap import Host, Snapshot, ZFSSNAP_REPL_STATUS, ZFSSNAP_LABEL, ZFSSNAP_VERSION

class TestSnapshot(object):
    @pytest.fixture
    def snapshot(self):
        host = Host()
        name = 'dev-1/test-1@zfssnap_20170119T094102Z'
        return Snapshot(host, name)

    def test_location_property_local_host(self):
        host = Host()
        name = 'dev-1/test-1@zfssnap_20170119T094102Z'
        snapshot = Snapshot(host, name)
        assert snapshot.location == 'dev-1/test-1@zfssnap_20170119T094102Z'

    def test_location_property_remote_host(self):
        host = Host(name='remote')
        name = 'dev-1/test-1@zfssnap_20170119T094102Z'
        snapshot = Snapshot(host, name)
        assert snapshot.location == 'remote: dev-1/test-1@zfssnap_20170119T094102Z'

    def test_timestamp_property(self, snapshot):
        assert snapshot.timestamp == '20170119T094102Z'

    def test_snapshot_datetime(self, snapshot):
        assert snapshot.datetime == datetime.datetime(
            2017, 1, 19, 9, 41, 2, tzinfo=datetime.timezone.utc)

    def test_set_property(self, monkeypatch, snapshot):
        name = 'name'
        value = 'value'
        def mock_cmd(cmd):
            assert cmd == ['zfs', 'set', '%s=%s' % (name, value),
                           'dev-1/test-1@zfssnap_20170119T094102Z']
        monkeypatch.setattr(subprocess, 'check_call', mock_cmd)
        snapshot.set_property(name, value)
        assert snapshot._properties[name] == value

    def test_refresh_properties(self, monkeypatch, snapshot):
        def mock_cmd(cmd):
            assert cmd == ['zfs', 'get', 'all', '-H', '-p', '-o',
                           'property,value',
                           'dev-1/test-1@zfssnap_20170119T094102Z']
            output = u"""type    snapshot
creation    1484818862
compressratio   1.00x
version 5
utf8only    off
normalization   none
clones  
zfssnap:version 3.5.3
zfssnap:label   replicate-disconnected
zfssnap:repl_status test
"""
            return output
        monkeypatch.setattr(subprocess, 'check_output', mock_cmd)
        snapshot._refresh_properties()
        properties = {
            'type': 'snapshot',
            'creation': 1484818862,
            'compressratio': '1.00x',
            'version': 5,
            'utf8only': 'off',
            'normalization': 'none',
            'clones': None,
            'zfssnap:version': '3.5.3',
            'zfssnap:label': 'replicate-disconnected',
            'zfssnap:repl_status': 'test'
        }
        assert snapshot._properties == properties
