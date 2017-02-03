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
            output = b'type\tsnapshot\ncreation\t1484840164\nused\t0\nreferenced\t19456\ncompressratio\t1.05x\ndevices\ton\nexec\ton\nsetuid\ton\nxattr\tsa\nversion\t5\nutf8only\toff\nnormalization\tnone\ncasesensitivity\tsensitive\nnbmand\toff\nprimarycache\tall\nsecondarycache\tall\ndefer_destroy\toff\nuserrefs\t0\nmlslabel\tnone\nrefcompressratio\t1.05x\nwritten\t0\nclones\t\nlogicalused\t0\nlogicalreferenced\t10240\nacltype\tposixacl\ncontext\tnone\nfscontext\tnone\ndefcontext\tnone\nrootcontext\tnone\nzfssnap:version\t3.5.3\nzfssnap:repl_status\tsuccess\nzfssnap:label\treplicate-disconnected\n'
            return output
        monkeypatch.setattr(subprocess, 'check_output', mock_cmd)
        snapshot._refresh_properties()
        properties = {
            'normalization': 'none',
            'rootcontext': 'none',
            'zfssnap:repl_status':
            'success',
            'used': 0,
            'utf8only': 'off',
            'setuid': 'on',
            'creation': 1484840164,
            'refcompressratio': '1.05x',
            'clones': '',
            'fscontext': 'none',
            'zfssnap:version': '3.5.3',
            'zfssnap:label': 'replicate-disconnected',
            'defer_destroy': 'off',
            'referenced': 19456,
            'logicalused': 0,
            'primarycache': 'all',
            'context': 'none',
            'userrefs': 0,
            'acltype': 'posixacl',
            'type': 'snapshot',
            'defcontext': 'none',
            'mlslabel': 'none',
            'xattr': 'sa',
            'logicalreferenced': 10240,
            'exec': 'on',
            'casesensitivity': 'sensitive',
            'devices': 'on',
            'written': 0,
            'nbmand': 'off',
            'version': 5,
            'compressratio': '1.05x',
            'secondarycache': 'all'
        }
        assert snapshot._properties == properties
