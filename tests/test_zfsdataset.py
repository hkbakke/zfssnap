import pytest
from zfssnap import ZFSHost, ZFSDataset
import subprocess


PROPERTY_PREFIX = 'zfssnap'


class TestZFSDataset(object):
    @pytest.fixture
    def fs(self):
        fs_name = 'zpool/dataset'
        host = ZFSHost()
        return ZFSDataset(host, fs_name)

    @pytest.fixture
    def ssh_fs(self):
        ssh_user = 'root'
        ssh_host = 'host'
        fs_name = 'zpool/dataset'
        host = ZFSHost(ssh_user=ssh_user, ssh_host=ssh_host)
        return ZFSDataset(host, fs_name)

    def test_autoconvert_to_int(self):
        assert isinstance(ZFSDataset._autoconvert('123'), int)

    def test_autoconvert_to_str(self):
        assert isinstance(ZFSDataset._autoconvert('12f'), str)

    def test_return_local_location(self, fs):
        assert fs.location == 'zpool/dataset'

    def test_return_ssh_location(self, ssh_fs):
        assert ssh_fs.location == 'root@host:zpool/dataset'

    def test_get_properties(self, fs, monkeypatch):
        def mock_subprocess_output(cmd):
            output = b'type\tfilesystem\ncreation\t1457005609\nused\t6409162752\navailable\t4084835488563\nreferenced\t6409064448\ncompressratio\t1.01x\nmounted\tyes\nquota\t0\nreservation\t0\nrecordsize\t131072\nmountpoint\t/zpools/zpool/dataset\nsharenfs\toff\nchecksum\ton\ncompression\tlz4\natime\ton\ndevices\ton\nexec\ton\nsetuid\ton\nreadonly\toff\nzoned\toff\nsnapdir\thidden\naclinherit\trestricted\ncanmount\ton\nxattr\tsa\ncopies\t1\nversion\t5\nutf8only\toff\nnormalization\tnone\ncasesensitivity\tsensitive\nvscan\toff\nnbmand\toff\nsharesmb\toff\nrefquota\t0\nrefreservation\t0\nprimarycache\tall\nsecondarycache\tall\nusedbysnapshots\t98304\nusedbydataset\t6409064448\nusedbychildren\t0\nusedbyrefreservation\t0\nlogbias\tlatency\ndedup\toff\nmlslabel\tnone\nsync\tstandard\nrefcompressratio\t1.01x\nwritten\t0\nlogicalused\t6473229312\nlogicalreferenced\t6473182208\nfilesystem_limit\t18446744073709551615\nsnapshot_limit\t18446744073709551615\nfilesystem_count\t18446744073709551615\nsnapshot_count\t18446744073709551615\nsnapdev\thidden\nacltype\tposixacl\ncontext\tnone\nfscontext\tnone\ndefcontext\tnone\nrootcontext\tnone\nrelatime\ton\nredundant_metadata\tall\noverlay\toff\n'
            return output

        result = {
            'sync': 'standard',
            'referenced': 6409064448,
            'sharenfs': 'off',
            'filesystem_limit': 18446744073709551615,
            'usedbydataset': 6409064448,
            'snapshot_limit': 18446744073709551615,
            'acltype': 'posixacl',
            'filesystem_count': 18446744073709551615,
            'snapshot_count': 18446744073709551615,
            'usedbyrefreservation': 0,
            'utf8only': 'off',
            'creation': 1457005609,
            'recordsize': 131072,
            'used': 6409162752,
            'canmount': 'on',
            'mlslabel': 'none',
            'devices': 'on',
            'snapdir': 'hidden',
            'mountpoint':
            '/zpools/zpool/dataset',
            'logicalused': 6473229312,
            'refreservation': 0,
            'secondarycache': 'all',
            'defcontext': 'none',
            'rootcontext': 'none',
            'usedbysnapshots': 98304,
            'xattr': 'sa',
            'sharesmb': 'off',
            'aclinherit': 'restricted',
            'snapdev': 'hidden',
            'redundant_metadata': 'all',
            'dedup': 'off',
            'refquota': 0,
            'version': 5,
            'refcompressratio': '1.01x',
            'exec': 'on',
            'overlay': 'off',
            'normalization': 'none',
            'reservation': 0,
            'atime': 'on',
            'readonly': 'off',
            'casesensitivity': 'sensitive',
            'fscontext': 'none',
            'available': 4084835488563,
            'written': 0,
            'setuid': 'on',
            'copies': 1,
            'logbias': 'latency',
            'type': 'filesystem',
            'checksum': 'on',
            'zoned': 'off',
            'relatime': 'on',
            'compressratio': '1.01x',
            'context': 'none',
            'quota': 0,
            'usedbychildren': 0,
            'mounted': 'yes',
            'logicalreferenced': 6473182208,
            'vscan': 'off',
            'nbmand': 'off',
            'primarycache': 'all',
            'compression': 'lz4'
        }

        monkeypatch.setattr(subprocess, 'check_output', mock_subprocess_output)
        assert fs.get_properties() == result