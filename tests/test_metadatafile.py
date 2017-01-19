import pytest
import datetime

from zfssnap import MetadataFile, ZFSSnapException

class TestMetadataFile(object):
    def test_get_metadata_checksum(self):
        metadata = {
            'segments': [
                'abc_20170116T160746Z-aaa',
                'abc_20170116T160746Z-aab',
                'abc_20170116T160746Z-aac',
                'abc_20170116T160746Z-aad'
            ],
            'label': 'replicate-disconnected',
            'depends_on': 'zfssnap_20170116T073154Z',
            'snapshot': 'zfssnap_20170116T160746Z',
            'version': '3.5.3',
            'timestamp': '20170116T160746Z'
        }
        assert MetadataFile._get_checksum(metadata) == '79d539585d6e062f9592f475f8988f44'

    @pytest.fixture
    def metadata(self, monkeypatch):
        return MetadataFile('/tmp/test/abc_20170116T160746Z.json')

    def test_read_attrs(self, monkeypatch, metadata):
        def mock_read_file():
            return {
                'checksum': '79d539585d6e062f9592f475f8988f44',
                'segments': [
                    'abc_20170116T160746Z-aaa',
                    'abc_20170116T160746Z-aab',
                    'abc_20170116T160746Z-aac',
                    'abc_20170116T160746Z-aad'
                ],
                'timestamp': '20170116T160746Z',
                'snapshot': 'zfssnap_20170116T160746Z',
                'depends_on': 'zfssnap_20170116T073154Z',
                'label': 'replicate-disconnected',
                'version': '3.5.3'
            }

        monkeypatch.setattr(metadata, '_read_file', mock_read_file)
        metadata.read()
        assert metadata.segments == [
                'abc_20170116T160746Z-aaa',
                'abc_20170116T160746Z-aab',
                'abc_20170116T160746Z-aac',
                'abc_20170116T160746Z-aad'
            ]
        assert metadata.timestamp == '20170116T160746Z'
        assert metadata.snapshot == 'zfssnap_20170116T160746Z'
        assert metadata.depends_on == 'zfssnap_20170116T073154Z'
        assert metadata.label == 'replicate-disconnected'
        assert metadata.version == '3.5.3'

    def test_read_corrupted_attrs(self, monkeypatch, metadata):
        def mock_read_file_corrupted():
            # Change the value of version from 3 to 2 to invalidate the checksum
            return {
                'checksum': '79d539585d6e062f9592f475f8988f44',
                'segments': [
                    'abc_20170116T160746Z-aaa',
                    'abc_20170116T160746Z-aab',
                    'abc_20170116T160746Z-aac',
                    'abc_20170116T160746Z-aad'
                ],
                'timestamp': '20170116T160746Z',
                'snapshot': 'zfssnap_20170116T160746Z',
                'depends_on': 'zfssnap_20170116T073154Z',
                'label': 'replicate-disconnected',
                'version': '3.5.2'
            }

        monkeypatch.setattr(metadata, '_read_file', mock_read_file_corrupted)
        with pytest.raises(ZFSSnapException):
            metadata.read()

    def test_read_invalid_checksum_attr(self, monkeypatch, metadata):
        def mock_invalid_checksum():
            # Replace 44 with 33 at end of checksum to invalidate it
            return {
                'checksum': '79d539585d6e062f9592f475f8988f33',
                'segments': [
                    'abc_20170116T160746Z-aaa',
                    'abc_20170116T160746Z-aab',
                    'abc_20170116T160746Z-aac',
                    'abc_20170116T160746Z-aad'
                ],
                'timestamp': '20170116T160746Z',
                'snapshot': 'zfssnap_20170116T160746Z',
                'depends_on': 'zfssnap_20170116T073154Z',
                'label': 'replicate-disconnected',
                'version': '3.5.3'
            }

        monkeypatch.setattr(metadata, '_read_file', mock_invalid_checksum)
        with pytest.raises(ZFSSnapException):
            metadata.read()

    def test_write_file(self, monkeypatch, metadata):
        def mock_write_metadata(metadata):
            assert metadata['segments'] == [
                    'abc_20170116T160746Z-aaa',
                    'abc_20170116T160746Z-aab',
                    'abc_20170116T160746Z-aac',
                    'abc_20170116T160746Z-aad'
                ]
            assert metadata['timestamp'] == '20170116T160746Z'
            assert metadata['snapshot'] == 'zfssnap_20170116T160746Z'
            assert metadata['depends_on'] == 'zfssnap_20170116T073154Z'
            assert metadata['label'] == 'replicate-disconnected'
            assert metadata['version'] == '3.5.3'
            assert metadata['checksum'] == '79d539585d6e062f9592f475f8988f44'

        monkeypatch.setattr(metadata, '_write_file', mock_write_metadata)
        metadata.segments = [
            'abc_20170116T160746Z-aaa',
            'abc_20170116T160746Z-aab',
            'abc_20170116T160746Z-aac',
            'abc_20170116T160746Z-aad'
        ]
        metadata.timestamp = '20170116T160746Z'
        metadata.snapshot = 'zfssnap_20170116T160746Z'
        metadata.depends_on = 'zfssnap_20170116T073154Z'
        metadata.label = 'replicate-disconnected'
        metadata.version = '3.5.3'
        metadata.write()
        
    def test_metadata_datetime(self, metadata):
        metadata.timestamp = '20170116T160746Z'
        assert metadata.datetime == datetime.datetime(
            2017, 1, 16, 16, 7, 46, tzinfo=datetime.timezone.utc)