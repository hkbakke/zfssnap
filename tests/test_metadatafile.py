import pytest
import datetime

from zfssnap import MetadataFile, MetadataFileException

class TestMetadataFile(object):
    @pytest.fixture
    def metadata(self):
        return MetadataFile('/tmp/test/abc_20170116T160746Z.json')

    @pytest.fixture
    def metadata_attr_tests(self, monkeypatch):
        metadata = MetadataFile('/tmp/test/abc_20170116T160746Z.json')

        def mock_write_metadata():
            return True

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
        return metadata

    def test_timestamp_property(self, metadata):
        timestamp = '20170116T160746Z'
        metadata.timestamp = timestamp
        assert metadata.timestamp == timestamp

    def test_snapshot_property(self, metadata):
        snapshot = 'zfssnap_20170116T160746Z'
        metadata.snapshot = snapshot
        assert metadata.snapshot == snapshot

    def test_depends_on_property(self, metadata):
        snapshot = 'zfssnap_20170116T073154Z'
        metadata.depends_on = snapshot
        assert metadata.depends_on == snapshot

    def test_version_property(self, metadata):
        version = '3.5.3'
        metadata.version = version
        assert metadata.version == version

    def test_label_property(self, metadata):
        label = 'test'
        metadata.label = label
        assert metadata.label == label

    def test_segments_property(self, metadata):
        segments = [
            'abc_20170116T160746Z-aaa',
            'abc_20170116T160746Z-aab',
            'abc_20170116T160746Z-aac',
            'abc_20170116T160746Z-aad'
        ]
        metadata.segments = segments
        assert metadata.segments == segments

    def test_metadata_datetime(self, metadata):
        metadata.timestamp = '20170116T160746Z'
        assert metadata.datetime == datetime.datetime(
            2017, 1, 16, 16, 7, 46, tzinfo=datetime.timezone.utc)

    def test_invalid_timestamp(self, metadata):
        with pytest.raises(MetadataFileException):
            # Replace Z with Y
            metadata.timestamp = '20170116T160746Y'

    def test_invalid_version(self, metadata):
        with pytest.raises(MetadataFileException):
            metadata.version = 1

    def test_empty_version(self, metadata):
        with pytest.raises(MetadataFileException):
            metadata.version = None

    def test_invalid_label(self, metadata):
        with pytest.raises(MetadataFileException):
            metadata.label = 1

    def test_empty_label(self, metadata):
        with pytest.raises(MetadataFileException):
            metadata.label = None

    def test_invalid_segments(self, metadata):
        with pytest.raises(MetadataFileException):
            metadata.segments = {}

    def test_empty_segments(self, metadata):
        with pytest.raises(MetadataFileException):
            metadata.segments = []

    def test_invalid_snapshot_name(self, metadata):
        with pytest.raises(MetadataFileException):
            metadata.snapshot = 'invalid@snapshot'

    def test_invalid_depends_on_name(self, metadata):
        with pytest.raises(MetadataFileException):
            metadata.depends_on = 'invalid@snapshot'

    def test_validate_snapshot_invalid_name(self, metadata):
        with pytest.raises(MetadataFileException):
            metadata._validate_snapshot_name('invalid_20170116T073154Z')

    def test_validate_snapshot_name(self, metadata):
        assert metadata._validate_snapshot_name(
            'zfssnap_20170116T073154Z') == 'zfssnap_20170116T073154Z'

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
        assert metadata.segments == ['abc_20170116T160746Z-aaa',
                                     'abc_20170116T160746Z-aab',
                                     'abc_20170116T160746Z-aac',
                                     'abc_20170116T160746Z-aad']
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
        with pytest.raises(MetadataFileException):
            metadata.read()

    def test_read_invalid_checksum_attr(self, monkeypatch, metadata):
        def mock_invalid_file():
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

        monkeypatch.setattr(metadata, '_read_file', mock_invalid_file)
        with pytest.raises(MetadataFileException):
            metadata.read()

    def test_write_file(self, monkeypatch, metadata):
        def mock_write_metadata(metadata):
            assert metadata['segments'] == ['abc_20170116T160746Z-aaa',
                                            'abc_20170116T160746Z-aab',
                                            'abc_20170116T160746Z-aac',
                                            'abc_20170116T160746Z-aad']
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

    def test_missing_version_on_write(self, monkeypatch, metadata_attr_tests):
        monkeypatch.setattr(metadata_attr_tests, '_version', None)
        with pytest.raises(MetadataFileException):
            metadata_attr_tests.write()

    def test_missing_timestamp_on_write(self, monkeypatch, metadata_attr_tests):
        monkeypatch.setattr(metadata_attr_tests, '_timestamp', None)
        with pytest.raises(MetadataFileException):
            metadata_attr_tests.write()

    def test_missing_snapshot_on_write(self, monkeypatch, metadata_attr_tests):
        monkeypatch.setattr(metadata_attr_tests, '_snapshot', None)
        with pytest.raises(MetadataFileException):
            metadata_attr_tests.write()

    def test_missing_segments_on_write(self, monkeypatch, metadata_attr_tests):
        monkeypatch.setattr(metadata_attr_tests, '_segments', [])
        with pytest.raises(MetadataFileException):
            metadata_attr_tests.write()

    def test_missing_label_on_write(self, monkeypatch, metadata_attr_tests):
        monkeypatch.setattr(metadata_attr_tests, '_label', None)
        with pytest.raises(MetadataFileException):
            metadata_attr_tests.write()
