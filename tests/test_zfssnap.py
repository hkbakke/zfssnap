import pytest

from zfssnap import autotype

class TestZFSSnap(object):
    def test_autotype_to_int(self):
        assert isinstance(autotype('123'), int)

    def test_autotype_to_str(self):
        assert isinstance(autotype('12f'), str)