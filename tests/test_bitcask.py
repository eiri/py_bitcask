import pytest
from py_bitcask import Bitcask


@pytest.fixture()
def db():
    yield Bitcask()


class TestBitcask:
    def test_open(self, db):
        ok = db.open('dir')
        assert ok

    def test_get(self, db):
        value = db.get('key')
        assert value == 'value'

    def test_put(self, db):
        ok = db.put('key', 'value')
        assert ok

    def test_delete(self, db):
        ok = db.delete('key')
        assert ok

    def test_list_keys(self, db):
        keys = db.list_keys()
        assert keys == []

    def test_fold(self, db):
        resp = db.fold(lambda x: x, [])
        assert resp == []

    def test_merge(self, db):
        ok = db.merge()
        assert ok

    def test_sync(self, db):
        ok = db.sync()
        assert ok

    def test_close(self, db):
        ok = db.close()
        assert ok
