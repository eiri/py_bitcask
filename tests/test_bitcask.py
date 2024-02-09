import random
import string

import pytest
from py_bitcask import Bitcask


def random_word(lower, upper):
    return str.encode(
        "".join(
            random.choice(string.printable)
            for _ in range(random.randint(lower, upper))
        )
    )


# we can scope for module, but re-init on every func check singleton
@pytest.fixture()
def db():
    yield Bitcask()


@pytest.fixture(scope="module")
def abc():
    yield {random_word(1, 8): random_word(9, 49) for _ in range(32)}


class TestBitcask:
    def test_open(self, db):
        ok = db.open("dir")
        assert ok

    def test_put(self, db, abc):
        for key, value in abc.items():
            ok = db.put(key, value)
            assert ok

    def test_invalid_put(self, db):
        with pytest.raises(ValueError):
            db.put(b"key", b"")

    def test_get(self, db, abc):
        for key, expect in abc.items():
            value = db.get(key)
            assert value == expect

    def test_missing_get(self, db):
        with pytest.raises(KeyError):
            db.get(b"missing")

    def test_list_keys(self, db, abc):
        expect = abc.keys()
        keys = db.list_keys()
        assert len(keys) == len(expect)
        assert all(a == b for a, b in zip(keys, expect))

    def test_fold(self, db):
        resp = db.fold(lambda x: x, [])
        assert resp == []

    def test_delete(self, db):
        keys = db.list_keys()
        assert len(keys) > 0
        for key in keys:
            db.get(key)
            ok = db.delete(key)
            assert ok
            with pytest.raises(KeyError):
                db.get(key)
        keys = db.list_keys()
        assert len(keys) == 0

    def test_missing_delete(self, db):
        with pytest.raises(KeyError):
            db.delete(b"missing")

    def test_merge(self, db):
        ok = db.merge()
        assert ok

    def test_sync(self, db):
        ok = db.sync()
        assert ok

    def test_close(self, db):
        ok = db.close()
        assert ok
