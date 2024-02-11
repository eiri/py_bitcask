import random
import string

import pytest
from py_bitcask import Bitcask


def random_word(lower, upper, table=string.printable):
    return str.encode(
        "".join(
            random.choice(table) for _ in range(random.randint(lower, upper))
        )
    )


# we can scope for module, but re-init on every func check singleton
@pytest.fixture()
def db():
    yield Bitcask()


@pytest.fixture(scope="module")
def abc():
    yield {random_word(1, 8): random_word(9, 49) for _ in range(32)}


@pytest.fixture(scope="module")
def seq():
    yield {
        random_word(4, 4, string.ascii_lowercase): n.to_bytes(1)
        for n in range(1, 121)
    }


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

    def test_update(self, db):
        for key in db.list_keys():
            value = db.get(key)
            new_value = random_word(3, 9)
            ok = db.put(key, new_value)
            assert ok
            updated_value = db.get(key)
            assert updated_value != value
            assert updated_value == new_value

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

    def test_fold_map(self, db, seq):
        # prepare key-values
        for key, value in seq.items():
            ok = db.put(key, value)
            assert ok

        def mapper(acc, val):
            acc.append(int.from_bytes(val))
            return acc

        map = db.fold(mapper, [])
        assert map == list(range(1, 121))

    def test_fold_reduce(self, db):
        fold = db.fold(lambda acc, val: acc + int.from_bytes(val), 0)
        assert fold == sum(range(1, 121))

    def test_delete_update_fold(self, db):
        keys = db.list_keys()
        assert len(keys) > 0
        for idx, key in enumerate(keys):
            if idx % 2 == 0:
                ok = db.delete(key)
                assert ok
            else:
                ok = db.put(key, (idx * 100).to_bytes(2))
                assert ok

        fold = db.fold(lambda acc, val: acc + int.from_bytes(val), 0)
        assert fold == sum(range(100, 12000, 200))

    def test_iterate(self, db):
        expect = list(range(100, 12000, 200))
        for idx, val in enumerate(db):
            assert val == expect[idx].to_bytes(2)

    def test_merge(self, db):
        ok = db.merge()
        assert ok

    def test_sync(self, db):
        ok = db.sync()
        assert ok

    def test_close(self, db):
        ok = db.close()
        assert ok
