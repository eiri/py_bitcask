import random
import string
import uuid

import pytest
from py_bitcask import Bitcask


def random_word(lower, upper, table=string.printable):
    return str.encode(
        "".join(
            random.choice(table) for _ in range(random.randint(lower, upper))
        )
    )


@pytest.fixture(scope="module")
def test_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("bitcask", numbered=True)


@pytest.fixture(scope="module")
def db():
    yield Bitcask()


@pytest.fixture(scope="module")
def randomized():
    yield {
        uuid.uuid4().bytes: (random_word(9, 49), random_word(3, 33))
        for _ in range(32)
    }


@pytest.fixture(scope="module")
def numbered():
    yield {uuid.uuid4().bytes: n.to_bytes(1) for n in range(1, 121)}


class TestBitcask:
    def test_open(self, db, test_dir):
        ok = db.open(test_dir)
        assert ok

    def test_open_invalid_dir(self, db):
        with pytest.raises(NotADirectoryError):
            db.open("missing")

    def test_open_opened(self, db, test_dir):
        with pytest.raises(RuntimeError):
            db.open(test_dir)

    def test_put(self, db, randomized):
        for key, value in randomized.items():
            ok = db.put(key, value[0])
            assert ok

    def test_invalid_put(self, db):
        with pytest.raises(ValueError):
            db.put(b"key", b"")

    def test_get(self, db, randomized):
        for key, expect in randomized.items():
            value = db.get(key)
            assert value == expect[0]

    def test_missing_get(self, db):
        with pytest.raises(KeyError):
            db.get(b"missing")

    def test_list_keys(self, db, randomized):
        expect = randomized.keys()
        keys = db.list_keys()
        assert len(keys) == len(expect)
        assert all(a == b for a, b in zip(keys, expect))

    def test_update(self, db, randomized):
        for key, expect in randomized.items():
            value = db.get(key)
            assert value == expect[0]
            new_value = expect[1]
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

    def test_close(self, db):
        ok = db.close()
        assert ok


class TestBitcaskOperations:
    def test_open(self, db, test_dir):
        ok = db.open(test_dir)
        assert ok

    def test_fold_map(self, db, numbered):
        # prepare key-values
        for key, value in numbered.items():
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


class TestBitcaskReopen:
    def test_open(self, db, test_dir):
        ok = db.open(test_dir)
        assert ok

    def test_put(self, db, randomized):
        for key, value in randomized.items():
            ok = db.put(key, value[0])
            assert ok

    def test_get(self, db, randomized):
        for key, expect in randomized.items():
            value = db.get(key)
            assert value == expect[0]

    def test_update(self, db, randomized):
        for key, expect in randomized.items():
            value = db.get(key)
            assert value == expect[0]
            new_value = expect[1]
            ok = db.put(key, new_value)
            assert ok
            updated_value = db.get(key)
            assert updated_value != value
            assert updated_value == new_value

    def test_close(self, db):
        ok = db.close()
        assert ok

    def test_reopen(self, db, test_dir):
        ok = db.open(test_dir)
        assert ok

    def test_reread(self, db, randomized):
        for key, expect in randomized.items():
            value = db.get(key)
            assert value == expect[1]

    def test_close_again(self, db):
        ok = db.close()
        assert ok


class TestInMemBitcask:
    def test_open(self, db):
        ok = db.open(":memory")
        assert ok

    def test_put(self, db, randomized):
        for key, value in randomized.items():
            ok = db.put(key, value[0])
            assert ok

    def test_get(self, db, randomized):
        for key, expect in randomized.items():
            value = db.get(key)
            assert value == expect[0]

    def test_close(self, db):
        ok = db.close()
        assert ok
