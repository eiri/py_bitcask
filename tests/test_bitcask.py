from py_bitcask import bitcask


def test_open():
    ok = bitcask.open('dir')
    assert ok


def test_get():
    value = bitcask.get('key')
    assert value == 'value'


def test_put():
    ok = bitcask.put('key', 'value')
    assert ok


def test_delete():
    ok = bitcask.delete('key')
    assert ok


def test_list_keys():
    keys = bitcask.list_keys()
    assert keys == []


def test_fold():
    resp = bitcask.fold(lambda x: x, [])
    assert resp == []


def test_merge():
    ok = bitcask.merge()
    assert ok


def test_close():
    ok = bitcask.close()
    assert ok
