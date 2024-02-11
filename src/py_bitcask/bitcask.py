import io
from collections import namedtuple
from functools import reduce
from struct import pack
from zlib import crc32

from uuid_extensions import uuid7

DEFAULT_THRESHOLD = 1024


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(
                *args, **kwargs
            )
        return cls._instances[cls]


class Bitcask(metaclass=Singleton):
    KeyRec = namedtuple("KeyRec", "file_id value_sz value_pos tstamp")

    def __init__(self, threshold=DEFAULT_THRESHOLD):
        self.threshold = threshold
        self.__keydir = {}
        self.__iter = None
        self.__dir = {}
        self.__active = 0

    def open(self, dataDir):
        self._reactivate()
        return True

    def _reactivate(self):
        active = io.BytesIO()
        self.__active = id(active)
        self.__dir[self.__active] = active
        self.__cur = 0

    def get(self, key):
        if key not in self.__keydir:
            raise KeyError
        return self._get(self.__keydir[key])

    def _get(self, keyrec):
        value = bytearray(keyrec.value_sz)
        active = self.__dir[keyrec.file_id]
        active.seek(keyrec.value_pos)
        active.readinto(value)
        return bytes(value)

    def put(self, key, value):
        if len(value) == 0:
            raise ValueError
        return self._put(key, value)

    def _put(self, key, value):
        tstamp = uuid7()
        key_sz = len(key)
        value_sz = len(value)
        head = bytes(tstamp.bytes + pack(">LL", key_sz, value_sz))
        crc = crc32(head)
        crc = crc32(key, crc)
        crc = pack(">I", crc32(value, crc))
        active = self.__dir[self.__active]
        active.seek(self.__cur)
        active.write(crc)
        active.write(head)
        active.write(key)
        active.write(value)
        self.__cur += len(crc) + len(head) + key_sz + value_sz
        self.__keydir[key] = self.KeyRec(
            self.__active,
            value_sz,
            self.__cur - value_sz,
            tstamp,
        )
        if self.__cur > self.threshold:
            self._reactivate()
        return True

    def delete(self, key):
        if key not in self.__keydir:
            raise KeyError
        self._put(key, bytes())
        del self.__keydir[key]
        return True

    def list_keys(self):
        return list(self.__keydir.keys())

    def fold(self, fun, acc):
        return reduce(fun, self, acc)

    def __iter__(self):
        self.__iter = iter(self.__keydir.values())
        return self

    def __next__(self):
        keyrec = next(self.__iter)
        return self._get(keyrec)

    def merge(self):
        return True

    def sync(self):
        self.__dir[self.__active].flush()
        return True

    def close(self):
        active = self.__dir[self.__active]
        active.close()
        return active.closed
