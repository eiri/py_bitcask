import ctypes
import io
import zlib
from collections import namedtuple

from uuid_extensions import uuid7

UINT_SZ = ctypes.sizeof(ctypes.c_uint)
ULONG_SZ = ctypes.sizeof(ctypes.c_ulong)

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

    def _get(self, block):
        value = bytearray(block.value_sz)
        active = self.__dir[block.file_id]
        active.seek(block.value_pos)
        active.readinto(value)
        return bytes(value)

    def put(self, key, value):
        if len(value) == 0:
            raise ValueError
        return self._put(key, value)

    def _put(self, key, value):
        tstamp = uuid7().bytes
        key_sz = len(key)
        value_sz = len(value)
        block = bytes(
            tstamp
            + key_sz.to_bytes(ULONG_SZ)
            + value_sz.to_bytes(ULONG_SZ)
            + key
            + value
        )
        active = self.__dir[self.__active]
        active.seek(self.__cur)
        active.write(zlib.crc32(block).to_bytes(UINT_SZ) + block)
        self.__cur += UINT_SZ + len(block)
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
        for block in self.__keydir.values():
            acc = fun(self._get(block), acc)
        return acc

    def merge(self):
        return True

    def sync(self):
        self.__dir[self.__active].flush()
        return True

    def close(self):
        active = self.__dir[self.__active]
        active.close()
        return active.closed
