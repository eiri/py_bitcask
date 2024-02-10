import ctypes
import io
import zlib

from uuid_extensions import uuid7

UINT_SZ = ctypes.sizeof(ctypes.c_uint)
ULONG_SZ = ctypes.sizeof(ctypes.c_ulong)
FILE_ID_SIZE = 1


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(
                *args, **kwargs
            )
        return cls._instances[cls]


class Bitcask(metaclass=Singleton):
    SIZE = slice(FILE_ID_SIZE, FILE_ID_SIZE + ULONG_SZ)
    POSITION = slice(FILE_ID_SIZE + ULONG_SZ, FILE_ID_SIZE + 2 * ULONG_SZ)

    def __init__(self):
        self.__cur = 0
        self.__keydir = {}
        self.__data = io.BytesIO()

    def open(self, dataDir):
        return True

    def get(self, key):
        if key not in self.__keydir:
            raise KeyError
        return self._get(self.__keydir[key])

    def _get(self, block):
        _, value_sz, value_pos, *_ = block
        value = bytearray(value_sz)
        self.__data.seek(value_pos)
        self.__data.readinto(value)
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
        self.__data.seek(self.__cur)
        self.__data.write(zlib.crc32(block).to_bytes(UINT_SZ) + block)
        self.__cur += UINT_SZ + len(block)
        self.__keydir[key] = (0, value_sz, self.__cur - value_sz, tstamp)
        return True

    def delete(self, key):
        if key not in self.__keydir:
            raise KeyError
        self._put(key, b"")
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
        self.__data.flush()
        return True

    def close(self):
        self.__data.close()
        return self.__data.closed
