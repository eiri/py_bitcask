import binascii
import ctypes

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
    V_VALUE = slice(FILE_ID_SIZE, FILE_ID_SIZE + ULONG_SZ)
    V_POSITION = slice(FILE_ID_SIZE + ULONG_SZ, FILE_ID_SIZE + 2 * ULONG_SZ)

    def __init__(self):
        self.__cur = 0
        self.__keydir = {}
        self.__data = bytearray()

    def open(self, dataDir):
        return True

    def get(self, key):
        if key not in self.__keydir:
            raise KeyError
        value_pos_bin = self.__keydir[key][self.V_POSITION]
        value_pos = int.from_bytes(value_pos_bin)
        value_sz_bin = self.__keydir[key][self.V_VALUE]
        value_sz = int.from_bytes(value_sz_bin)
        VALUE = slice(value_pos, value_pos + value_sz)
        return self.__data[VALUE]

    def put(self, key, value):
        if len(value) == 0:
            raise ValueError
        return self._put(key, value)

    def _put(self, key, value):
        tstamp = uuid7().bytes
        block = bytes(
            tstamp
            + len(key).to_bytes(ULONG_SZ)
            + len(value).to_bytes(ULONG_SZ)
            + key
            + value
        )
        self.__data += binascii.crc32(block).to_bytes(UINT_SZ) + block
        self.__cur += UINT_SZ + len(block)
        self.__keydir[key] = bytes(
            b"0"
            + len(value).to_bytes(ULONG_SZ)
            + (self.__cur - len(value)).to_bytes(ULONG_SZ)
            + tstamp
        )
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
        return []

    def merge(self):
        return True

    def sync(self):
        return True

    def close(self):
        return True
