import binascii

from uuid_extensions import uuid7


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(
                *args, **kwargs
            )
        return cls._instances[cls]


class Bitcask(metaclass=Singleton):
    def __init__(self):
        self.cur = 0
        self.keydir = {}
        self.data = bytearray()

    def open(self, dataDir):
        return True

    def get(self, key):
        if key not in self.keydir:
            raise KeyError
        key_block = self.keydir[key]
        value_sz = int.from_bytes(key_block[1:5])
        value_pos = int.from_bytes(key_block[5:9])
        return self.data[value_pos : value_pos + value_sz]

    def put(self, key, value):
        tstamp = uuid7().bytes
        val_len = len(value)
        block = bytes(
            tstamp + len(key).to_bytes(4) + val_len.to_bytes(4) + key + value
        )
        self.data += binascii.crc32(block).to_bytes(4) + block
        self.cur += 4 + len(block)
        self.keydir[key] = bytes(
            b"0"
            + len(value).to_bytes(4)
            + (self.cur - val_len).to_bytes(4)
            + tstamp
        )
        return True

    def delete(self, key):
        return True

    def list_keys(self):
        return []

    def fold(self, fun, acc):
        return []

    def merge(self):
        return True

    def sync(self):
        return True

    def close(self):
        return True
