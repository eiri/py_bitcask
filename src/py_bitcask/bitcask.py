import io
import uuid
from dataclasses import dataclass
from functools import reduce
from struct import pack
from typing import Any, Callable, List, Optional, Union
from zlib import crc32

from uuid_extensions import uuid7


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(
                *args, **kwargs
            )
        return cls._instances[cls]


@dataclass
class KeyRec:
    """
    Represents a record for a key in the keydir index.

    Attributes:
    - file_id (int): The identifier of the storage file containing the value.
    - value_sz (int): The size of the value in bytes.
    - value_pos (int): The position of the value in the storage file.
    - tstamp (uuid.UUID): The timestamp associated with the key record as uuid7.
    """

    file_id: int
    value_sz: int
    value_pos: int
    tstamp: Union[uuid.UUID, str, int, bytes]


class Bitcask(metaclass=Singleton):
    DEFAULT_THRESHOLD = 1024

    def __init__(self, threshold: Optional[int] = DEFAULT_THRESHOLD) -> None:
        """
        Initializes a new instance of the class.

        Parameters:
        - threshold (Optional[int]): The threshold for triggering reactivation.
        """
        self.threshold = threshold
        self.__keydir = {}
        self.__iter = None
        self.__dir = {}
        self.__active = 0

    def open(self, dataDir: str) -> bool:
        """
        Opens the storage in the specified data directory.

        Parameters:
        - dataDir (str): The data directory to open.

        Returns:
        bool: True if the operation is successful.
        """
        self._reactivate()
        return True

    def _reactivate(self) -> None:
        """
        Reactivates the storage by creating a new active storage file.
        """
        active = io.BytesIO()
        self.__active = id(active)
        self.__dir[self.__active] = active
        self.__cur = 0

    def get(self, key: bytes) -> bytes:
        """
        Retrieves the value associated with the given key.

        Parameters:
        - key (bytes): The key for which the value is to be retrieved.

        Returns:
        bytes: The value associated with the key.

        Raises:
        KeyError: If the key is not present in the storage.
        """
        if key not in self.__keydir:
            raise KeyError("Key not found.")
        return self._get(self.__keydir[key])

    def _get(self, keyrec: KeyRec) -> bytes:
        """
        Retrieves the value associated with the given key record.

        Parameters:
        - keyrec (KeyRec): The keydir record containing information about the value.

        Returns:
        bytes: The value associated with the key record.
        """
        value = bytearray(keyrec.value_sz)
        active = self.__dir[keyrec.file_id]
        active.seek(keyrec.value_pos)
        active.readinto(value)
        return bytes(value)

    def put(self, key: bytes, value: bytes) -> bool:
        """
        Adds a key-value pair to the storage.

        Parameters:
        - key (bytes): The key to be added.
        - value (bytes): The value corresponding to the key.

        Returns:
        bool: True if the operation is successful.

        Raises:
        ValueError: If the length of the value is zero.
        """
        if len(value) == 0:
            raise ValueError("Value cannot be empty.")
        return self._put(key, value)

    def _put(self, key: bytes, value: bytes) -> bool:
        """
        Adds a key-value pair to the storage.

        Parameters:
        - key (bytes): The key to be added.
        - value (bytes): The value corresponding to the key.

        Returns:
        bool: True if the operation is successful.
        """
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
        self.__keydir[key] = KeyRec(
            self.__active,
            value_sz,
            self.__cur - value_sz,
            tstamp,
        )
        if self.__cur > self.threshold:
            self._reactivate()
        return True

    def delete(self, key: bytes) -> bool:
        """
        Deletes the key-value pair associated with the given key.

        Parameters:
        - key (bytes): The key to be deleted.

        Returns:
        bool: True if the operation is successful.

        Raises:
        KeyError: If the key is not present in the storage.
        """
        if key not in self.__keydir:
            raise KeyError("Key not found.")
        self._put(key, bytes())
        del self.__keydir[key]
        return True

    def list_keys(self) -> List[bytes]:
        """
        Returns a list of all keys present in the storage.

        Returns:
        List[bytes]: A list of keys.
        """
        return list(self.__keydir.keys())

    def fold(
        self, fun: Callable[[Any, Union[bytes, "Bitcask"]], Any], acc: Any
    ) -> Any:
        """
        Applies a binary function to the elements in the storage, using an accumulator.

        Parameters:
        - fun (Callable[[Any, Union[bytes, Bitcask]], Any]): The binary function to be applied.
        - acc (Any): The initial accumulator value.

        Returns:
        Any: The final accumulator value.
        """
        return reduce(fun, self, acc)

    def __iter__(self):
        """
        Returns an iterator over the values associated with the keys in the storage.

        Returns:
        Bitcask: An iterator object.
        """
        self.__iter = iter(self.__keydir.values())
        return self

    def __next__(self) -> bytes:
        """
        Returns the next value from the iterator.

        Returns:
        bytes: The next value associated with the key.

        Raises:
        StopIteration: If there are no more values in the iterator.
        """
        keyrec = next(self.__iter)
        return self._get(keyrec)

    def merge(self):
        return True

    def sync(self) -> bool:
        """
        Force any writes to sync to disk.

        Returns:
        bool: True if the operation is successful.
        """
        self.__dir[self.__active].flush()
        return True

    def close(self) -> bool:
        """
        Closes the active storage file.

        Returns:
        bool: True if the file is closed.
        """
        active = self.__dir[self.__active]
        active.close()
        return active.closed
