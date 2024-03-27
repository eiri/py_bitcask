import os
import shutil
from collections import namedtuple
from dataclasses import dataclass
from functools import reduce
from io import BytesIO
from struct import pack, unpack
from typing import Any, Callable, Dict, List, Optional, Union
from zlib import crc32

import uuid_utils as uuid


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


@dataclass
class Hint:
    """
    Represents a hint item in a Bitcask hint file.

    Attributes:
    - tstamp (uuid.UUID): The timestamp associated with the key record as uuid7.
    - key_sz (int): The size of the key in bytes.
    - value_sz (int): The size of the value in bytes.
    - value_pos (int): The position of the value within the corresponding data file.
    - key (bytes): The key associated with the data value.
    """

    tstamp: Union[uuid.UUID, str, int, bytes]
    key_sz: int
    value_sz: int
    value_pos: int
    key: bytes


class Bitcask:
    DEFAULT_THRESHOLD = 1024
    HEADER_FORMAT = ">I16sLL"
    header_size = 28  # struct.calcsize(HEADER_FORMAT)
    HINT_FORMAT = ">16sLLL"
    hint_size = 28

    def __init__(self, threshold: Optional[int] = DEFAULT_THRESHOLD) -> None:
        """
        Initializes a new instance of the class.

        Parameters:
        - threshold (Optional[int]): The threshold for triggering reactivation.
        """
        self.threshold = threshold
        self._reset()

    def _reset(self) -> None:
        """
        Resets internal state variables to their initial values.

        This method resets the internal state of the object by clearing
        the key directory, resetting the iterator,
        clearing the directory mapping,
        and setting the active file descriptor to None.

        Returns:
        None
        """
        self.__dirname = None
        self.__active = None
        self.__keydir = {}
        self.__datadir = {}
        self.__iter = None

    def open(self, dataDir: str) -> bool:
        """
        Opens the storage in the specified data directory.

        Parameters:
        - dataDir (str): The data directory to open.

        Returns:
        bool: True if the operation is successful.

        Raises:
        NotADirectoryError: If the provided path is invalid.
        """
        if dataDir != ":memory" and not (
            os.path.exists(dataDir) and os.path.isdir(dataDir)
        ):
            raise NotADirectoryError(
                f"The path '{dataDir}' is not a directory."
            )
        if self.__dirname is not None:
            raise RuntimeError("Bitcask is already open.")
        self.__dirname = dataDir
        self._open()
        return True

    def _open(self) -> None:
        """
        Reads the storage in the specified data directory
        and propagates the keydir hash.

        Returns:
        None
        """
        if self.__dirname == ":memory":
            return
        hint_files = self._read_hints()
        self._open_with_hints(hint_files)

    def _open_with_hints(self, hint_files) -> None:
        """
        This utility function opens data files associated with each hint file
        and populates the key directory with key records extracted from
        the hint files.

        Parameters:
        - hint_files (Dict[str, List[Hint]]): A dictionary where
        keys are data file names in form of uuid7
        and values are lists of Hint objects representing hint files.

        Returns:
        None
        """
        for file_stem, hints in hint_files.items():
            file_id = crc32(file_stem.encode("utf-8"))
            file_name = os.path.join(self.__dirname, file_stem + ".db")
            current = open(file_name, "rb")
            self.__datadir[file_id] = current
            for hint in hints:
                self.__keydir[hint.key] = KeyRec(
                    file_id, hint.value_sz, hint.value_pos, hint.tstamp
                )

    def _read_hints(self) -> Optional[Dict[str, List[Hint]]]:
        """
        Reads data or hint files from the data directory
        and returns a dictionary of hint files.

        Returns:
        Optional[Dict[str, List[Hint]]]: A dictionary where
        keys are data file names in form of uuid7
        and values are lists of Hint objects representing hint files.

        Returns None if DataDir been set to ":memory".
        """
        KeyState = namedtuple("KeyState", "tstamp deleted file_id hint")

        def read_data_file(file_name, keys):
            with open(file_name, "rb") as current:
                while True:
                    data = current.read(self.header_size)
                    if not data:
                        break
                    _, ts_bytes, key_sz, value_sz = unpack(
                        self.HEADER_FORMAT, data
                    )
                    tstamp = uuid.UUID(int=int.from_bytes(ts_bytes, "big"))
                    key = current.read(key_sz)
                    value_pos = current.tell()
                    if key not in keys or keys[key].tstamp < tstamp:
                        hint = Hint(tstamp, key_sz, value_sz, value_pos, key)
                        deleted = value_sz == 0
                        keys[key] = KeyState(tstamp, deleted, file_id, hint)
                    current.seek(value_sz, 1)

        def read_hint_file(file_name, keys):
            with open(file_name, "rb") as current:
                while True:
                    data = current.read(self.hint_size)
                    if not data:
                        break
                    ts_bytes, key_sz, value_sz, value_pos = unpack(
                        self.HINT_FORMAT, data
                    )
                    tstamp = uuid.UUID(int=int.from_bytes(ts_bytes, "big"))
                    key = current.read(key_sz)
                    hint = Hint(tstamp, key_sz, value_sz, value_pos, key)
                    keys[key] = KeyState(tstamp, False, file_id, hint)

        if self.__dirname == ":memory":
            return
        keys = {}
        files = os.listdir(self.__dirname)
        for file in files:
            file_id, ext = os.path.splitext(file)
            if ext != ".db":
                continue
            file_name = os.path.join(self.__dirname, file_id + ".hint")
            if (
                os.path.isfile(file_name)
                and os.path.getsize(file_name) >= self.hint_size
            ):
                read_hint_file(file_name, keys)
                continue
            file_name = os.path.join(self.__dirname, file)
            if (
                os.path.isfile(file_name)
                and os.path.getsize(file_name) >= self.header_size
            ):
                read_data_file(file_name, keys)
        hint_files = {}
        for key_state in keys.values():
            if key_state.deleted:
                continue
            if key_state.file_id not in hint_files:
                hint_files[key_state.file_id] = []
            hint_files[key_state.file_id].append(key_state.hint)
        return hint_files

    def _reactivate(self) -> None:
        """
        Reactivates the storage by creating a new active storage file.
        """
        uid = str(uuid.uuid7())
        new_active = BytesIO()
        if self.__dirname != ":memory":
            if self.__active is not None:
                prev_active = self.__datadir[self.__active]
                prev_active.close()
                self.__datadir[self.__active] = open(prev_active.name, "rb")
            file_name = os.path.join(self.__dirname, uid + ".db")
            new_active = open(file_name, "a+b")
        self.__active = crc32(uid.encode("utf-8"))
        self.__datadir[self.__active] = new_active
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
        active = self.__datadir[keyrec.file_id]
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
        if self.__active is None or self.__cur > self.threshold:
            self._reactivate()
        tstamp = uuid.uuid7()
        key_sz = len(key)
        value_sz = len(value)
        head = pack(">16sLL", tstamp.bytes, key_sz, value_sz)
        crc = crc32(head)
        crc = crc32(key, crc)
        crc = pack(">I", crc32(value, crc))
        active = self.__datadir[self.__active]
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
        Applies a binary function to the elements in the storage,
        using an accumulator.

        Parameters:
        - fun (Callable[[Any, Union[bytes, Bitcask]], Any]): The binary function to be applied.
        - acc (Any): The initial accumulator value.

        Returns:
        Any: The final accumulator value.
        """
        return reduce(fun, self, acc)

    def __iter__(self):
        """
        Returns an iterator over the values
        associated with the keys in the storage.

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

    def merge(self) -> bool:
        """
        Runs merge operation on data dir removing all obsolete
        and deleted keys from immutable data files and creating
        hint files for new merged data files.

        Returns:
        bool: True if the operation is successful.

        Raises:
        RuntimeError: If bitcask is of type :memory.
        """
        if self.__dirname == ":memory":
            raise RuntimeError("Notsupported operation for type :memory.")
        # create bitcask instance for the latest values
        merge_cask = Bitcask(self.threshold)
        merge_dir = os.path.join(self.__dirname, "merge")
        os.makedirs(merge_dir)
        merge_cask.open(merge_dir)
        # store all the latest keys from immutable files in merge bitcask
        for key, keyrec in self.__keydir.items():
            if keyrec.file_id != self.__active:
                value = self._get(keyrec)
                merge_cask.put(key, value)
        # reset active file
        merge_cask._reactivate()
        # build and store hint fils for merged data files
        hint_files = merge_cask._read_hints()
        for file_stem, hints in hint_files.items():
            hint_file_name = os.path.join(merge_dir, file_stem + ".hint")
            hint_file = open(hint_file_name, "a+b")
            for hint in hints:
                head = pack(
                    self.HINT_FORMAT,
                    hint.tstamp.bytes,
                    hint.key_sz,
                    hint.value_sz,
                    hint.value_pos,
                )
                hint_file.write(head)
                hint_file.write(hint.key)
            hint_file.close()
        merge_cask.close()
        # move merged files in working dir, then delete merge dir
        for file in os.listdir(merge_dir):
            file_path = os.path.join(merge_dir, file)
            if os.path.isfile(file_path):
                shutil.move(file_path, os.path.join(self.__dirname, file))
        shutil.rmtree(merge_dir)
        # delete all old immutable keys and files
        keydir = self.__keydir.copy()
        for key, keyrec in keydir.items():
            if keyrec.file_id != self.__active:
                del self.__keydir[key]
        datadir = self.__datadir.copy()
        for file_id, file in datadir.items():
            if file_id != self.__active:
                file.close()
                os.remove(os.path.join(self.__dirname, file.name))
                del self.__datadir[file_id]
        # open all new files and propagate keydir from hint_files
        self._open_with_hints(hint_files)
        return True

    def sync(self) -> bool:
        """
        Force any writes to sync to disk.

        Returns:
        bool: True if the operation is successful.

        Raises:
        RuntimeError: If bitcask is of type :memory.
        """
        if self.__dirname == ":memory":
            raise RuntimeError("Notsupported operation for type :memory.")
        self.__datadir[self.__active].flush()
        return True

    def close(self) -> bool:
        """
        Closes the active storage file.

        Returns:
        bool: True if the file is closed.
        """
        if self.__active is None:
            self._reset()
            return True
        else:
            active = self.__datadir[self.__active]
            active.close()
            if active.closed:
                self._reset()
            return active.closed
