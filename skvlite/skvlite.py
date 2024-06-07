import os
import pickle
import sqlite3
import zlib
from os.path import join
from typing import Any, Generator, Mapping, Optional, Tuple, TypeVar
from pytools.persistent_dict import KeyBuilder

K = TypeVar("K")
V = TypeVar("V")

class NoSuchEntryError(KeyError):
    """Raised when an entry is not found in a :class:`PersistentDict`."""
    pass

class NoSuchEntryCollisionError(NoSuchEntryError):
    """Raised when an entry is not found in a :class:`PersistentDict`, but it
    contains an entry with the same hash key (hash collision)."""
    pass

class ReadOnlyEntryError(KeyError):
    """Raised when an attempt is made to overwrite an entry in a
    :class:`WriteOncePersistentDict`."""
    pass

class CollisionWarning(UserWarning):
    """Warning raised when a collision is detected in a :class:`PersistentDict`."""
    pass

class KVStore(Mapping[K, V]):
    def __init__(self, filename: str, container_dir: Optional[str] = None,
                 enable_wal: bool = False) -> None:
        os.makedirs(container_dir, exist_ok=True)
        self.filename = join(container_dir, filename + ".sqlite")
        self.container_dir = container_dir
        self.key_builder = KeyBuilder()
        self.conn = sqlite3.connect(self.filename, isolation_level=None)
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS dict "
            "(keyhash TEXT NOT NULL PRIMARY KEY, key_value BLOB NOT NULL)"
            )

        if enable_wal:
            self.conn.execute("PRAGMA journal_mode = 'WAL'")

        self.conn.execute("PRAGMA temp_store = 2")
        self.conn.execute("PRAGMA synchronous = 1")
        self.conn.execute("PRAGMA cache_size = -64000")

    def _collision_check(self, key: K, stored_key: K) -> None:
        if stored_key != key:
            print(stored_key, key)
            raise Exception(f"{self.identifier}: key collision in cache at "
                    f"'{self.container_dir}' -- these are sufficiently unlikely "
                    "that they're often indicative of a broken hash key "
                    "implementation (that is not considering some elements "
                    "relevant for equality comparison)")

    def _store_data(self, key: K, value: V, replace: bool) -> None:
        keyhash = self.key_builder(key)
        pickled_data = pickle.dumps((key, value))
        compressed_data = zlib.compress(pickled_data)
        
        if replace:
            self.conn.execute("INSERT OR REPLACE INTO dict VALUES (?, ?)", (keyhash, compressed_data))
        else:
            self.conn.execute("INSERT OR IGNORE INTO dict VALUES (?, ?)", (keyhash, compressed_data))

    def _load_data(self, keyhash: str) -> Tuple[K, V]:
        c = self.conn.execute("SELECT key_value FROM dict WHERE keyhash=?", (keyhash,))
        row = c.fetchone()
        if row is None:
            raise NoSuchEntryError(keyhash)
        compressed_data = row[0]
        pickled_data = zlib.decompress(compressed_data)
        return pickle.loads(pickled_data)

    def store(self, key: K, value: V, _skip_if_present: bool = False) -> None:
        self._store_data(key, value, not _skip_if_present)

    def fetch(self, key: K) -> V:
        keyhash = self.key_builder(key)
        stored_key, value = self._load_data(keyhash)
        self._collision_check(key, stored_key)
        return value

    def __setitem__(self, key: K, value: V) -> None:
        self.store(key, value)

    def __getitem__(self, key: K) -> V:
        return self.fetch(key)

    def remove(self, key: K) -> None:
        keyhash = self.key_builder(key)

        self.conn.execute("BEGIN EXCLUSIVE TRANSACTION")

        try:
            c = self.conn.execute("SELECT key_value FROM dict WHERE keyhash=?",
                                (keyhash,))
            row = c.fetchone()
            if row is None:
                raise NoSuchEntryError(key)

            compressed_data = row[0]
            pickled_data = zlib.decompress(compressed_data)
            stored_key, _value = pickle.loads(pickled_data)
            self._collision_check(key, stored_key)

            self.conn.execute("DELETE FROM dict WHERE keyhash=?", (keyhash,))
            self.conn.execute("COMMIT")
        except Exception as e:
            self.conn.execute("ROLLBACK")
            raise e

    def __delitem__(self, key: K) -> None:
        self.remove(key)

    def __len__(self) -> int:
        return next(self.conn.execute("SELECT COUNT(*) FROM dict"))[0]

    def __iter__(self) -> Generator[K, None, None]:
        return self.keys()

    def keys(self) -> Generator[K, None, None]:
        for row in self.conn.execute("SELECT key_value FROM dict ORDER BY rowid"):
            pickled_data = zlib.decompress(row[0])
            yield pickle.loads(pickled_data)[0]

    def values(self) -> Generator[V, None, None]:
        for row in self.conn.execute("SELECT key_value FROM dict ORDER BY rowid"):
            pickled_data = zlib.decompress(row[0])
            yield pickle.loads(pickled_data)[1]

    def items(self) -> Generator[Tuple[K, V], None, None]:
        for row in self.conn.execute("SELECT key_value FROM dict ORDER BY rowid"):
            pickled_data = zlib.decompress(row[0])
            yield pickle.loads(pickled_data)

    def size(self) -> int:
        return next(self.conn.execute("SELECT page_size * page_count FROM "
                          "pragma_page_size(), pragma_page_count()"))[0]

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.filename}, nitems={len(self)})"

    def clear(self) -> None:
        self.conn.execute("DELETE FROM dict")

    def store_if_not_present(self, key: Any, value: Any) -> None:
        self.store(key, value, _skip_if_present=True)

    def vacuum(self) -> None:
        self.conn.execute("VACUUM")

    def close(self) -> None:
        self.conn.close()

class ReadOnlyKVStore(KVStore):
    def __setitem__(self, key: Any, value: Any) -> None:
        raise AttributeError("Read-only KVStore")

    def __delitem__(self, key: Any) -> None:
        raise AttributeError("Read-only KVStore")

class WriteOnceKVStore(KVStore):
    def store(self, key: K, value: V, _skip_if_present: bool = False) -> None:
        self._store_data(key, value, not _skip_if_present)

    def fetch(self, key: K) -> V:
        keyhash = self.key_builder(key)
        stored_key, value = self._load_data(keyhash)
        self._collision_check(key, stored_key)
        return value

    def __delitem__(self, key: Any) -> None:
        raise AttributeError("Write-once KVStore")
