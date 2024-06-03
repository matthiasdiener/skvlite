import os
import pickle
import sqlite3
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

        # isolation_level=None: enable autocommit mode
        # https://www.sqlite.org/lang_transaction.html#implicit_versus_explicit_transactions
        self.conn = sqlite3.connect(self.filename, isolation_level=None)

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS dict "
            "(keyhash TEXT NOT NULL PRIMARY KEY, key_value TEXT NOT NULL)"
            )

        # https://www.sqlite.org/wal.html
        if enable_wal:
            print("Enabling WAL mode")
            self.conn.execute("PRAGMA journal_mode = 'WAL'")

        # Use in-memory temp store
        # https://www.sqlite.org/pragma.html#pragma_temp_store
        self.conn.execute("PRAGMA temp_store = 'MEMORY'")

        # https://www.sqlite.org/pragma.html#pragma_synchronous
        self.conn.execute("PRAGMA synchronous = 'NORMAL'")

        # 64 MByte of cache
        # https://www.sqlite.org/pragma.html#pragma_cache_size
        self.conn.execute("PRAGMA cache_size = -64000")

    def _collision_check(self, key: K, stored_key: K) -> None:
        if stored_key != key:
            print(stored_key, key)
            # Key collision, oh well.
            raise Exception(f"{self.identifier}: key collision in cache at "
                    f"'{self.container_dir}' -- these are sufficiently unlikely "
                    "that they're often indicative of a broken hash key "
                    "implementation (that is not considering some elements "
                    "relevant for equality comparison)"
                 )

    def store(self, key: K, value: V, _skip_if_present: bool = False) -> None:
        keyhash = self.key_builder(key)
        v = pickle.dumps((key, value))

        mode = "IGNORE" if _skip_if_present else "REPLACE"

        while True:
            try:
                self.conn.execute(f"INSERT OR {mode} INTO dict VALUES (?, ?)",
                                (keyhash, v))
            except sqlite3.OperationalError as e:
                # If the database is busy, retry
                if (hasattr(e, "sqlite_errorcode")
                    and not e.sqlite_errorcode == sqlite3.SQLITE_BUSY):
                    raise
            else:
                break

    def fetch(self, key: K) -> V:
        keyhash = self.key_builder(key)

        c = self.conn.execute("SELECT key_value FROM dict WHERE keyhash=?",
                              (keyhash,))
        row = c.fetchone()
        if row is None:
            raise NoSuchEntryError(key)

        stored_key, value = pickle.loads(row[0])
        self._collision_check(key, stored_key)
        return value

    def __setitem__(self, key: K, value: V) -> None:
        self.store(key, value)

    def __getitem__(self, key: K) -> V:
        return self.fetch(key)

    def remove(self, key: K) -> None:
        """Remove the entry associated with *key* from the dictionary."""
        keyhash = self.key_builder(key)

        while True:
            try:
                self.conn.execute("BEGIN EXCLUSIVE TRANSACTION")

                try:
                    # This is split into SELECT/DELETE to allow for a collision check
                    c = self.conn.execute("SELECT key_value FROM dict WHERE keyhash=?",
                                        (keyhash,))
                    row = c.fetchone()
                    if row is None:
                        raise NoSuchEntryError(key)

                    stored_key, _value = pickle.loads(row[0])
                    self._collision_check(key, stored_key)

                    self.conn.execute("DELETE FROM dict WHERE keyhash=?", (keyhash,))
                    self.conn.execute("COMMIT")
                except Exception as e:
                    self.conn.execute("ROLLBACK")
                    raise e
            except sqlite3.OperationalError as e:
                # If the database is busy, retry
                if (hasattr(e, "sqlite_errorcode")
                    and not e.sqlite_errorcode == sqlite3.SQLITE_BUSY):
                    raise
            else:
                break

    def __delitem__(self, key: K) -> None:
        """Remove the entry associated with *key* from the dictionary."""
        self.remove(key)

    def __len__(self) -> int:
        """Return the number of entries in the dictionary."""
        return next(self.conn.execute("SELECT COUNT(*) FROM dict"))[0]

    def __iter__(self) -> Generator[K, None, None]:
        """Return an iterator over the keys in the dictionary."""
        return self.keys()

    def keys(self) -> Generator[K, None, None]:
        """Return an iterator over the keys in the dictionary."""
        for row in self.conn.execute("SELECT key_value FROM dict ORDER BY rowid"):
            yield pickle.loads(row[0])[0]

    def values(self) -> Generator[V, None, None]:
        """Return an iterator over the values in the dictionary."""
        for row in self.conn.execute("SELECT key_value FROM dict ORDER BY rowid"):
            yield pickle.loads(row[0])[1]

    def items(self) -> Generator[Tuple[K, V], None, None]:
        """Return an iterator over the items in the dictionary."""
        for row in self.conn.execute("SELECT key_value FROM dict ORDER BY rowid"):
            yield pickle.loads(row[0])

    def size(self) -> int:
        """Return the size of the dictionary in bytes."""
        return next(self.conn.execute("SELECT page_size * page_count FROM "
                          "pragma_page_size(), pragma_page_count()"))[0]

    def __repr__(self) -> str:
        """Return a string representation of the dictionary."""
        return f"{type(self).__name__}({self.filename}, nitems={len(self)})"

    def clear(self) -> None:
        """Remove all entries from the dictionary."""
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
        keyhash = self.key_builder(key)
        v = pickle.dumps((key, value))

        while True:
            try:
                try:
                    self.conn.execute("INSERT INTO dict VALUES (?, ?)", (keyhash, v))
                except sqlite3.IntegrityError:
                    if not _skip_if_present:
                        raise ReadOnlyEntryError("WriteOncePersistentDict, "
                                                "tried overwriting key")
            except sqlite3.OperationalError as e:
                # If the database is busy, retry
                if (hasattr(e, "sqlite_errorcode")
                    and not e.sqlite_errorcode == sqlite3.SQLITE_BUSY):
                    raise
            else:
                break


    def _fetch(self, keyhash: str) -> Tuple[K, V]:  # pylint:disable=method-hidden
        # This method is separate from fetch() to allow for LRU caching
        c = self.conn.execute("SELECT key_value FROM dict WHERE keyhash=?",
                              (keyhash,))
        row = c.fetchone()
        if row is None:
            raise KeyError
        return pickle.loads(row[0])

    def fetch(self, key: K) -> V:
        keyhash = self.key_builder(key)

        try:
            stored_key, value = self._fetch(keyhash)
        except KeyError:
            raise NoSuchEntryError(key)
        else:
            self._collision_check(key, stored_key)
            return value

    def __delitem__(self, key: Any) -> None:
        raise AttributeError("Write-once KVStore")
