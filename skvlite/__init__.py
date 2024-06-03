import os
import pickle
import sqlite3
from typing import cast, Any, Generator, Mapping, Optional, Tuple, TypeVar

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
        from os.path import join

        if container_dir is None:
            import platformdirs
            import sys

            if sys.platform == "darwin" and os.getenv("XDG_CACHE_HOME") is not None:
                from typing import cast
                # platformdirs does not handle XDG_CACHE_HOME on macOS
                # https://github.com/platformdirs/platformdirs/issues/269
                container_dir = join(
                    cast(str, os.getenv("XDG_CACHE_HOME")), "pytools")
            else:
                container_dir = platformdirs.user_cache_dir("pytools", "pytools")

        os.makedirs(container_dir, exist_ok=True)
        self.filename = join(container_dir, filename + ".sqlite")

        self.key_builder = KeyBuilder()

        # isolation_level=None: enable autocommit mode
        # https://www.sqlite.org/lang_transaction.html#implicit_versus_explicit_transactions
        self.conn = sqlite3.connect(self.filename, isolation_level=None)

        self._exec_sql(
            "CREATE TABLE IF NOT EXISTS dict "
            "(keyhash TEXT NOT NULL PRIMARY KEY, key_value TEXT NOT NULL)"
        )

        # https://www.sqlite.org/wal.html
        if enable_wal:
            print("Enabling WAL mode")
            self._exec_sql("PRAGMA journal_mode = 'WAL'")

        # Use in-memory temp store
        # https://www.sqlite.org/pragma.html#pragma_temp_store
        self._exec_sql("PRAGMA temp_store = 'MEMORY'")

        # https://www.sqlite.org/pragma.html#pragma_synchronous
        self._exec_sql("PRAGMA synchronous = 'NORMAL'")

        # 64 MByte of cache
        # https://www.sqlite.org/pragma.html#pragma_cache_size
        self._exec_sql("PRAGMA cache_size = -64000")

    def _exec_sql(self, *args: Any) -> Any:
        while True:
            try:
                return self.conn.execute(*args)
            except sqlite3.OperationalError as e:
                # If the database is busy, retry
                if (hasattr(e, "sqlite_errorcode")
                        and not e.sqlite_errorcode == sqlite3.SQLITE_BUSY):
                    raise
            else:
                break

    def _collision_check(self, key: K, stored_key: K) -> None:
        if stored_key != key:
            # Key collision, oh well.
            raise Exception(
                f"Key collision in cache at "
                f"'{self.filename}' -- these are sufficiently unlikely "
                "that they're often indicative of a broken hash key "
                "implementation (that is not considering some elements "
                "relevant for equality comparison)"
            )

    def store(self, key: K, value: V, _skip_if_present: bool = False) -> None:
        keyhash = self.key_builder(key)
        v = pickle.dumps((key, value))

        mode = "IGNORE" if _skip_if_present else "REPLACE"

        self._exec_sql(f"INSERT OR {mode} INTO dict VALUES (?, ?)",
                       (keyhash, v))

    def fetch(self, key: K) -> V:
        keyhash = self.key_builder(key)

        c = self._exec_sql("SELECT key_value FROM dict WHERE keyhash=?",
                           (keyhash,))
        row = c.fetchone()
        if row is None:
            raise NoSuchEntryError(key)

        stored_key, value = pickle.loads(row[0])
        self._collision_check(key, stored_key)

        return cast(V, value)

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
                    c = self.conn.execute("SELECT key_value FROM dict "
                                          "WHERE keyhash=?",
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
        return cast(int, next(self._exec_sql("SELECT COUNT(*) FROM dict"))[0])

    def __iter__(self) -> Generator[K, None, None]:
        """Return an iterator over the keys in the dictionary."""
        return self.keys()

    def keys(self) -> Generator[K, None, None]:  # type: ignore[override]
        """Return an iterator over the keys in the dictionary."""
        for row in self._exec_sql("SELECT key_value FROM dict ORDER BY rowid"):
            yield pickle.loads(row[0])[0]

    def values(self) -> Generator[V, None, None]:  # type: ignore[override]
        """Return an iterator over the values in the dictionary."""
        for row in self._exec_sql("SELECT key_value FROM dict ORDER BY rowid"):
            yield pickle.loads(row[0])[1]

    def items(self) -> Generator[Tuple[K, V], None, None]:  # type: ignore[override]
        """Return an iterator over the items in the dictionary."""
        for row in self._exec_sql("SELECT key_value FROM dict ORDER BY rowid"):
            yield pickle.loads(row[0])

    def nbytes(self) -> int:
        """Return the size of the dictionary in bytes."""
        return cast(int,
                    next(self._exec_sql("SELECT page_size * page_count FROM "
                                        "pragma_page_size(), pragma_page_count()")
                         )[0]
                    )

    def __repr__(self) -> str:
        """Return a string representation of the dictionary."""
        return f"{type(self).__name__}({self.filename}, nitems={len(self)})"

    def clear(self) -> None:
        """Remove all entries from the dictionary."""
        self._exec_sql("DELETE FROM dict")

    def store_if_not_present(self, key: Any, value: Any) -> None:
        self.store(key, value, _skip_if_present=True)

    def vacuum(self) -> None:
        self._exec_sql("VACUUM")

    def close(self) -> None:
        self.conn.close()


class ReadOnlyKVStore(KVStore[K, V]):
    def __setitem__(self, key: Any, value: Any) -> None:
        raise AttributeError("Read-only KVStore")

    def __delitem__(self, key: Any) -> None:
        raise AttributeError("Read-only KVStore")


class WriteOnceKVStore(KVStore[K, V]):
    def store(self, key: K, value: V, _skip_if_present: bool = False) -> None:
        keyhash = self.key_builder(key)
        v = pickle.dumps((key, value))

        try:
            self._exec_sql("INSERT INTO dict VALUES (?, ?)", (keyhash, v))
        except sqlite3.IntegrityError:
            if not _skip_if_present:
                raise ReadOnlyEntryError("WriteOncePersistentDict, "
                                         "tried overwriting key")

    def _fetch(self, keyhash: str) -> Tuple[K, V]:  # pylint:disable=method-hidden
        # This method is separate from fetch() to allow for LRU caching
        c = self._exec_sql("SELECT key_value FROM dict WHERE keyhash=?",
                           (keyhash,))
        row = c.fetchone()
        if row is None:
            raise KeyError
        return pickle.loads(row[0])  # type: ignore[no-any-return]

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
