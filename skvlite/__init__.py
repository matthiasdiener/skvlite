import os
import pickle
import sqlite3
import sqlite_zstd
from typing import Any, Generator, Mapping, Optional, Tuple, TypeVar, cast

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
            import sys
            import platformdirs

            if sys.platform == "darwin" and os.getenv("XDG_CACHE_HOME") is not None:
                container_dir = join(
                    cast(str, os.getenv("XDG_CACHE_HOME")), "pytools")
            else:
                container_dir = platformdirs.user_cache_dir("pytools", "pytools")

        os.makedirs(container_dir, exist_ok=True)
        self.filename = join(container_dir, filename + ".sqlite")

        self.key_builder = KeyBuilder()

        # isolation_level=None: enable autocommit mode
        self.conn = sqlite3.connect(self.filename, isolation_level=None)

        self._exec_sql(
            "CREATE TABLE IF NOT EXISTS dict "
            "(keyhash TEXT NOT NULL PRIMARY KEY, key_value BLOB NOT NULL)"
        )

        if enable_wal:
            self._exec_sql("PRAGMA journal_mode = 'WAL'")

        self._exec_sql("PRAGMA temp_store = 'MEMORY'")
        self._exec_sql("PRAGMA synchronous = 'NORMAL'")
        self._exec_sql("PRAGMA cache_size = -64000")

        # Load zstd extension for SQLite
        self.conn.enable_load_extension(True)
        self._exec_sql("PRAGMA trusted_schema = OFF;")
        sqlite_zstd.load(self.conn)
        print("Initialized zstd extension.")

    def _exec_sql(self, *args: Any) -> Any:
        while True:
            try:
                return self.conn.execute(*args)
            except sqlite3.OperationalError as e:
                if hasattr(e, "sqlite_errorcode") and not e.sqlite_errorcode == sqlite3.SQLITE_BUSY:
                    raise

    def _collision_check(self, key: K, stored_key: K) -> None:
        if stored_key != key:
            raise Exception(
                f"Key collision in cache at '{self.filename}' -- these are sufficiently unlikely "
                "that they're often indicative of a broken hash key implementation "
                "(that is not considering some elements relevant for equality comparison)"
            )

    def _store_data(self, key: K, value: V, replace: bool) -> None:
        keyhash = self.key_builder(key)
        pickled_data = pickle.dumps((key, value))
        compressed_data = self._exec_sql("SELECT zstd_compress(?)", (pickled_data,)).fetchone()[0]

        mode = "REPLACE" if replace else "IGNORE"

        self._exec_sql(
            f"INSERT OR {mode} INTO dict VALUES (?, ?)", (keyhash, compressed_data)
        )

        # Vacuum after every store to optimize database space
        self.vacuum()

    def _load_data(self, keyhash: str) -> Any:
        c = self._exec_sql("SELECT key_value FROM dict WHERE keyhash=?", (keyhash,))
        row = c.fetchone()
        if row is None:
            raise NoSuchEntryError(keyhash)
        compressed_data = row[0]
        pickled_data = self._exec_sql("SELECT zstd_decompress(?)", (compressed_data,)).fetchone()[0]
        return pickle.loads(pickled_data)

    def store(self, key: K, value: V, _skip_if_present: bool = False) -> None:
        self._store_data(key, value, not _skip_if_present)

    def fetch(self, key: K) -> V:
        keyhash = self.key_builder(key)
        stored_key, value = self._load_data(keyhash)
        self._collision_check(key, stored_key)

        return cast(V, value)

    def __setitem__(self, key: K, value: V) -> None:
        self.store(key, value)

    def __getitem__(self, key: K) -> V:
        return self.fetch(key)

    def remove(self, key: K) -> None:
        keyhash = self.key_builder(key)

        while True:
            try:
                self.conn.execute("BEGIN EXCLUSIVE TRANSACTION")

                try:
                    c = self.conn.execute("SELECT key_value FROM dict WHERE keyhash=?", (keyhash,))
                    row = c.fetchone()
                    if row is None:
                        raise NoSuchEntryError(key)

                    compressed_data = row[0]
                    pickled_data = self._exec_sql("SELECT zstd_decompress(?)", (compressed_data,)).fetchone()[0]
                    stored_key, _value = pickle.loads(pickled_data)
                    self._collision_check(key, stored_key)

                    self.conn.execute("DELETE FROM dict WHERE keyhash=?", (keyhash,))
                    self.conn.execute("COMMIT")
                except Exception as e:
                    self.conn.execute("ROLLBACK")
                    raise e
            except sqlite3.OperationalError as e:
                if hasattr(e, "sqlite_errorcode") and not e.sqlite_errorcode == sqlite3.SQLITE_BUSY:
                    raise
            else:
                break

    def __delitem__(self, key: K) -> None:
        self.remove(key)

    def __len__(self) -> int:
        return cast(int, next(self._exec_sql("SELECT COUNT(*) FROM dict"))[0])

    def __iter__(self) -> Generator[K, None, None]:
        return self.keys()

    def keys(self) -> Generator[K, None, None]:  # type: ignore[override]
        for row in self._exec_sql("SELECT key_value FROM dict ORDER BY rowid"):
            pickled_data = self._exec_sql("SELECT zstd_decompress(?)", (row[0],)).fetchone()[0]
            yield pickle.loads(pickled_data)[0]

    def values(self) -> Generator[V, None, None]:  # type: ignore[override]
        for row in self._exec_sql("SELECT key_value FROM dict ORDER BY rowid"):
            pickled_data = self._exec_sql("SELECT zstd_decompress(?)", (row[0],)).fetchone()[0]
            yield pickle.loads(pickled_data)[1]

    def items(self) -> Generator[Tuple[K, V], None, None]:  # type: ignore[override]
        for row in self._exec_sql("SELECT key_value FROM dict ORDER BY rowid"):
            pickled_data = self._exec_sql("SELECT zstd_decompress(?)", (row[0],)).fetchone()[0]
            yield pickle.loads(pickled_data)

    def nbytes(self) -> int:
        return cast(int, next(self._exec_sql("SELECT page_size * page_count FROM "
                                             "pragma_page_size(), pragma_page_count()"))[0])

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.filename}, nitems={len(self)})"

    def clear(self) -> None:
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
        if not _skip_if_present and key in self:
            raise ReadOnlyEntryError(key)
        super().store(key, value, _skip_if_present)

    def __setitem__(self, key: K, value: V) -> None:
        self.store(key, value)

    def __delitem__(self, key: K) -> None:
        raise ReadOnlyEntryError(key)
