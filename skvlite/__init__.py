import os
import pickle
import sqlite3
import tempfile
from typing import Any, Generator, Mapping, Optional, Tuple, TypeVar, cast

import sqlite_zstd
from pytools.persistent_dict import PersistentDict

K = TypeVar("K")
V = TypeVar("V")

class KeyBuilder:
    def __call__(self, key: Any) -> str:
        return str(key)
    
class NoSuchEntryError(KeyError):
    """Raised when an entry is not found in a :class:`PersistentDict`."""
    pass

class NoSuchEntryCollisionError(NoSuchEntryError):
    """Raised when an entry is not found in a :class:`PersistentDict`, but it
    contains an entry with the same hash key (hash collision)."""
    pass
class ReadOnlyEntryError(KeyError):
    """Raised when an attempt is made to overwrite an entry in a
    :class:`WriteOnceKVStore`."""
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
                from typing import cast
                # platformdirs does not handle XDG_CACHE_HOME on macOS
                # https://github.com/platformdirs/platformdirs/issues/269
                container_dir = join(
                    cast(str, os.getenv("XDG_CACHE_HOME")), "pytools")
            else:
                container_dir = platformdirs.user_cache_dir("pytools", "pytools")

        os.makedirs(container_dir, exist_ok=True)
        self.filename = join(container_dir, filename + ".sqlite")
        #self.key_builder = lambda x: str(x)
        self.key_builder = KeyBuilder()
        # isolation_level=None: enable autocommit mode
        # https://www.sqlite.org/lang_transaction.html#implicit_versus_explicit_transactions

        # Connect to SQLite database and enable extension loading
        self.conn = sqlite3.connect(self.filename, isolation_level=None)
        self.conn.enable_load_extension(True)

        # Load sqlite-zstd extension
        sqlite_zstd.load(self.conn)

        # Create dictionary table if not exists
        self._exec_sql(
            "CREATE TABLE IF NOT EXISTS dict "
            "(keyhash TEXT NOT NULL PRIMARY KEY, key_value TEXT NOT NULL)"
        )

        if enable_wal:
            self._exec_sql("PRAGMA journal_mode = 'WAL'")

        self._exec_sql("PRAGMA temp_store = 'MEMORY'")
        self._exec_sql("PRAGMA synchronous = 'NORMAL'")
        self._exec_sql("PRAGMA cache_size = -64000")

        # Perform VACUUM operation after enabling compression
        # uncompressed_size, compressed_size = self.vacuum_and_report_size()
        # print(f"Database sizes - Uncompressed: {uncompressed_size} bytes, Compressed: {compressed_size} bytes")

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
            raise Exception(
                f"Key collision in cache at '{self.filename}'"
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
        keyhash = self.key_builder(key)

        while True:
            try:
                self.conn.execute("BEGIN EXCLUSIVE TRANSACTION")

                try:
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
                if (hasattr(e, "sqlite_errorcode")
                        and not e.sqlite_errorcode == sqlite3.SQLITE_BUSY):
                    raise
            else:
                break

    def __delitem__(self, key: K) -> None:
        self.remove(key)

    def __len__(self) -> int:
        return cast(int, next(self._exec_sql("SELECT COUNT(*) FROM dict"))[0])

    def __iter__(self) -> Generator[K, None, None]:
        return self.keys()

    def keys(self) -> Generator[K, None, None]:
        for row in self._exec_sql("SELECT key_value FROM dict ORDER BY rowid"):
            yield pickle.loads(row[0])[0]

    def values(self) -> Generator[V, None, None]:
        for row in self._exec_sql("SELECT key_value FROM dict ORDER BY rowid"):
            yield pickle.loads(row[0])[1]

    def items(self) -> Generator[Tuple[K, V], None, None]:
        for row in self._exec_sql("SELECT key_value FROM dict ORDER BY rowid"):
            yield pickle.loads(row[0])

    def nbytes(self) -> int:
        return cast(int,
                    next(self._exec_sql("SELECT page_size * page_count FROM "
                                        "pragma_page_size(), pragma_page_count()")
                         )[0]
                    )

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.filename}, nitems={len(self)})"

    def clear(self) -> None:
        self._exec_sql("DELETE FROM dict")

    def store_if_not_present(self, key: Any, value: Any) -> None:
        self.store(key, value, _skip_if_present=True)

    def vacuum_and_report_size(self) -> Tuple[int, int]:
        """Perform VACUUM operation and return size before and after."""
        uncompressed_size = os.path.getsize(self.filename)
        self._exec_sql("VACUUM")
        self.conn.commit()
        compressed_size = os.path.getsize(self.filename)
        return uncompressed_size, compressed_size

    def close(self) -> None:
        self.conn.close()

    def enable_zstd_compression(self, table: str, column: str, compression_level: int = 19, dict_chooser: str = "''a''") -> None:
        """Enable zstd compression for a specific table and column."""
        # Create the table if not exists
        self._exec_sql(
            f"CREATE TABLE IF NOT EXISTS {table} (id INTEGER PRIMARY KEY AUTOINCREMENT, {column} TEXT)"
        )

        compression_config = f'{{"table": "{table}", "column": "{column}", "compression_level": {compression_level}, "dict_chooser": "{dict_chooser}"}}'
        self.conn.execute(f"SELECT zstd_enable_transparent('{compression_config}')")
        self.conn.execute('SELECT zstd_incremental_maintenance(null, 1)')


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
                raise ReadOnlyEntryError("WriteOnceKVStore, tried overwriting key")

    def _fetch(self, keyhash: str) -> Tuple[K, V]:
        c = self._exec_sql("SELECT key_value FROM dict WHERE keyhash=?",
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

