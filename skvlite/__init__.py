import pickle
import sqlite3
from typing import Any, Generator, Mapping, Optional, Tuple, TypeVar, cast
import zlib
from pytools.persistent_dict import KeyBuilder

K = TypeVar("K")
V = TypeVar("V")

class KVStore(Mapping[K, V]):
    def __init__(self, filename: str, container_dir: Optional[str] = None, enable_wal: bool = True) -> None:
        if container_dir is None:
            import sys
            import platformdirs

            if sys.platform == "darwin" and os.getenv("XDG_CACHE_HOME") is not None:
                from typing import cast
                container_dir = join(cast(str, os.getenv("XDG_CACHE_HOME")), "pytools")
            else:
                container_dir = platformdirs.user_cache_dir("pytools", "pytools")

        os.makedirs(container_dir, exist_ok=True)
        self.filename = join(container_dir, filename + ".sqlite")

        self.key_builder = KeyBuilder()
        self.conn = sqlite3.connect(self.filename, isolation_level=None)

        self._exec_sql(
            "CREATE TABLE IF NOT EXISTS dict "
            "(keyhash TEXT NOT NULL PRIMARY KEY, key_value BLOB NOT NULL)"
        )

        if enable_wal:
            self._exec_sql("PRAGMA journal_mode = 'WAL'")

    def _exec_sql(self, *args: Any) -> Any:
        while True:
            try:
                return self.conn.execute(*args)
            except sqlite3.OperationalError as e:
                # If the database is busy, retry
                if hasattr(e, "sqlite_errorcode") and not e.sqlite_errorcode == sqlite3.SQLITE_BUSY:
                    raise

    def _collision_check(self, key: K, stored_key: K) -> None:
        if stored_key != key:
            raise KeyError(
                "Stored key collision detected (keys are different but have the same hash, "
                "relevant for equality comparison)"
            )

    def _store_data(self, key: K, value: V, replace: bool) -> None:
        keyhash = self.key_builder(key)
        pickled_data = pickle.dumps((key, value))
        compressed_data = zlib.compress(pickled_data)

        if replace:
            self.conn.execute(
                "INSERT OR REPLACE INTO dict VALUES (?, ?)", (keyhash, compressed_data)
            )
        else:
            self.conn.execute(
                "INSERT OR IGNORE INTO dict VALUES (?, ?)", (keyhash, compressed_data)
            )

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

        return cast(V, value)

    def __getitem__(self, key: K) -> V:
        return self.fetch(key)

    def remove(self, key: K) -> None:
        keyhash = self.key_builder(key)

        while True:
            try:
                # This is split into SELECT/DELETE to allow for a collision check
                c = self.conn.execute(
                    "SELECT key_value FROM dict WHERE keyhash=?", (keyhash,)
                )
                row = c.fetchone()
                if row is None:
                    raise NoSuchEntryError(key)

                compressed_data = row[0]
                pickled_data = zlib.decompress(compressed_data)

                stored_key, _value = pickle.loads(pickled_data)
                self._collision_check(key, stored_key)

                self.conn.execute("DELETE FROM dict WHERE keyhash=?", (keyhash,))
            except sqlite3.OperationalError as e:
                # If the database is busy, retry
                if hasattr(e, "sqlite_errorcode") and not e.sqlite_errorcode == sqlite3.SQLITE_BUSY:
                    raise
            else:
                break

    def __delitem__(self, key: K) -> None:
        self.remove(key)

    def __len__(self) -> int:
        """Return the number of entries in the dictionary."""
        return cast(int, next(self._exec_sql("SELECT COUNT(*) FROM dict"))[0])

    def __iter__(self) -> Generator[K, None, None]:
        return self.keys()

    def keys(self) -> Generator[K, None, None]:  # type: ignore[override]
        """Return an iterator over the keys in the dictionary."""
        for row in self._exec_sql("SELECT key_value FROM dict ORDER BY rowid"):
            pickled_data = zlib.decompress(row[0])
            yield pickle.loads(pickled_data)[0]

    def values(self) -> Generator[V, None, None]:  # type: ignore[override]
        """Return an iterator over the values in the dictionary."""
        for row in self._exec_sql("SELECT key_value FROM dict ORDER BY rowid"):
            pickled_data = zlib.decompress(row[0])
            yield pickle.loads(pickled_data)[1]

    def items(self) -> Generator[Tuple[K, V], None, None]:  # type: ignore[override]
        """Return an iterator over the items in the dictionary."""
        for row in self._exec_sql("SELECT key_value FROM dict ORDER BY rowid"):
            pickled_data = zlib.decompress(row[0])
            yield pickle.loads(pickled_data)

    def nbytes(self) -> int:
        """Return the size of the dictionary in bytes."""
        return cast(
            int,
            next(
                self._exec_sql(
                    "SELECT page_size * page_count FROM pragma_page_size(), pragma_page_count()"
                )
            )[0],
        )

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.filename}, nitems={len(self)})"

    def clear(self) -> None:
        self._exec_sql("DELETE FROM dict")

class WriteOnceKVStore(KVStore[K, V]):
    def store(self, key: K, value: V, _skip_if_present: bool = False) -> None:
        if not _skip_if_present and key in self:
            raise ReadOnlyEntryError(key)
        super().store(key, value, _skip_if_present)

    def __setitem__(self, key: K, value: V) -> None:
        self.store(key, value)

    def __delitem__(self, key: K) -> None:
        raise AttributeError("Write-once KVStore")
