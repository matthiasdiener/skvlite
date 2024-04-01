import os
import pickle
import sqlite3
from os.path import join
from typing import Any


class KVStore:
    def __init__(self, filename: str, container_dir: str = None, **kwargs) -> None:
        os.makedirs(container_dir, exist_ok=True)

        self.filename = join(container_dir, filename + ".sqlite")

        self.container_dir = container_dir

        self.conn = sqlite3.connect(self.filename, isolation_level=None)
        self.conn.execute(
                "CREATE TABLE IF NOT EXISTS dict (key text NOT NULL PRIMARY KEY, value);"
            )

        self.conn.execute("PRAGMA journal_mode = 'WAL';")
        self.conn.execute("PRAGMA temp_store = 2;")
        self.conn.execute("PRAGMA synchronous = 1;")
        self.conn.execute("PRAGMA cache_size = -64000;")

    def __setitem__(self, key: Any, value: Any) -> None:
        k = pickle.dumps(key)
        v = pickle.dumps(value)
        self.conn.execute("INSERT OR REPLACE INTO dict VALUES (?, ?)", (k, v))

    def __getitem__(self, key: Any) -> Any:
        k = pickle.dumps(key)
        c = self.conn.execute("SELECT value FROM dict WHERE key=?", (k,))
        row = c.fetchone()
        if row is None:
            raise KeyError(key)
        return pickle.loads(row[0])

    def __delitem__(self, key: Any) -> None:
        k = pickle.dumps(key)
        self.conn.execute("DELETE FROM dict WHERE key=?", (k,))
        changes = self.conn.execute("SELECT changes()").fetchone()[0]
        if changes == 0:
            raise KeyError(key)

    def __len__(self) -> int:
        return next(self.conn.execute("SELECT COUNT(*) FROM dict"))[0]

    def __iter__(self):
        for row in self.conn.execute("SELECT key FROM dict"):
            yield pickle.loads(row[0])

    def keys(self):
        for row in self.conn.execute("SELECT key FROM dict"):
            yield pickle.loads(row[0])

    def values(self):
        for row in self.conn.execute("SELECT value FROM dict"):
            yield pickle.loads(row[0])

    def items(self):
        c = self.conn.execute("SELECT key, value FROM dict")
        for row in c:
            yield (pickle.loads(row[0]), pickle.loads(row[1]))

    def __repr__(self) -> str:
        return f"{type(self).__name__}(Connection={self.conn!r}, items={len(self)})"

    def store_if_not_present(self, key: Any, value: Any) -> None:
        if key in self:
            return
        self[key] = value

    def clear(self) -> None:
        self.conn.execute("DELETE FROM dict;")

    def vacuum(self) -> None:
        self.conn.execute("VACUUM;")

    def close(self) -> None:
        self.conn.close()


class ReadOnlyKVStore(KVStore):
    def __setitem__(self, key: Any, value: Any) -> None:
        raise AttributeError("Read-only KVStore")

    def __delitem__(self, key: Any) -> None:
        raise AttributeError("Read-only KVStore")


class WriteOnceKVStore(KVStore):
    def __setitem__(self, key: Any, value: Any) -> None:
        k = pickle.dumps(key)
        v = pickle.dumps(value)
        try:
            self.conn.execute("INSERT INTO dict VALUES (?, ?)", (k, v))
        except sqlite3.IntegrityError:
            raise AttributeError("Write-once KVStore, tried overwriting key")

    def __delitem__(self, key: Any) -> None:
        raise AttributeError("Write-once KVStore")
