import json
import sqlite3
from pathlib import Path
from contextlib import contextmanager


class CatalogStore:
    def __init__(self, root, schema):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.before_commit = None
        self.connection = sqlite3.connect(self.root / "catalog.sqlite",
                                          isolation_level=None)
        self.connection.execute("PRAGMA foreign_keys=ON")
        try:
            self.initialize(schema)
        except BaseException:
            self.close()
            raise

    def initialize(self, schema):
        statements = (
            "CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS segments(id TEXT PRIMARY KEY, descriptor TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS snapshots(id INTEGER PRIMARY KEY AUTOINCREMENT, members TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS commits(seq INTEGER PRIMARY KEY, batch_id TEXT UNIQUE NOT NULL, file_id TEXT NOT NULL REFERENCES segments(id))",
            "CREATE TABLE IF NOT EXISTS receipts(batch_id TEXT PRIMARY KEY, digest TEXT NOT NULL, canonical TEXT NOT NULL, receipt TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS bindings(id TEXT PRIMARY KEY, source TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS artifacts(id TEXT PRIMARY KEY, binding_id TEXT UNIQUE NOT NULL REFERENCES bindings(id), payload TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS plans(id TEXT PRIMARY KEY, payload TEXT NOT NULL)",
        )
        with self.transaction("initialize"):
            for statement in statements:
                self.execute(statement)
            descriptor = json.dumps(schema.to_descriptor(), separators=(",", ":"))
            existing = self.fetchone("SELECT value FROM meta WHERE key='schema'")
            if existing is not None:
                if existing[0] != descriptor:
                    raise ValueError("incompatible fixed schema")
                return
            self.execute("INSERT INTO meta VALUES('schema',?)", (descriptor,))
            cursor = self.execute("INSERT INTO snapshots(members) VALUES('[]')")
            self.execute("INSERT INTO meta VALUES('current',?)",
                         (str(cursor.lastrowid),))

    @contextmanager
    def transaction(self, label):
        if self.connection.in_transaction:
            raise RuntimeError("nested catalog transaction")
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield
            if self.before_commit is not None:
                self.before_commit(label)
            self.connection.execute("COMMIT")
        except BaseException:
            if self.connection.in_transaction:
                self.connection.execute("ROLLBACK")
            raise

    def execute(self, statement, parameters=()):
        return self.connection.execute(statement, parameters)

    def fetchone(self, statement, parameters=()):
        return self.execute(statement, parameters).fetchone()

    def fetchall(self, statement, parameters=()):
        return self.execute(statement, parameters).fetchall()

    def close(self):
        self.connection.close()
