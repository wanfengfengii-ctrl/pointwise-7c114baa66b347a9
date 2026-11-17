import json
from dataclasses import dataclass


@dataclass(frozen=True)
class Snapshot:
    id: int
    files: tuple

    def to_descriptor(self):
        return {"id": self.id, "files": list(self.files)}


class SnapshotRepository:
    def __init__(self, catalog):
        self.catalog = catalog

    def current(self):
        row = self.catalog.fetchone("SELECT value FROM meta WHERE key='current'")
        return self.get(int(row[0]))

    def get(self, snapshot_id):
        if type(snapshot_id) is not int:
            raise KeyError(snapshot_id)
        row = self.catalog.fetchone("SELECT members FROM snapshots WHERE id=?",
                                   (snapshot_id,))
        if row is None:
            raise KeyError(snapshot_id)
        return Snapshot(snapshot_id, tuple(json.loads(row[0])))

    def create(self, files):
        if not self.catalog.connection.in_transaction:
            raise RuntimeError("snapshot creation requires transaction")
        files = tuple(files)
        if len(set(files)) != len(files):
            raise ValueError("duplicate snapshot member")
        for file_id in files:
            if self.catalog.fetchone("SELECT id FROM segments WHERE id=?", (file_id,)) is None:
                raise KeyError(file_id)
        cursor = self.catalog.execute("INSERT INTO snapshots(members) VALUES(?)",
                                      (json.dumps(files),))
        return Snapshot(cursor.lastrowid, files)

    def set_current(self, snapshot_id):
        if not self.catalog.connection.in_transaction:
            raise RuntimeError("current pointer requires transaction")
        self.get(snapshot_id)
        self.catalog.execute("UPDATE meta SET value=? WHERE key='current'",
                             (str(snapshot_id),))
