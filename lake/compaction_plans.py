import json
import uuid
from dataclasses import dataclass
from .segments import SegmentDescriptor


@dataclass(frozen=True)
class CompactionPlan:
    id: str
    snapshot_id: int
    files: tuple
    status: str
    candidate: object = None
    error: object = None
    receipt: object = None


class CompactionPlanner:
    def __init__(self, catalog, snapshots):
        self.catalog = catalog
        self.snapshots = snapshots

    def prepare(self, files):
        if not isinstance(files, (list, tuple)):
            raise ValueError("files must be a list or tuple")
        if not files or any(not isinstance(f, str) or not f for f in files):
            raise ValueError("nonempty file selection required")
        if len(set(files)) != len(files):
            raise ValueError("duplicate selected file")
        with self.catalog.transaction("plan"):
            snapshot = self.snapshots.current()
            if not set(files) <= set(snapshot.files):
                raise ValueError("selection is not in current snapshot")
            selected = [f for f in snapshot.files if f in set(files)]
            plan_id = uuid.uuid4().hex
            payload = {"id": plan_id, "snapshot_id": snapshot.id,
                       "files": selected, "status": "PLANNED",
                       "candidate": None, "error": None, "receipt": None}
            self.catalog.execute("INSERT INTO plans VALUES(?,?)",
                                 (plan_id, json.dumps(payload)))
        return self.get(plan_id)

    def get(self, plan_id):
        row = self.catalog.fetchone("SELECT payload FROM plans WHERE id=?", (plan_id,))
        if row is None:
            raise KeyError(plan_id)
        value = json.loads(row[0])
        value["files"] = tuple(value["files"])
        if value["candidate"] is not None:
            value["candidate"]["page_hashes"] = tuple(value["candidate"]["page_hashes"])
            value["candidate"] = SegmentDescriptor(**value["candidate"])
        return CompactionPlan(**value)

    def _transition(self, plan_id, expected, status, **changes):
        with self.catalog.transaction("plan_state"):
            row = self.catalog.fetchone("SELECT payload FROM plans WHERE id=?", (plan_id,))
            if row is None:
                raise KeyError(plan_id)
            value = json.loads(row[0])
            if value["status"] not in expected:
                raise ValueError("invalid compaction transition")
            value.update(changes, status=status)
            self.catalog.execute("UPDATE plans SET payload=? WHERE id=?",
                                 (json.dumps(value), plan_id))
        return self.get(plan_id)

    def mark_candidate(self, plan_id, candidate):
        return self._transition(plan_id, {"PLANNED"}, "CANDIDATE",
                                candidate=candidate.to_descriptor())

    def mark_verified(self, plan_id):
        return self._transition(plan_id, {"CANDIDATE"}, "VERIFIED")

    def mark_failed(self, plan_id, error):
        return self._transition(plan_id, {"PLANNED", "CANDIDATE", "VERIFIED"},
                                "FAILED", error=str(error))
