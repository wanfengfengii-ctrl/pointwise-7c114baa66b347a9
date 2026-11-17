import json
from dataclasses import dataclass


@dataclass(frozen=True)
class MaintenanceReceipt:
    plan_id: str
    input_snapshot_id: int
    snapshot_id: int
    file_id: str
    selected: tuple

    def to_descriptor(self):
        return {"plan_id": self.plan_id, "input_snapshot_id": self.input_snapshot_id,
                "snapshot_id": self.snapshot_id, "file_id": self.file_id,
                "selected": list(self.selected)}


class CompactionPublisher:
    def __init__(self, catalog, snapshots, planner, segments):
        self.catalog = catalog
        self.snapshots = snapshots
        self.planner = planner
        self.segments = segments

    def publish(self, plan_id):
        with self.catalog.transaction("compact"):
            plan = self.planner.get(plan_id)
            if plan.status != "VERIFIED" or plan.candidate is None:
                raise ValueError("compaction is not verified")
            current = self.snapshots.current()
            if current.id != plan.snapshot_id:
                raise ValueError("compaction input layout changed")
            selected = set(plan.files)
            if not selected <= set(current.files):
                raise ValueError("selected members changed")
            self.segments.register(plan.candidate)
            members = tuple(f for f in current.files if f not in selected)
            snapshot = self.snapshots.create(members + (plan.candidate.id,))
            receipt = MaintenanceReceipt(plan.id, plan.snapshot_id, snapshot.id,
                                         plan.candidate.id, plan.files)
            row = self.catalog.fetchone("SELECT payload FROM plans WHERE id=?", (plan.id,))
            payload = json.loads(row[0])
            payload.update(status="PUBLISHED", receipt=receipt.to_descriptor())
            self.catalog.execute("UPDATE plans SET payload=? WHERE id=?",
                                 (json.dumps(payload), plan.id))
            self.snapshots.set_current(snapshot.id)
        return receipt.to_descriptor()
