import json
from dataclasses import dataclass


@dataclass(frozen=True)
class AdmissionReceipt:
    batch_id: str
    commit: int
    snapshot_id: int
    file_id: str

    def to_descriptor(self):
        return {"batch_id": self.batch_id, "commit": self.commit,
                "snapshot_id": self.snapshot_id, "file_id": self.file_id}


class ReceiptRepository:
    def __init__(self, catalog):
        self.catalog = catalog

    def lookup(self, batch):
        row = self.catalog.fetchone(
            "SELECT digest,canonical,receipt FROM receipts WHERE batch_id=?",
            (batch.batch_id,))
        if row is None:
            return None
        if row[0] != batch.digest or row[1] != batch.canonical:
            raise ValueError("batch ID already binds different content")
        return AdmissionReceipt(**json.loads(row[2]))

    def save(self, batch, receipt):
        if not self.catalog.connection.in_transaction:
            raise RuntimeError("receipt requires publication transaction")
        self.catalog.execute("INSERT INTO receipts VALUES(?,?,?,?)",
                             (batch.batch_id, batch.digest, batch.canonical,
                              json.dumps(receipt.to_descriptor())))
