from .receipts import AdmissionReceipt


class CommitPublisher:
    def __init__(self, catalog, receipts, snapshots, segments):
        self.catalog = catalog
        self.receipts = receipts
        self.snapshots = snapshots
        self.segments = segments

    def publish_ingest(self, batch, build):
        with self.catalog.transaction("ingest"):
            replay = self.receipts.lookup(batch)
            if replay is not None:
                return replay
            row = self.catalog.fetchone("SELECT COALESCE(MAX(seq),0)+1 FROM commits")
            sequence = row[0]
            descriptor = build(sequence)
            if any(entry.op == "patch"
                   for entry in self.segments.directory(descriptor)):
                # Sparse batches never read historical payloads; their
                # self-check stays at operation metadata plus digests.
                self.segments.verify_directory(descriptor)
            else:
                self.segments.verify(descriptor)
            self.segments.register(descriptor)
            self.catalog.execute("INSERT INTO commits VALUES(?,?,?)",
                                 (sequence, batch.batch_id, descriptor.id))
            snapshot = self.snapshots.create(
                self.snapshots.current().files + (descriptor.id,))
            receipt = AdmissionReceipt(batch.batch_id, sequence,
                                       snapshot.id, descriptor.id)
            self.receipts.save(batch, receipt)
            self.snapshots.set_current(snapshot.id)
            return receipt
