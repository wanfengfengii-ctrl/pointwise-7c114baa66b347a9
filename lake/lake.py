from .schema import Schema
from .catalog_store import CatalogStore
from .payload_port import PayloadPort
from .segments import SegmentStore
from .snapshots import SnapshotRepository
from .receipts import ReceiptRepository
from .commits import CommitPublisher
from .admission import AdmissionService
from .scans import SnapshotScanner
from .bindings import BindingRepository
from .artifacts import ArtifactRepository
from .accounting import AccountingService
from .compaction_plans import CompactionPlanner
from .compaction_publish import CompactionPublisher
from .rewrite import CompactionService


class Lake:
    def __init__(self, path, columns=("station", "a", "b")):
        self.schema = Schema(columns)
        self.catalog = CatalogStore(path, self.schema)
        try:
            self.payload_port = PayloadPort()
            self.segments = SegmentStore(self.catalog, self.schema, self.payload_port)
            self.snapshots = SnapshotRepository(self.catalog)
            receipts = ReceiptRepository(self.catalog)
            publisher = CommitPublisher(self.catalog, receipts, self.snapshots, self.segments)
            self.admission = AdmissionService(self.schema, self.segments, publisher)
            self.scanner = SnapshotScanner(self.schema, self.snapshots, self.segments)
            self.bindings = BindingRepository(self.catalog, self.snapshots)
            self.artifacts = ArtifactRepository(self.catalog, self.bindings)
            self.accounting = AccountingService(self.schema, self.bindings,
                                                self.scanner, self.artifacts)
            self.planner = CompactionPlanner(self.catalog, self.snapshots)
            maintenance = CompactionPublisher(self.catalog, self.snapshots,
                                               self.planner, self.segments)
            self.compaction = CompactionService(self.planner, self.segments, maintenance)
        except BaseException:
            self.catalog.close()
            raise

    def ingest(self, batch_id, operations):
        return self.admission.ingest(batch_id, operations)

    def snapshot(self):
        return self.snapshots.current().to_descriptor()

    def get_snapshot(self, snapshot_id):
        return self.snapshots.get(snapshot_id).to_descriptor()

    def read(self, snapshot_id, columns):
        return self.scanner.read(snapshot_id, columns)

    def run_totals(self, snapshot_id, filter_spec, group_column, value_column):
        return self.accounting.run_totals(snapshot_id, filter_spec, group_column, value_column)

    def read_artifact(self, artifact_id):
        return self.artifacts.read(artifact_id)

    def compact(self, files):
        return self.compaction.compact(files)

    def close(self):
        self.catalog.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()
