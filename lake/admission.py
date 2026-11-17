from .batches import normalize_batch
from .patching import normalize_patch


class AdmissionService:
    def __init__(self, schema, segments, publisher):
        self.schema = schema
        self.segments = segments
        self.publisher = publisher

    def ingest(self, batch_id, operations):
        batch = normalize_batch(self.schema, batch_id, operations)

        def build(sequence):
            records = []
            for operation in batch.operations:
                operation["seq"] = sequence
                records.append(operation)
            return self.segments.write(records)

        return self.publisher.publish_ingest(batch, build).to_descriptor()

    def apply_patch(self, batch_id, changes):
        batch = normalize_patch(self.schema, batch_id, changes)

        def build(sequence):
            records = []
            for operation in batch.operations:
                records.append({
                    "id": operation["id"],
                    "seq": sequence,
                    "op": "patch",
                    # Sparse mapping: only these columns are present; every
                    # other column inherits its prior value (or starts NULL).
                    "values": operation["values"],
                })
            return self.segments.write(records)

        return self.publisher.publish_ingest(batch, build).to_descriptor()
