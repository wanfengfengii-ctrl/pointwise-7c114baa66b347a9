from .batches import normalize_batch


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
