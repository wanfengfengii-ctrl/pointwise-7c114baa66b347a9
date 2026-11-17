import json
from .batches import PreparedBatch, batch_digest


def normalize_patch(schema, batch_id, changes):
    schema.validate_id(batch_id)
    if not isinstance(changes, dict):
        raise ValueError("patch changes must be a mapping of row IDs to columns")
    if not changes:
        raise ValueError("empty patch batch")
    result = []
    seen = set()
    for row_id, mapping in changes.items():
        key = schema.validate_id(row_id)
        if key in seen:
            raise ValueError("duplicate ID in patch batch")
        seen.add(key)
        if not isinstance(mapping, dict) or not mapping:
            raise ValueError("patch requires a nonempty column mapping")
        unknown = [column for column in mapping if column not in schema.columns]
        if unknown:
            raise ValueError("unknown patch column")
        item = {"op": "patch", "id": key, "values": {}}
        # Canonical identity follows row ID order and the fixed schema column
        # order; dict insertion order never changes batch identity.
        for column in schema.columns:
            if column in mapping:
                item["values"][column] = schema.encode_value(mapping[column])
        result.append(item)
    result.sort(key=lambda item: item["id"])
    canonical = json.dumps(result, ensure_ascii=True, separators=(",", ":"))
    return PreparedBatch(batch_id, canonical, batch_digest(canonical))
