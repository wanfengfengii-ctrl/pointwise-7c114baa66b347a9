import json
from dataclasses import dataclass
from .integrity import digest_bytes
from .schema import Schema


@dataclass(frozen=True)
class PreparedBatch:
    batch_id: str
    canonical: str
    digest: str

    @property
    def operations(self):
        operations = json.loads(self.canonical)
        for operation in operations:
            if operation["op"] in ("put", "patch"):
                operation["values"] = {
                    column: Schema.decode_value(value)
                    for column, value in operation["values"].items()}
        return operations


def batch_digest(canonical):
    return digest_bytes(canonical.encode("utf-8"))


def normalize_batch(schema, batch_id, operations):
    schema.validate_id(batch_id)
    try:
        iterator = iter(operations)
    except TypeError as exc:
        raise ValueError("operations must be iterable") from exc
    result = []
    seen = set()
    for operation in iterator:
        if not isinstance(operation, dict):
            raise ValueError("operation must be a mapping")
        kind = operation.get("op")
        expected = {"op", "id", "values"} if kind == "put" else {"op", "id"}
        if kind not in ("put", "delete") or set(operation) != expected:
            raise ValueError("invalid complete-row operation")
        key = schema.validate_id(operation["id"])
        if key in seen:
            raise ValueError("duplicate ID in batch")
        seen.add(key)
        item = {"op": kind, "id": key}
        if kind == "put":
            full = schema.normalize_full_row(operation["values"])
            item["values"] = {c: schema.encode_value(full[c]) for c in schema.columns}
        result.append(item)
    if not result:
        raise ValueError("empty batch")
    result.sort(key=lambda item: item["id"])
    canonical = json.dumps(result, ensure_ascii=True, separators=(",", ":"))
    return PreparedBatch(batch_id, canonical, batch_digest(canonical))
