import json
import uuid
from dataclasses import dataclass
from .schema import Schema


@dataclass(frozen=True)
class InputBinding:
    id: str
    canonical_source: str

    def to_descriptor(self):
        source = json.loads(self.canonical_source)
        if source["filter"] is not None:
            for name in ("value", "min", "max"):
                if name in source["filter"]:
                    source["filter"][name] = Schema.decode_value(source["filter"][name])
        return source


class BindingRepository:
    def __init__(self, catalog, snapshots):
        self.catalog = catalog
        self.snapshots = snapshots

    def prepare(self, snapshot_id, filter_spec, group_column, value_column):
        self.snapshots.get(snapshot_id)
        source = {"snapshot_id": snapshot_id,
                  "filter": filter_spec.to_descriptor(),
                  "group_column": group_column, "value_column": value_column}
        if source["filter"] is not None:
            for name in ("value", "min", "max"):
                if name in source["filter"]:
                    source["filter"][name] = Schema.encode_value(source["filter"][name])
        return InputBinding(uuid.uuid4().hex,
                            json.dumps(source, sort_keys=True, separators=(",", ":")))

    def save(self, binding):
        if not self.catalog.connection.in_transaction:
            raise RuntimeError("binding requires artifact transaction")
        self.catalog.execute("INSERT INTO bindings VALUES(?,?)",
                             (binding.id, binding.canonical_source))

    def get(self, binding_id):
        row = self.catalog.fetchone("SELECT source FROM bindings WHERE id=?",
                                   (binding_id,))
        if row is None:
            raise KeyError(binding_id)
        return InputBinding(binding_id, row[0])
