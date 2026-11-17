import json
import uuid
from .integrity import IntegrityError
from .schema import Schema


def encode_artifact(groups):
    encoded = [dict(group, group=Schema.encode_value(group["group"]),
                    sum=Schema.encode_value(group["sum"])) for group in groups]
    return json.dumps({"version": 1, "groups": encoded},
                      ensure_ascii=True, separators=(",", ":"))


def decode_artifact(payload):
    try:
        value = json.loads(payload)
        if value["version"] != 1 or not isinstance(value["groups"], list):
            raise ValueError("invalid artifact format")
        groups = value["groups"]
        for group in groups:
            if set(group) != {"group", "rows", "non_null", "sum"}:
                raise ValueError("invalid group fields")
            group["group"] = Schema.decode_value(group["group"])
            group["sum"] = Schema.decode_value(group["sum"])
            if (type(group["rows"]) is not int or type(group["non_null"]) is not int
                    or not 0 <= group["non_null"] <= group["rows"]
                    or group["rows"] <= 0
                    or (group["sum"] is None) != (group["non_null"] == 0)):
                raise ValueError("invalid counts or NULL total")
        return groups
    except (ValueError, KeyError, TypeError) as exc:
        raise IntegrityError("invalid archived artifact") from exc


class ArtifactRepository:
    def __init__(self, catalog, bindings):
        self.catalog = catalog
        self.bindings = bindings

    def publish(self, binding, groups):
        payload = encode_artifact(groups)
        artifact_id = uuid.uuid4().hex
        with self.catalog.transaction("artifact"):
            self.bindings.save(binding)
            self.catalog.execute("INSERT INTO artifacts VALUES(?,?,?)",
                                 (artifact_id, binding.id, payload))
        return artifact_id

    def read(self, artifact_id):
        row = self.catalog.fetchone(
            "SELECT binding_id,payload FROM artifacts WHERE id=?", (artifact_id,))
        if row is None:
            raise KeyError(artifact_id)
        source = self.bindings.get(row[0]).to_descriptor()
        return {"id": artifact_id, "source": source,
                "groups": decode_artifact(row[1])}
