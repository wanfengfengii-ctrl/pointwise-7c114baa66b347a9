import copy
import json
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from lake import Lake


COLUMNS = ("station", "a", "b")


def put(key, a, b=None, station=1):
    return {"op": "put", "id": key,
            "values": {"station": station, "a": a, "b": b}}


def delete(key):
    return {"op": "delete", "id": key}


def replay(batches, columns=COLUMNS):
    state = {}
    for batch in batches:
        for operation in batch:
            if operation["op"] == "delete":
                state.pop(operation["id"], None)
            else:
                state[operation["id"]] = copy.deepcopy(operation["values"])
    return [{"id": key, "values": {c: state[key][c] for c in columns}}
            for key in sorted(state)]


def totals(rows, filter_spec=None, group="station", value="a"):
    selected = []
    for row in rows:
        values = row["values"]
        if filter_spec is not None:
            item = values[filter_spec["column"]]
            if filter_spec["op"] == "eq":
                if item != filter_spec["value"]:
                    continue
            else:
                if item is None:
                    continue
                lower, upper = filter_spec.get("min"), filter_spec.get("max")
                if lower is not None and item < lower:
                    continue
                if upper is not None and item > upper:
                    continue
        selected.append(values)
    keys = set(row[group] for row in selected)
    result = []
    for key in sorted(keys, key=lambda item: (item is not None, item or 0)):
        members = [row for row in selected if row[group] == key]
        readings = [row[value] for row in members if row[value] is not None]
        result.append({"group": key, "rows": len(members),
                       "non_null": len(readings),
                       "sum": sum(readings) if readings else None})
    return result


def day_sample():
    path = Path(__file__).resolve().parents[1] / "samples" / "day.json"
    return json.loads(path.read_text(encoding="utf-8"))


@contextmanager
def fail_before_commit(catalog, label):
    previous = catalog.before_commit

    def fail(actual):
        if actual == label:
            raise RuntimeError("ordinary publication failure")
        if previous is not None:
            previous(actual)

    catalog.before_commit = fail
    try:
        yield
    finally:
        catalog.before_commit = previous


class LakeCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="monitoring-lake-")
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)
        self.lake = Lake(self.path)
        self.addCleanup(lambda: self.lake.close())

    def reopen(self):
        self.lake.close()
        self.lake = Lake(self.path)

    def state(self):
        result = {"snapshot": self.lake.snapshot()}
        for table in ("commits", "receipts", "segments", "snapshots", "bindings", "artifacts"):
            result[table] = self.lake.catalog.fetchall(f"SELECT * FROM {table} ORDER BY 1")
        return result

    def groups(self, snapshot_id, spec=None, group="station", value="a"):
        artifact_id = self.lake.run_totals(snapshot_id, spec, group, value)
        return self.lake.read_artifact(artifact_id)["groups"]
