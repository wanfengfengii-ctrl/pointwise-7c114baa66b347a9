import json
from pathlib import Path
from tempfile import TemporaryDirectory
from lake import Lake
from .reference import LakeCase, put, day_sample, replay, totals


class TestTotals(LakeCase):
    def test_null_integer_totals(self):
        operations = [put("x", 2), put("y", None), put("z", -2),
                      put("empty", None, station=2), put("null-group", -5, station=None)]
        receipt = self.lake.ingest("readings", operations)
        expected = [{"group": None, "rows": 1, "non_null": 1, "sum": -5},
                    {"group": 1, "rows": 3, "non_null": 2, "sum": 0},
                    {"group": 2, "rows": 1, "non_null": 0, "sum": None}]
        actual = self.groups(receipt["snapshot_id"])
        self.assertEqual(actual, expected)
        self.assertEqual(self.groups(receipt["snapshot_id"]), expected)
        big = 10 ** 80
        last = self.lake.ingest("large", [put("huge", big, station=-3)])
        self.assertEqual(self.groups(last["snapshot_id"])[1],
                         {"group": -3, "rows": 1, "non_null": 1, "sum": big})
        self.assertEqual(self.groups(last["snapshot_id"],
                                    {"op": "eq", "column": "station", "value": 900}), [])

    def test_integer_roundtrip(self):
        huge = 1 << 20000
        receipt = self.lake.ingest("arbitrary-int", [put("x", huge, station=huge)])
        self.assertEqual(self.lake.read(receipt["snapshot_id"], ["a"]),
                         [{"id": "x", "values": {"a": huge}}])
        spec = {"op": "range", "column": "a", "min": huge, "max": huge}
        artifact = self.lake.run_totals(receipt["snapshot_id"], spec, "station", "a")
        self.reopen()
        saved = self.lake.read_artifact(artifact)
        self.assertEqual(saved["source"]["filter"], spec)
        self.assertEqual(saved["groups"],
                         [{"group": huge, "rows": 1, "non_null": 1, "sum": huge}])
        self.assertEqual(self.lake.ingest("arbitrary-int", [put("x", huge, station=huge)]),
                         receipt)

    def test_filter_boundaries(self):
        operations = [put("low", -2), put("middle", 0), put("high", 2),
                      put("outside", 3), put("missing", None)]
        receipt = self.lake.ingest("range", operations)
        rows = replay([operations])
        specifications = [
            {"op": "range", "column": "a", "min": -2, "max": 2},
            {"op": "range", "column": "a", "min": 2},
            {"op": "range", "column": "a", "max": 0},
            {"op": "eq", "column": "a", "value": None},
            {"op": "range", "column": "a", "min": 2, "max": 2},
        ]
        for spec in specifications:
            with self.subTest(spec=spec):
                self.assertEqual(self.groups(receipt["snapshot_id"], spec), totals(rows, spec))
        self.assertEqual(self.groups(receipt["snapshot_id"], specifications[0]),
                         [{"group": 1, "rows": 3, "non_null": 3, "sum": 0}])
        file_id = receipt["file_id"]
        self.lake.payload_port.set_available(file_id, "b", False)
        self.lake.payload_port.clear_access_log()
        self.groups(receipt["snapshot_id"], specifications[0], group="a", value="a")
        self.assertEqual(self.lake.payload_port.access_log, [(file_id, "a")])

    def test_invalid_spec(self):
        receipt = self.lake.ingest("base", [put("x", 1)])
        invalid = [
            {"op": "range", "column": "a", "min": True},
            {"op": "range", "column": "a", "min": 2, "max": 1},
            {"op": "range", "column": "a"},
            {"op": "eq", "column": "a", "value": False},
            {"op": "eq", "column": "missing", "value": 1},
            {"op": "eq", "column": "a", "value": 1, "extra": 0},
            {"op": "or", "column": "a"},
        ]
        before = self.state()
        for spec in invalid:
            with self.assertRaises(ValueError):
                self.groups(receipt["snapshot_id"], spec)
            self.assertEqual(self.state(), before)
        with self.assertRaises(ValueError):
            self.groups(receipt["snapshot_id"], group="missing")
        with self.assertRaises(KeyError):
            self.groups(9999)
        self.assertEqual(self.state(), before)

    def test_day_workflow(self):
        run_day(self.path / "day")


def run_day(path):
    sample = day_sample()
    history = [batch["operations"] for batch in sample["batches"]]
    with Lake(path, sample["columns"]) as lake:
        receipts = [lake.ingest(batch["id"], batch["operations"])
                    for batch in sample["batches"]]
        assert lake.ingest(sample["batches"][0]["id"], history[0]) == receipts[0]
        before = lake.snapshot()
        for batch_id, operations in [
                ("bad-missing", [{"op": "put", "id": "bad", "values": {"a": 1}}]),
                (sample["batches"][0]["id"], [put("different", 100)])]:
            try:
                lake.ingest(batch_id, operations)
            except ValueError:
                pass
            else:
                raise AssertionError("invalid export accepted")
        assert lake.snapshot() == before
        rows = lake.read(before["id"], sample["columns"])
        assert rows == replay(history)
        artifact = lake.run_totals(before["id"], None, "station", "a")
        saved = lake.read_artifact(artifact)
        assert saved["groups"] == sample["expected"]
        assert saved["groups"] == totals(replay(history))
        spec = {"op": "eq", "column": "station", "value": 1}
        station_artifact = lake.run_totals(before["id"], spec, "station", "a")
        station_actual = lake.read_artifact(station_artifact)["groups"]
        assert station_actual == [sample["expected"][1]]
        maintenance = lake.compact([receipts[0]["file_id"], receipts[2]["file_id"]])
        after = lake.snapshot()
        assert set(before["files"]) - set(maintenance["selected"]) <= set(after["files"])
        assert lake.read(after["id"], sample["columns"]) == rows
        downstream_id = lake.run_totals(after["id"], None, "station", "a")
        downstream = lake.read_artifact(downstream_id)["groups"]
        assert downstream == sample["expected"]
        assert lake.read(before["id"], sample["columns"]) == rows
        assert lake.read_artifact(artifact) == saved
    with Lake(path, sample["columns"]) as lake:
        assert lake.read(before["id"], sample["columns"]) == rows
        assert lake.read_artifact(artifact) == saved
        assert lake.read_artifact(downstream_id)["groups"] == downstream
        assert lake.read_artifact(station_artifact)["groups"] == station_actual
        actual = lake.read_artifact(artifact)["groups"]
        evidence = {"case_id": "v7.logical_equivalence",
                    "input": sample["batches"], "expected": sample["expected"],
                    "actual": actual, "downstream_expected": sample["expected"],
                    "downstream_actual": downstream,
                    "code_paths": ["lake/admission.py", "lake/commits.py", "lake/scans.py",
                                   "lake/accounting.py", "lake/group_totals.py",
                                   "lake/artifacts.py", "lake/compaction_merge.py",
                                   "lake/compaction_checks.py", "lake/compaction_publish.py"],
                    "oracle": "sample hand totals and independent sequential dictionary replay"}
        print("POINTWISE_SCENARIO " + json.dumps(evidence, ensure_ascii=False,
                                                 separators=(",", ":")))


if __name__ == "__main__":
    with TemporaryDirectory(prefix="monitoring-day-") as temporary:
        run_day(Path(temporary))
