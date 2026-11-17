from lake import Unavailable, IntegrityError
from lake.segments import SegmentStore
from .reference import LakeCase, put, fail_before_commit, COLUMNS


class TestPublication(LakeCase):
    def test_failed_rewrite_retry(self):
        first = self.lake.ingest("first", [put("x", 1, 2)])
        last = self.lake.ingest("last", [put("x", 3, 4), put("y", None)])
        artifact_id = self.lake.run_totals(last["snapshot_id"], None, "station", "a")
        saved = self.lake.read_artifact(artifact_id)
        rows = self.lake.read(last["snapshot_id"], COLUMNS)
        before = self.state()
        files = self.lake.snapshot()["files"]
        with fail_before_commit(self.lake.catalog, "compact"):
            with self.assertRaises(RuntimeError):
                self.lake.compact(files)
        self.assertEqual(self.state(), before)
        failed = self.lake.catalog.fetchall("SELECT id FROM plans")
        self.assertEqual(len(failed), 1)
        plan = self.lake.planner.get(failed[0][0])
        self.assertEqual(plan.status, "FAILED")
        self.assertIsNone(plan.receipt)
        self.assertIsNotNone(plan.candidate)
        self.reopen()
        self.assertEqual(self.state(), before)
        self.assertEqual(self.lake.read_artifact(artifact_id), saved)
        result = self.lake.compact(files)
        self.assertEqual(self.lake.read(result["snapshot_id"], COLUMNS), rows)
        self.assertEqual(self.lake.read(last["snapshot_id"], COLUMNS), rows)
        self.assertEqual(self.lake.read(first["snapshot_id"], ["a"])[0]["values"]["a"], 1)
        self.assertEqual(self.lake.read_artifact(artifact_id), saved)
        self.assertEqual(self.lake.planner.get(result["plan_id"]).status, "PUBLISHED")
        after = self.lake.ingest("after", [put("z", 8)])
        self.assertEqual(after["commit"], last["commit"] + 1)

    def test_unavailable_rewrite_retry(self):
        first = self.lake.ingest("one", [put("x", 1)])
        self.lake.ingest("two", [put("y", 2)])
        files = self.lake.snapshot()["files"]
        before = self.state()
        self.lake.payload_port.set_available(first["file_id"], "a", False)
        with self.assertRaises(Unavailable):
            self.lake.compact(files)
        self.assertEqual(self.state(), before)
        self.lake.payload_port.set_available(first["file_id"], "a", True)
        result = self.lake.compact(files)
        self.assertEqual(self.lake.read(result["snapshot_id"], ["a"]),
                         [{"id": "x", "values": {"a": 1}},
                          {"id": "y", "values": {"a": 2}}])

    def test_bad_candidate_service_does_not_publish(self):
        self.lake.ingest("one", [put("x", 1)])
        before = self.state()
        real_segments = self.lake.segments

        class DamagedCandidateStore(SegmentStore):
            def write(inner, records):
                candidate = super().write(records)
                folder = inner.root / candidate.id
                (folder / "c1.json").write_bytes(b"[999]")
                return candidate

        damaged = DamagedCandidateStore(self.lake.catalog, self.lake.schema,
                                         self.lake.payload_port)
        self.lake.compaction.segments = damaged
        with self.assertRaises(IntegrityError):
            self.lake.compact(self.lake.snapshot()["files"])
        self.assertEqual(self.state(), before)
        self.lake.compaction.segments = real_segments
        result = self.lake.compact(self.lake.snapshot()["files"])
        self.assertEqual(self.lake.read(result["snapshot_id"], ["a"])[0]["values"]["a"], 1)
