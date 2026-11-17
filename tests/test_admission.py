import copy
from lake import Lake
from .reference import LakeCase, put, delete, fail_before_commit


class TestAdmission(LakeCase):
    def test_full_replace_delete(self):
        first = self.lake.ingest("first", [put("x", 1, 2), put("y", 5, 6)])
        second = self.lake.ingest("replace", [put("x", 9, None)])
        third = self.lake.ingest("withdraw", [delete("y")])
        self.assertEqual([first["commit"], second["commit"], third["commit"]], [1, 2, 3])
        self.assertEqual(self.lake.read(third["snapshot_id"], ["a", "b"]),
                         [{"id": "x", "values": {"a": 9, "b": None}}])
        self.assertEqual(len(self.lake.read(first["snapshot_id"], ["a"])), 2)
        self.reopen()
        fourth = self.lake.ingest("after-open", [put("z", -7)])
        self.assertEqual(fourth["commit"], 4)

    def test_replay_and_rejection(self):
        batch = [put("y", None, 3), put("x", 2, None)]
        first = self.lake.ingest("export", batch)
        self.lake.ingest("later", [put("z", 8)])
        before = self.state()
        reordered = copy.deepcopy(batch[::-1])
        for operation in reordered:
            operation["values"] = dict(reversed(list(operation["values"].items())))
        self.assertEqual(self.lake.ingest("export", reordered), first)
        self.assertEqual(self.state(), before)
        changed = [
            [put("y", 0, 3), put("x", 2, None)],
            [put("y", None, 3), put("x", 2, 0)],
            [delete("y"), put("x", 2, None)],
        ]
        for operations in changed:
            with self.subTest(operations=operations):
                with self.assertRaises(ValueError):
                    self.lake.ingest("export", operations)
                self.assertEqual(self.state(), before)
        bad = put("bad", 1)
        del bad["values"]["b"]
        with self.assertRaises(ValueError):
            self.lake.ingest("missing", [bad])
        self.assertEqual(self.state(), before)

    def test_bad_batch_atomic(self):
        invalid = [
            [], [put("x", 1), put("x", 2)], [put("", 1)],
            [put("x", True)], [put("x", 1, False)], [put("x", 1.5)],
            [{"op": "patch", "id": "x", "values": {"a": 9}}],
            [{"op": "delete", "id": "x", "values": {}}],
            [{"op": "put", "id": "x", "values": {"a": 1}}],
            [dict(put("x", 1), extra=0)],
        ]
        before = self.state()
        for index, operations in enumerate(invalid):
            with self.subTest(index=index):
                with self.assertRaises(ValueError):
                    self.lake.ingest(f"bad-{index}", [put("valid", 7)] + operations
                                     if operations else operations)
                self.assertEqual(self.state(), before)
        extra = put("x", 1)
        extra["values"]["unknown"] = 3
        with self.assertRaises(ValueError):
            self.lake.ingest("extra", [extra])
        self.assertEqual(self.state(), before)
        self.assertFalse(hasattr(self.lake, "patch"))

    def test_initialization_preserves_state(self):
        receipt = self.lake.ingest("initial", [put("x", 7, 8)])
        artifact = self.lake.run_totals(receipt["snapshot_id"], None, "station", "a")
        self.lake.ingest("user-change", [put("x", 42, None)])
        before = self.state()
        saved = self.lake.read_artifact(artifact)
        self.reopen()
        self.assertEqual(self.state(), before)
        self.assertEqual(self.lake.read_artifact(artifact), saved)
        self.assertEqual(self.lake.ingest("initial", [put("x", 7, 8)]), receipt)
        with self.assertRaises(ValueError):
            Lake(self.path, ("station", "a"))
        self.assertEqual(self.state(), before)
        self.assertEqual(self.lake.read(self.lake.snapshot()["id"], ["a"])[0]["values"]["a"], 42)

    def test_publish_rollback(self):
        before = self.state()
        with fail_before_commit(self.lake.catalog, "ingest"):
            with self.assertRaises(RuntimeError):
                self.lake.ingest("retry", [put("x", 3)])
        self.assertEqual(self.state(), before)
        self.reopen()
        self.assertEqual(self.state(), before)
        receipt = self.lake.ingest("retry", [put("x", 3)])
        self.assertEqual(receipt["commit"], 1)
        self.assertEqual(self.lake.ingest("retry", [put("x", 3)]), receipt)

    def test_source_failure_atomic(self):
        def broken():
            yield put("x", 1)
            raise RuntimeError("export interrupted")
        before = self.state()
        with self.assertRaises(RuntimeError):
            self.lake.ingest("broken", broken())
        self.assertEqual(self.state(), before)
