from lake import Unavailable
from .reference import LakeCase, put, fail_before_commit


class TestBindings(LakeCase):
    def test_old_artifact_after_reopen(self):
        first = self.lake.ingest("first", [put("x", 2), put("y", None)])
        snapshot = self.lake.get_snapshot(first["snapshot_id"])
        rows = self.lake.read(snapshot["id"], ["a"])
        artifact_id = self.lake.run_totals(snapshot["id"], None, "station", "a")
        saved = self.lake.read_artifact(artifact_id)
        self.lake.ingest("changed", [put("x", 100)])
        self.lake.compact(self.lake.snapshot()["files"])
        self.reopen()
        self.assertEqual(self.lake.get_snapshot(snapshot["id"]), snapshot)
        self.assertEqual(self.lake.read(snapshot["id"], ["a"]), rows)
        port = self.lake.payload_port
        for (file_id,) in self.lake.catalog.fetchall("SELECT id FROM segments"):
            for column in self.lake.schema.columns:
                port.set_available(file_id, column, False)
        port.clear_access_log()
        self.assertEqual(self.lake.read_artifact(artifact_id), saved)
        self.assertEqual(port.access_log, [])
        with self.assertRaises(Unavailable):
            self.lake.read(snapshot["id"], ["a"])
        changed = self.lake.read_artifact(artifact_id)
        changed["source"]["snapshot_id"] = -1
        changed["groups"][0]["sum"] = -100
        snapshot["files"].clear()
        self.assertEqual(self.lake.read_artifact(artifact_id), saved)
        self.assertEqual(self.lake.get_snapshot(first["snapshot_id"])["files"],
                         [first["file_id"]])
        with self.assertRaises(KeyError):
            self.lake.get_snapshot(99999)
        with self.assertRaises(KeyError):
            self.lake.read_artifact("missing")
        self.reopen()
        self.assertEqual(self.lake.read_artifact(artifact_id), saved)
        self.assertEqual(self.lake.read(first["snapshot_id"], ["a"]), rows)

    def test_artifact_publish_rollback(self):
        receipt = self.lake.ingest("base", [put("x", 3)])
        saved_id = self.lake.run_totals(receipt["snapshot_id"], None, "station", "a")
        saved = self.lake.read_artifact(saved_id)
        before = self.state()
        with fail_before_commit(self.lake.catalog, "artifact"):
            with self.assertRaises(RuntimeError):
                self.lake.run_totals(receipt["snapshot_id"], None, "station", "a")
        self.assertEqual(self.state(), before)
        self.reopen()
        self.assertEqual(self.state(), before)
        self.assertEqual(self.lake.read_artifact(saved_id), saved)
        self.assertEqual(self.groups(receipt["snapshot_id"]), saved["groups"])
