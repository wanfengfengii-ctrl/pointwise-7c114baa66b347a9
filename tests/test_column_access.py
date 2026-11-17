from lake import Unavailable, IntegrityError
from .reference import LakeCase, put, delete


class TestColumns(LakeCase):
    def test_unavailable_projection(self):
        old = self.lake.ingest("old", [put("x", 1, 2)])
        new = self.lake.ingest("new", [put("x", 9, 8)])
        port = self.lake.payload_port
        for column in self.lake.schema.columns:
            port.set_available(old["file_id"], column, False)
        port.set_available(new["file_id"], "b", False)
        port.clear_access_log()
        self.assertEqual(self.lake.read(new["snapshot_id"], ["a"]),
                         [{"id": "x", "values": {"a": 9}}])
        self.assertEqual(port.access_log, [(new["file_id"], "a")])
        port.clear_access_log()
        with self.assertRaises(Unavailable):
            self.lake.read(new["snapshot_id"], ["b"])
        self.assertEqual(port.access_log, [(new["file_id"], "b")])
        port.set_available(new["file_id"], "b", True)
        self.assertEqual(self.lake.read(new["snapshot_id"], ["b"])[0]["values"]["b"], 8)
        with self.assertRaises(Unavailable):
            self.lake.read(old["snapshot_id"], ["a"])
        port.clear_access_log()
        self.assertEqual(self.lake.read(new["snapshot_id"], []), [{"id": "x", "values": {}}])
        self.assertEqual(port.access_log, [])
        with self.assertRaises(ValueError):
            self.lake.read(new["snapshot_id"], ["unknown"])

    def test_integrity_failure(self):
        receipt = self.lake.ingest("readings", [put("x", 2, 7)])
        saved_id = self.lake.run_totals(receipt["snapshot_id"], None, "station", "a")
        saved = self.lake.read_artifact(saved_id)
        before = self.state()
        path = self.lake.segments.payload_path(receipt["file_id"], "a")
        original = path.read_bytes()
        path.write_bytes(b"[900]")
        with self.assertRaises(IntegrityError):
            self.lake.run_totals(receipt["snapshot_id"], None, "station", "a")
        self.assertEqual(self.state(), before)
        self.assertEqual(self.lake.read_artifact(saved_id), saved)
        self.assertEqual(self.lake.read(receipt["snapshot_id"], ["b"])[0]["values"]["b"], 7)
        path.write_bytes(original)
        self.assertEqual(self.groups(receipt["snapshot_id"]), saved["groups"])

    def test_winner_and_tombstone_dependencies(self):
        old = self.lake.ingest("old", [put("x", 1, 2)])
        for column in self.lake.schema.columns:
            self.lake.payload_port.set_available(old["file_id"], column, False)
        new = self.lake.ingest("independent", [put("x", 4, None)])
        gone = self.lake.ingest("gone", [delete("x")])
        self.lake.payload_port.clear_access_log()
        self.assertEqual(self.lake.read(gone["snapshot_id"], ["a", "b"]), [])
        self.assertEqual(self.lake.payload_port.access_log, [])
        self.assertEqual(self.lake.read(new["snapshot_id"], ["a", "b"]),
                         [{"id": "x", "values": {"a": 4, "b": None}}])

    def test_directory_integrity(self):
        receipt = self.lake.ingest("base", [put("x", 2)])
        path = self.lake.segments.root / receipt["file_id"] / "directory.json"
        path.write_bytes(b"[]")
        with self.assertRaises(IntegrityError):
            self.lake.read(receipt["snapshot_id"], [])
