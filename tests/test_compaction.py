import itertools
import tempfile
from pathlib import Path
from lake import Lake, IntegrityError, Unavailable
from lake.compaction_merge import merge_selected_segments
from lake.compaction_checks import verify_compaction_candidate
from lake.row_directory import decode_directory, encode_directory, RowEntry
from lake.segments import SegmentDescriptor
from lake.integrity import digest_bytes
from .reference import LakeCase, put, delete, replay, totals, COLUMNS


class TestCompaction(LakeCase):
    def test_noncontiguous_full_rows(self):
        batches = [[put("x", 1, 10), put("q", 99, 9)],
                   [put("x", 2, None)],
                   [put("y", 3, 30), delete("q")]]
        receipts = [self.lake.ingest(str(index), batch)
                    for index, batch in enumerate(batches)]
        before = self.lake.snapshot()
        rows = replay(batches)
        self.assertEqual(self.lake.read(before["id"], COLUMNS), rows)
        saved_id = self.lake.run_totals(before["id"], None, "station", "a")
        saved = self.lake.read_artifact(saved_id)
        unselected = receipts[1]["file_id"]
        for column in COLUMNS:
            self.lake.payload_port.set_available(unselected, column, False)
        self.lake.payload_port.clear_access_log()
        maintenance = self.lake.compact([receipts[0]["file_id"], receipts[2]["file_id"]])
        self.assertTrue(all(file_id != unselected
                            for file_id, column in self.lake.payload_port.access_log))
        self.assertEqual(set(self.lake.snapshot()["files"]),
                         {unselected, maintenance["file_id"]})
        entries = {entry.id: entry for entry in
                   self.lake.segments.directory(maintenance["file_id"])}
        self.assertEqual((entries["x"].seq, entries["q"].seq, entries["q"].op),
                         (1, 3, "delete"))
        for column in COLUMNS:
            self.lake.payload_port.set_available(unselected, column, True)
        self.assertEqual(self.lake.read(self.lake.snapshot()["id"], COLUMNS), rows)
        self.assertEqual(self.lake.read(before["id"], COLUMNS), rows)
        specs = [None, {"op": "range", "column": "a", "min": 2, "max": 3}]
        for spec in specs:
            self.assertEqual(self.groups(self.lake.snapshot()["id"], spec),
                             totals(rows, spec))
        self.assertEqual(self.lake.read_artifact(saved_id), saved)
        self.lake.compact(self.lake.snapshot()["files"])
        self.assertEqual(self.lake.read(self.lake.snapshot()["id"], COLUMNS), rows)
        self.assertEqual(self.lake.read(before["id"], COLUMNS), rows)

    def test_tombstone_shields_unselected_old_row(self):
        old = self.lake.ingest("old", [put("q", 99)])
        irrelevant = self.lake.ingest("other", [put("y", 4)])
        gone = self.lake.ingest("gone", [delete("q")])
        for column in COLUMNS:
            self.lake.payload_port.set_available(old["file_id"], column, False)
        receipt = self.lake.compact([irrelevant["file_id"], gone["file_id"]])
        self.assertEqual(self.lake.read(receipt["snapshot_id"], ["a"]),
                         [{"id": "y", "values": {"a": 4}}])
        entries = self.lake.segments.directory(receipt["file_id"])
        self.assertEqual([(entry.id, entry.seq, entry.op) for entry in entries],
                         [("q", 3, "delete"), ("y", 2, "put")])

    def test_selected_subsets_against_reference(self):
        operations = [put("x", 1, 2), put("x", None, 7), delete("x")]
        histories = list(itertools.product(operations, repeat=3))
        for history_index, history in enumerate(histories):
            for mask in range(1, 8):
                with self.subTest(history=history_index, mask=mask):
                    with tempfile.TemporaryDirectory(dir=self.path) as temporary:
                        with Lake(temporary) as lake:
                            batches = [[operation] for operation in history]
                            receipts = [lake.ingest(str(i), batch)
                                        for i, batch in enumerate(batches)]
                            old = [(receipt["snapshot_id"], replay(batches[:i + 1]))
                                   for i, receipt in enumerate(receipts)]
                            selected = [receipt["file_id"] for i, receipt in enumerate(receipts)
                                        if mask & (1 << i)]
                            lake.compact(selected)
                            lake.compact(lake.snapshot()["files"])
                            expected = replay(batches)
                            self.assertEqual(lake.read(lake.snapshot()["id"], COLUMNS), expected)
                            artifact = lake.run_totals(lake.snapshot()["id"], None, "station", "a")
                            self.assertEqual(lake.read_artifact(artifact)["groups"], totals(expected))
                            for snapshot_id, rows in old:
                                self.assertEqual(lake.read(snapshot_id, COLUMNS), rows)

    def test_bounded_output(self):
        receipts = [self.lake.ingest(str(i), [put("x", i % 2, 1)]) for i in range(30)]
        result = self.lake.compact([r["file_id"] for r in receipts])
        folder = self.lake.segments.root / result["file_id"]
        self.assertEqual(len(self.lake.segments.directory(result["file_id"])), 1)
        self.assertLess(sum(path.stat().st_size for path in folder.iterdir()), 256)
        self.assertEqual(self.lake.read(result["snapshot_id"], ["a"])[0]["values"]["a"], 1)

    def test_candidate_integrity(self):
        first = self.lake.ingest("first", [put("x", 1, 2)])
        last = self.lake.ingest("last", [put("y", 3, 4)])
        before = self.lake.snapshot()
        plan = self.lake.planner.prepare(before["files"])
        merged = merge_selected_segments(self.lake.segments, plan.files)
        records = list(merged.records)
        variants = [
            [dict(record, seq=99) for record in records],
            [records[0]],
            [dict(records[0], values={"station": 1, "a": 999, "b": 2}), records[1]],
            [delete("x") | {"seq": 1}, records[1]],
        ]
        for variant in variants:
            candidate = self.lake.segments.write(variant)
            with self.assertRaises(IntegrityError):
                verify_compaction_candidate(self.lake.segments, plan, candidate)
            self.assertEqual(self.lake.snapshot(), before)
        candidate = self.lake.segments.write(merged.records)
        directory = self.lake.segments.root / candidate.id / "directory.json"
        directory.write_bytes(b"[]")
        with self.assertRaises(IntegrityError):
            verify_compaction_candidate(self.lake.segments, plan, candidate)
        self.assertEqual(self.lake.snapshot(), before)
        self.lake.planner.mark_failed(plan.id, "test candidate rejected")
        self.assertEqual(self.lake.planner.get(plan.id).status, "FAILED")

    def test_invalid_selection_and_stale_plan(self):
        receipt = self.lake.ingest("first", [put("x", 1)])
        before = self.lake.snapshot()
        for files in ([], ["unknown"], [receipt["file_id"], receipt["file_id"]]):
            with self.assertRaises(ValueError):
                self.lake.compact(files)
            self.assertEqual(self.lake.snapshot(), before)
        plan = self.lake.planner.prepare(before["files"])
        candidate = self.lake.segments.write(
            merge_selected_segments(self.lake.segments, plan.files).records)
        plan = self.lake.planner.mark_candidate(plan.id, candidate)
        verify_compaction_candidate(self.lake.segments, plan, candidate)
        self.lake.planner.mark_verified(plan.id)
        self.lake.ingest("later", [put("y", 2)])
        later = self.lake.snapshot()
        with self.assertRaises(ValueError):
            self.lake.compaction.publisher.publish(plan.id)
        self.assertEqual(self.lake.snapshot(), later)
