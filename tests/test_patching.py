"""Sparse patch admission, projection inheritance and interleaved compaction.

The oracle below is an independent sequential replay over full-row state:
it never calls the lake's scanner, admission or compaction code to compute
an expectation.
"""
import itertools
import tempfile
from lake import Lake, Unavailable, IntegrityError
from .reference import LakeCase, fail_before_commit, COLUMNS


# ---------------------------------------------------------------------------
# Independent oracle
# ---------------------------------------------------------------------------

def replay_events(events, columns):
    """Replay (kind, id, mapping) events in order.

    A cell is ("set", value) once explicitly written by a put or patch and
    ("unset", None) while it has never been written since the last delete.
    Both states surface as integers/None through the public API; only the
    internal distinction lets the oracle model inheritance.
    """
    rows = {}
    for kind, row_id, mapping in events:
        if kind == "delete":
            rows.pop(row_id, None)
            continue
        if kind == "put":
            rows[row_id] = {column: ("set", mapping[column]) for column in columns}
        else:  # patch
            state = rows.setdefault(row_id, {column: ("unset", None)
                                             for column in columns})
            for column, value in mapping.items():
                state[column] = ("set", value)
    return [{"id": row_id,
             "values": {column: rows[row_id][column][1]
                        for column in columns}}
            for row_id in sorted(rows)]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestPatching(LakeCase):
    def test_hand_example_put_delete_patch(self):
        columns = ("a", "b")
        with tempfile.TemporaryDirectory(dir=self.path) as temporary:
            with Lake(temporary, columns) as lake:
                first = lake.ingest("write", [
                    {"op": "put", "id": "x", "values": {"a": 1, "b": 2}}])
                lake.ingest("remove", [{"op": "delete", "id": "x"}])
                patched = lake.apply_patch("correct", {"x": {"a": 9}})
                self.assertEqual(
                    lake.read(patched["snapshot_id"], ["a", "b"]),
                    [{"id": "x", "values": {"a": 9, "b": None}}])
                # The original fixed snapshot keeps the original full row.
                self.assertEqual(
                    lake.read(first["snapshot_id"], ["a", "b"]),
                    [{"id": "x", "values": {"a": 1, "b": 2}}])

    def test_patch_on_missing_row_creates_null_row(self):
        receipt = self.lake.apply_patch("sparse", {"new": {"a": 5}})
        self.assertEqual(
            self.lake.read(receipt["snapshot_id"], COLUMNS),
            [{"id": "new",
              "values": {"station": None, "a": 5, "b": None}}])

    def test_omitted_columns_inherit_explicit_null_overwrites(self):
        base = self.lake.ingest("base", [
            {"op": "put", "id": "x",
             "values": {"station": 1, "a": 2, "b": 3}}])
        first = self.lake.apply_patch("p1", {"x": {"a": None}})
        self.assertEqual(
            self.lake.read(first["snapshot_id"], COLUMNS)[0]["values"],
            {"station": 1, "a": None, "b": 3})
        second = self.lake.apply_patch("p2", {"x": {"b": 8}})
        # Explicit NULL stays until overwritten; omitted columns inherit.
        self.assertEqual(
            self.lake.read(second["snapshot_id"], COLUMNS)[0]["values"],
            {"station": 1, "a": None, "b": 8})
        # Full write replaces every column, cutting patch inheritance.
        full = self.lake.ingest("replace", [
            {"op": "put", "id": "x",
             "values": {"station": 4, "a": 0, "b": 0}}])
        self.assertEqual(
            self.lake.read(full["snapshot_id"], COLUMNS)[0]["values"],
            {"station": 4, "a": 0, "b": 0})
        again = self.lake.apply_patch("p3", {"x": {"a": -9}})
        self.assertEqual(
            self.lake.read(again["snapshot_id"], COLUMNS)[0]["values"],
            {"station": 4, "a": -9, "b": 0})
        # Delete cuts inheritance; a later patch rebuilds from NULL.
        self.lake.ingest("erase", [{"op": "delete", "id": "x"}])
        rebuilt = self.lake.apply_patch("p4", {"x": {"b": 11}})
        self.assertEqual(
            self.lake.read(rebuilt["snapshot_id"], COLUMNS),
            [{"id": "x",
              "values": {"station": None, "a": None, "b": 11}}])

    def test_interleaved_compaction_hand_example(self):
        columns = ("a", "b")
        with tempfile.TemporaryDirectory(dir=self.path) as temporary:
            with Lake(temporary, columns) as lake:
                f1 = lake.apply_patch("p1", {"x": {"a": 1}})
                f2 = lake.ingest("del", [{"op": "delete", "id": "x"}])
                f3 = lake.apply_patch("p2", {"x": {"b": 3}})
                before = lake.snapshot()
                expected = [{"id": "x", "values": {"a": None, "b": 3}}]
                self.assertEqual(lake.read(before["id"], columns), expected)
                maintenance = lake.compact([f1["file_id"], f3["file_id"]])
                # The unselected delete file stays untouched.
                self.assertEqual(set(lake.snapshot()["files"]),
                                 {f2["file_id"], maintenance["file_id"]})
                self.assertEqual(
                    lake.read(lake.snapshot()["id"], columns), expected)
                entries = lake.segments.directory(maintenance["file_id"])
                # No synthesized full row carrying F3's sequence.
                self.assertEqual(
                    [(entry.id, entry.seq, entry.op) for entry in entries],
                    [("x", f1["commit"], "patch"),
                     ("x", f3["commit"], "patch")])
                # Compacting again over the new layout stays equivalent.
                second = lake.compact(lake.snapshot()["files"])
                self.assertEqual(
                    lake.read(second["snapshot_id"], columns), expected)

    def test_replay_oracle_put_patch_delete_interleavings(self):
        columns = ("a", "b")
        choices = [
            ("put", {"a": 1, "b": 2}),
            ("put", {"a": 4, "b": None}),
            ("patch", {"a": 7}),
            ("patch", {"b": None}),
            ("delete", None),
        ]
        for length in (1, 2, 3):
            for history in itertools.product(choices, repeat=length):
                with self.subTest(history=history):
                    with tempfile.TemporaryDirectory(dir=self.path) as temporary:
                        self._check_history(temporary, columns, history)

    def test_replay_oracle_curated_long_histories(self):
        # Barrier-curtained four/five-step histories too large to enumerate.
        columns = ("a", "b")
        curated = [
            ("put", {"a": 1, "b": 2}), ("patch", {"a": 7}),
            ("delete", None), ("patch", {"b": 3}), ("patch", {"a": 4}),
        ]
        histories = [
            curated,
            list(reversed(curated)),
            [("patch", {"a": 7}), ("patch", {"a": 8}),
             ("put", {"a": 1, "b": 2}), ("patch", {"b": None})],
            [("delete", None), ("patch", {"a": 7}),
             ("delete", None), ("patch", {"b": 3})],
            [("put", {"a": 4, "b": None}), ("patch", {"b": 3}),
             ("patch", {"b": None}), ("patch", {"a": 9}),
             ("delete", None)],
        ]
        for history in histories:
            with self.subTest(history=history):
                with tempfile.TemporaryDirectory(dir=self.path) as temporary:
                    self._check_history(temporary, columns, tuple(history))

    def _build_history(self, lake, columns, history):
        events = []
        receipts = []
        for index, (kind, payload) in enumerate(history):
            batch_id = f"batch-{index}"
            if kind == "put":
                receipt = lake.ingest(batch_id, [
                    {"op": "put", "id": "x", "values": dict(payload)}])
                events.append(("put", "x", dict(payload)))
            elif kind == "patch":
                receipt = lake.apply_patch(batch_id, {"x": dict(payload)})
                events.append(("patch", "x", dict(payload)))
            else:
                receipt = lake.ingest(batch_id, [
                    {"op": "delete", "id": "x"}])
                events.append(("delete", "x", None))
            receipts.append(receipt)
        return events, receipts

    def _check_history(self, path, columns, history):
        # Fresh lake and data root per subset so every selection still names
        # current files (reopening the same root would replay batch IDs).
        for mask in range(1, 1 << len(history)):
            with tempfile.TemporaryDirectory(dir=path) as root:
                with Lake(root, columns) as lake:
                    events, receipts = self._build_history(lake, columns, history)
                    final_expected = replay_events(events, columns)
                    selected = [receipts[i]["file_id"]
                                for i in range(len(receipts))
                                if mask & (1 << i)]
                    plan = lake.compact(selected)
                    self.assertEqual(
                        lake.read(plan["snapshot_id"], columns), final_expected)
                    again = lake.compact(lake.snapshot()["files"])
                    self.assertEqual(
                        lake.read(again["snapshot_id"], columns), final_expected)
                    self._assert_bounded(lake, again["file_id"], columns)
                    # Fixed historical snapshots keep prefix-replay meaning.
                    for index, receipt in enumerate(receipts):
                        self.assertEqual(
                            lake.read(receipt["snapshot_id"], columns),
                            replay_events(events[:index + 1], columns))

    def _assert_bounded(self, lake, file_id, columns):
        entries = lake.segments.directory(file_id)
        # At most one entry per row per barrier-bounded run; with one row the
        # candidate directory cannot grow with overwrite count.
        self.assertLessEqual(len(entries), len(columns) + 1)
        folder = lake.segments.root / file_id
        self.assertLess(sum(path.stat().st_size for path in folder.iterdir()),
                        1024)

    def test_repeated_overwrite_compaction_is_bounded(self):
        first = self.lake.ingest("base", [
            {"op": "put", "id": "x",
             "values": {"station": 1, "a": 0, "b": 0}}])
        receipts = [first]
        for index in range(40):
            receipts.append(self.lake.apply_patch(
                f"patch-{index}", {"x": {"a": index}}))
        before_rows = self.lake.read(self.lake.snapshot()["id"], COLUMNS)
        result = self.lake.compact([r["file_id"] for r in receipts])
        folder = self.lake.segments.root / result["file_id"]
        entries = self.lake.segments.directory(result["file_id"])
        self.assertEqual(len(entries), 2)  # one put, one coalesced patch
        self.assertLess(
            sum(path.stat().st_size for path in folder.iterdir()), 512)
        self.assertEqual(
            self.lake.read(result["snapshot_id"], COLUMNS), before_rows)
        # Compact the output again; it must not duplicate the surviving ops.
        again = self.lake.compact(self.lake.snapshot()["files"])
        self.assertEqual(
            self.lake.read(again["snapshot_id"], COLUMNS), before_rows)
        entries = self.lake.segments.directory(again["file_id"])
        self.assertEqual(len(entries), 2)

    def test_patch_validation_and_identity(self):
        before = self.state()
        invalid_changes = [
            None, {}, [], "x",
            {"": {"a": 1}},
            {"x": {}},
            {"x": None},
            {"x": []},
            {"x": {"unknown": 1}},
            {"x": {"a": 1.5}},
            {"x": {"a": True}},
            {"x": {"a": False}},
            {"x": {"a": 1}, "y": None},
        ]
        for index, changes in enumerate(invalid_changes):
            with self.subTest(index=index):
                with self.assertRaises(ValueError):
                    self.lake.apply_patch(f"bad-{index}", changes)
                self.assertEqual(self.state(), before)
        with self.assertRaises(ValueError):
            self.lake.apply_patch("", {"x": {"a": 1}})
        # Ingest still refuses the patch operation kind.
        with self.assertRaises(ValueError):
            self.lake.ingest("legacy", [
                {"op": "patch", "id": "x", "values": {"a": 9}}])
        self.assertEqual(self.state(), before)
        # V1 name stays absent.
        self.assertFalse(hasattr(self.lake, "patch"))

    def test_replay_identity_normalization_and_kind_conflict(self):
        changes = {"y": {"b": 3, "a": None}, "x": {"a": 1}}
        first = self.lake.apply_patch("export", changes)
        before = self.state()
        # Different insertion order, reversed inner mapping: same content.
        replay = self.lake.apply_patch(
            "export", {"x": dict(reversed(list(changes["x"].items()))),
                       "y": {"b": 3, "a": None}})
        self.assertEqual(replay, first)
        self.assertEqual(self.state(), before)
        # Different content under the same batch ID is rejected.
        with self.assertRaises(ValueError):
            self.lake.apply_patch("export", {"x": {"a": 2}})
        # Same batch ID across operation kinds is rejected.
        with self.assertRaises(ValueError):
            self.lake.ingest("export", [
                {"op": "put", "id": "x",
                 "values": {"station": 1, "a": 1, "b": None}}])
        self.assertEqual(self.state(), before)

    def test_sequence_space_and_reopen(self):
        first = self.lake.ingest("ingest-1", [
            {"op": "put", "id": "x",
             "values": {"station": 1, "a": 1, "b": 1}}])
        patch = self.lake.apply_patch("patch-1", {"x": {"a": 2}})
        self.assertEqual(patch["commit"], first["commit"] + 1)
        self.reopen()
        after = self.lake.apply_patch("patch-2", {"x": {"b": 5}})
        self.assertEqual(after["commit"], patch["commit"] + 1)
        self.assertEqual(
            self.lake.read(after["snapshot_id"], COLUMNS)[0]["values"],
            {"station": 1, "a": 2, "b": 5})
        # Replay after reopen returns the original receipt.
        self.assertEqual(
            self.lake.apply_patch("patch-1", {"x": {"a": 2}}), patch)

    def test_patch_publish_failure_is_atomic(self):
        before = self.state()
        with fail_before_commit(self.lake.catalog, "ingest"):
            with self.assertRaises(RuntimeError):
                self.lake.apply_patch("atomic", {"x": {"a": 1}})
        self.assertEqual(self.state(), before)
        self.reopen()
        self.assertEqual(self.state(), before)
        receipt = self.lake.apply_patch("atomic", {"x": {"a": 1}})
        self.assertEqual(receipt["commit"], 1)
        # Replay stays idempotent after the failed attempt.
        self.assertEqual(
            self.lake.apply_patch("atomic", {"x": {"a": 1}}), receipt)

    def test_patch_never_reads_historical_payloads(self):
        old = self.lake.ingest("old", [
            {"op": "put", "id": "x",
             "values": {"station": 1, "a": 1, "b": 2}}])
        for column in COLUMNS:
            self.lake.payload_port.set_available(old["file_id"], column, False)
        self.lake.payload_port.clear_access_log()
        # Publication only writes a new segment; no historical column bytes.
        patched = self.lake.apply_patch("fix", {"x": {"a": 7}})
        self.assertEqual(self.lake.payload_port.access_log, [])
        self.assertEqual(
            self.lake.read(patched["snapshot_id"], ["a"]),
            [{"id": "x", "values": {"a": 7}}])
        self.assertEqual(
            self.lake.payload_port.access_log,
            [(patched["file_id"], "a")])
        # Inherited b lives in the offline old payload: Unavailable, not NULL.
        with self.assertRaises(Unavailable):
            self.lake.read(patched["snapshot_id"], ["b"])
        # The fixed old snapshot still surfaces the offline error.
        with self.assertRaises(Unavailable):
            self.lake.read(old["snapshot_id"], ["a"])
        self.lake.payload_port.set_available(old["file_id"], "b", True)
        self.assertEqual(
            self.lake.read(patched["snapshot_id"], ["b"]),
            [{"id": "x", "values": {"b": 2}}])

    def test_compaction_only_reads_selected_payloads(self):
        columns = ("a", "b")
        with tempfile.TemporaryDirectory(dir=self.path) as temporary:
            with Lake(temporary, columns) as lake:
                f1 = lake.ingest("f1", [
                    {"op": "put", "id": "x", "values": {"a": 1, "b": 2}}])
                middle = lake.apply_patch("f2", {"x": {"a": 5}})
                f3 = lake.apply_patch("f3", {"x": {"b": 9}})
                for column in columns:
                    lake.payload_port.set_available(
                        middle["file_id"], column, False)
                lake.payload_port.clear_access_log()
                plan = lake.compact([f1["file_id"], f3["file_id"]])
                touched = {file_id for file_id, _ in
                           lake.payload_port.access_log}
                self.assertNotIn(middle["file_id"], touched)
                # b's winning value comes from a selected file.
                self.assertEqual(
                    lake.read(plan["snapshot_id"], ["b"]),
                    [{"id": "x", "values": {"b": 9}}])
                # a's winning value lives in the unselected offline file:
                # Unavailable, never silently treated as NULL.
                with self.assertRaises(Unavailable):
                    lake.read(plan["snapshot_id"], ["a"])
                for column in columns:
                    lake.payload_port.set_available(
                        middle["file_id"], column, True)
                expected = [{"id": "x", "values": {"a": 5, "b": 9}}]
                self.assertEqual(
                    lake.read(lake.snapshot()["id"], columns), expected)

    def test_totals_and_artifacts_over_patched_rows(self):
        receipt = self.lake.apply_patch("p1", {
            "x": {"station": 1, "a": 4},
            "y": {"station": 1, "a": None},
            "z": {"station": 2, "a": -3},
        })
        artifact = self.lake.run_totals(
            receipt["snapshot_id"], None, "station", "a")
        saved = self.lake.read_artifact(artifact)
        self.assertEqual(saved["groups"], [
            {"group": 1, "rows": 2, "non_null": 1, "sum": 4},
            {"group": 2, "rows": 1, "non_null": 1, "sum": -3},
        ])
        maintenance = self.lake.compact(self.lake.snapshot()["files"])
        again = self.lake.run_totals(
            maintenance["snapshot_id"], None, "station", "a")
        self.assertEqual(
            self.lake.read_artifact(again)["groups"], saved["groups"])
        # Saved artifacts remain immutable and do not rescan data.
        for file_id, in self.lake.catalog.fetchall("SELECT id FROM segments"):
            for column in COLUMNS:
                self.lake.payload_port.set_available(file_id, column, False)
        self.lake.payload_port.clear_access_log()
        self.assertEqual(self.lake.read_artifact(artifact), saved)
        self.assertEqual(self.lake.payload_port.access_log, [])

    def test_integrity_error_on_corrupt_patch_page(self):
        patched = self.lake.apply_patch("p", {"x": {"a": 7}})
        path = self.lake.segments.payload_path(patched["file_id"], "a")
        original = path.read_bytes()
        path.write_bytes(b"[900]")
        with self.assertRaises(IntegrityError):
            self.lake.read(patched["snapshot_id"], ["a"])
        path.write_bytes(original)
        self.assertEqual(
            self.lake.read(patched["snapshot_id"], ["a"]),
            [{"id": "x", "values": {"a": 7}}])

    def test_multiple_rows_and_snapshot_fixation_through_compaction(self):
        events = []
        r1 = self.lake.ingest("init", [
            {"op": "put", "id": "x",
             "values": {"station": 1, "a": 1, "b": 1}},
            {"op": "put", "id": "y",
             "values": {"station": 2, "a": 2, "b": 2}},
        ])
        events += [("put", "x", {"station": 1, "a": 1, "b": 1}),
                   ("put", "y", {"station": 2, "a": 2, "b": 2})]
        r2 = self.lake.apply_patch("fix", {
            "x": {"b": None}, "y": {"a": 9}})
        events += [("patch", "x", {"b": None}),
                   ("patch", "y", {"a": 9})]
        r3 = self.lake.ingest("drop", [{"op": "delete", "id": "y"}])
        events.append(("delete", "y", None))
        fixed = replay_events(events, COLUMNS)
        self.assertEqual(self.lake.read(r3["snapshot_id"], COLUMNS), fixed)
        self.assertEqual(self.lake.read(r1["snapshot_id"], COLUMNS),
                         replay_events(events[:2], COLUMNS))
        self.lake.compact([r1["file_id"], r2["file_id"]])
        self.assertEqual(
            self.lake.read(self.lake.snapshot()["id"], COLUMNS), fixed)
        self.assertEqual(self.lake.read(r2["snapshot_id"], COLUMNS),
                         replay_events(events[:4], COLUMNS))
        self.reopen()
        self.assertEqual(
            self.lake.read(self.lake.snapshot()["id"], COLUMNS), fixed)
