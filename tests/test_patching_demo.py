"""Joint business demonstration for sparse corrections.

Runnable directly:

    python3 -B -m tests.test_patching_demo

It exercises, against a fresh temporary lake and the public API only:
  * a sparse correction while the superseded column payload is offline;
  * explicit NULL, omitted-column inheritance and delete/rebuild;
  * non-contiguous compaction interleaved with an unselected delete barrier;
  * a second compaction proving repeated rewrites do not accumulate history;
  * fixed snapshots, saved totals and close/reopen persistence.

Expectations come from an independent in-memory sequential replay; the lake
implementation is never used to compute them.
"""
import json
from tempfile import TemporaryDirectory
from pathlib import Path

from lake import Lake, Unavailable


COLUMNS = ("station", "a", "b")


def replay(events, columns):
    rows = {}
    for kind, row_id, mapping in events:
        if kind == "delete":
            rows.pop(row_id, None)
        elif kind == "put":
            rows[row_id] = {column: mapping[column] for column in columns}
        else:
            rows.setdefault(row_id, {column: None for column in columns})
            rows[row_id].update(mapping)
    return [{"id": row_id, "values": {column: rows[row_id][column]
                                      for column in columns}}
            for row_id in sorted(rows)]


def totals(rows, group="station", value="a"):
    groups = {}
    for row in rows:
        key = row["values"][group]
        state = groups.setdefault(key, [0, 0, 0])
        state[0] += 1
        if row["values"][value] is not None:
            state[1] += 1
            state[2] += row["values"][value]
    result = []
    for key in sorted(groups, key=lambda item: (item is not None, item or 0)):
        count, non_null, total = groups[key]
        result.append({"group": key, "rows": count, "non_null": non_null,
                       "sum": total if non_null else None})
    return result


def run_demo(path):
    events = []
    with Lake(path, COLUMNS) as lake:
        morning = lake.ingest("morning", [
            {"op": "put", "id": "x",
             "values": {"station": 1, "a": 2, "b": 20}},
            {"op": "put", "id": "y",
             "values": {"station": 1, "a": 4, "b": 30}},
        ])
        events += [("put", "x", {"station": 1, "a": 2, "b": 20}),
                   ("put", "y", {"station": 1, "a": 4, "b": 30})]

        # The old reading for x/a is temporarily unavailable at the offline
        # port; the correction must not be blocked by that legacy payload.
        lake.payload_port.set_available(morning["file_id"], "a", False)
        lake.payload_port.clear_access_log()
        correction = lake.apply_patch("correct-a", {
            "x": {"a": 7}, "y": {"a": 5}})
        events += [("patch", "x", {"a": 7}), ("patch", "y", {"a": 5})]
        # Publication writes only the new segment and reads no history.
        assert lake.payload_port.access_log == [], lake.payload_port.access_log
        # Every current a value now lives in the new segment.
        assert lake.read(correction["snapshot_id"], ["a"]) == [
            {"id": "x", "values": {"a": 7}},
            {"id": "y", "values": {"a": 5}}]
        assert (morning["file_id"], "a") not in lake.payload_port.access_log
        # The fixed old snapshot still needs the offline page.
        try:
            lake.read(morning["snapshot_id"], ["a"])
            raise AssertionError("old snapshot must stay Unavailable")
        except Unavailable:
            pass
        lake.payload_port.set_available(morning["file_id"], "a", True)
        assert lake.read(morning["snapshot_id"], ["a"]) == [
            {"id": "x", "values": {"a": 2}},
            {"id": "y", "values": {"a": 4}}]

        # Explicit NULL overrides; omitted columns keep inheriting.
        null_batch = lake.apply_patch("null-x-b", {"x": {"b": None}})
        events.append(("patch", "x", {"b": None}))
        expected = replay(events, COLUMNS)
        assert lake.read(null_batch["snapshot_id"], COLUMNS) == expected

        # Withdraw y, then rebuild it sparsely: every other column starts NULL.
        withdrawn = lake.ingest("withdraw-y", [{"op": "delete", "id": "y"}])
        events.append(("delete", "y", None))
        rebuilt = lake.apply_patch("rebuild-y", {"y": {"a": 9}})
        events.append(("patch", "y", {"a": 9}))
        expected = replay(events, COLUMNS)
        assert lake.read(rebuilt["snapshot_id"], COLUMNS) == expected

        artifact = lake.run_totals(rebuilt["snapshot_id"], None, "station", "a")
        saved = lake.read_artifact(artifact)
        assert saved["groups"] == totals(expected)

        # Non-contiguous compaction: keep the delete file as a barrier.
        before_files = lake.snapshot()["files"]
        plan = lake.compact([morning["file_id"], rebuilt["file_id"]])
        assert withdrawn["file_id"] in lake.snapshot()["files"]
        assert lake.read(plan["snapshot_id"], COLUMNS) == expected
        # A second rewrite over the new layout stays equivalent and bounded.
        folder = lake.segments.root / plan["file_id"]
        first_size = sum(p.stat().st_size for p in folder.iterdir())
        again = lake.compact(lake.snapshot()["files"])
        assert lake.read(again["snapshot_id"], COLUMNS) == expected
        second_size = sum(
            p.stat().st_size for p in
            (lake.segments.root / again["file_id"]).iterdir())
        assert second_size <= first_size * 2
        downstream = lake.run_totals(
            again["snapshot_id"], None, "station", "a")
        assert lake.read_artifact(downstream)["groups"] == saved["groups"]

        # Fixed old snapshot remains readable with its original contents.
        assert lake.read(morning["snapshot_id"], COLUMNS) == replay(
            events[:2], COLUMNS)
        snapshot_before_compaction = lake.get_snapshot(plan["input_snapshot_id"])
        evidence = {
            "case_id": "v7.sparse_patching",
            "expected_rows": expected,
            "totals": saved["groups"],
            "selected": plan["selected"],
            "kept_barrier": withdrawn["file_id"],
            "files_before": before_files,
            "files_after": lake.snapshot()["files"],
            "input_snapshot_fixed": snapshot_before_compaction,
        }

    with Lake(path, COLUMNS) as lake:
        assert lake.read(lake.snapshot()["id"], COLUMNS) == expected
        assert lake.read_artifact(artifact)["groups"] == saved["groups"]
        assert lake.read_artifact(downstream)["groups"] == saved["groups"]
        # Replay after reopen returns the original receipt.
        assert lake.apply_patch("rebuild-y", {"y": {"a": 9}}) == rebuilt
        evidence["reopen_rows"] = lake.read(lake.snapshot()["id"], COLUMNS)

    print("PATCHING_SCENARIO " + json.dumps(evidence, ensure_ascii=False,
                                            separators=(",", ":")))


if __name__ == "__main__":
    with TemporaryDirectory(prefix="monitoring-patching-") as temporary:
        run_demo(Path(temporary))
