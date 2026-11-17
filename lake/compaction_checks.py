import json
from .integrity import IntegrityError


def _snapshot_files(segments, snapshot_id):
    row = segments.catalog.fetchone(
        "SELECT members FROM snapshots WHERE id=?", (snapshot_id,))
    if row is None:
        raise IntegrityError("compaction input snapshot is missing")
    return tuple(json.loads(row[0]))


def _read_page(segments, pages, target, column, position):
    key = target.id if not isinstance(target, str) else target
    cache = pages.setdefault(key, {})
    if column not in cache:
        cache.update(segments.read_columns(target, [column]))
    return cache[column][position]


def _expected_records(segments, plan):
    """Independent re-derivation of the bounded candidate contents.

    This mirrors the reduction rules without calling the merge
    implementation: replay per row across the whole input snapshot, split
    selected operations at every unselected barrier, keep only the last
    complete operation of each run and the last patch per column.
    """
    schema = segments.schema
    selected = set(plan.files)
    histories = {}
    for file_id in _snapshot_files(segments, plan.snapshot_id):
        for entry in segments.directory(file_id):
            histories.setdefault(entry.id, []).append(
                (file_id, entry, file_id in selected))
    records = []
    pages = {}
    for row_id in sorted(histories):
        history = sorted(histories[row_id], key=lambda item: item[1].seq)
        runs, current = [], []
        for item in history:
            if item[2]:
                current.append(item)
            else:
                if current:
                    runs.append(current)
                    current = []
        if current:
            runs.append(current)
        for run in runs:
            complete = [item for item in run
                        if item[1].op in ("put", "delete")]
            if complete:
                base_file, base_entry, _ = complete[-1]
                record = {"id": row_id, "seq": base_entry.seq,
                          "op": base_entry.op, "_source": (base_file, base_entry)}
                if base_entry.op == "put":
                    record["values"] = {
                        column: _read_page(segments, pages, base_file,
                                           column, base_entry.position)
                        for column in schema.columns}
                records.append(record)
                trailing = [item for item in run
                            if item[1].seq > base_entry.seq]
            else:
                trailing = run
            if trailing:
                # Each coalesced column keeps its own winning source; the
                # surviving patch entry only supplies the new sequence.
                chosen = {}
                for file_id, entry, _ in trailing:
                    for index in entry.columns:
                        chosen[index] = (file_id, entry)
                values = {}
                for index in sorted(chosen):
                    column = schema.columns[index]
                    file_id, entry = chosen[index]
                    values[column] = _read_page(
                        segments, pages, file_id, column, entry.position)
                _, last_entry, _ = trailing[-1]
                records.append({"id": row_id, "seq": last_entry.seq,
                                "op": "patch", "values": values,
                                "_mask": tuple(sorted(chosen)),
                                "_column_sources": chosen})
    return records


def verify_compaction_candidate(segments, plan, candidate):
    segments.verify(candidate)
    output = segments.directory(candidate)
    expected = _expected_records(segments, plan)
    if {entry.id for entry in output} != {record["id"] for record in expected}:
        raise IntegrityError("candidate changed ID coverage")
    if len(output) != len(expected):
        raise IntegrityError("candidate changed operation coverage")
    candidate_pages = {}
    for actual, record in zip(output, expected):
        if (actual.id, actual.seq, actual.op) != (
                record["id"], record["seq"], record["op"]):
            raise IntegrityError("candidate changed original operation or sequence")
        if record["op"] == "delete":
            continue
        if record["op"] == "patch":
            mask = actual.columns
            if mask is None or tuple(mask) != record["_mask"]:
                raise IntegrityError("candidate changed patch column mask")
            for index in mask:
                column = segments.schema.columns[index]
                source_file, source_entry = record["_column_sources"][index]
                actual_value = _read_page(segments, candidate_pages,
                                          candidate, column, actual.position)
                expected_value = _read_page(segments, {}, source_file,
                                            column, source_entry.position)
                if actual_value != expected_value:
                    raise IntegrityError("candidate changed payload value")
        else:
            source_file, source_entry = record["_source"]
            for column in segments.schema.columns:
                actual_value = _read_page(segments, candidate_pages,
                                          candidate, column, actual.position)
                expected_value = _read_page(segments, {}, source_file,
                                            column, source_entry.position)
                if actual_value != expected_value:
                    raise IntegrityError("candidate changed full-row value")
    return candidate
