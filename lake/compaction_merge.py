from dataclasses import dataclass


@dataclass(frozen=True)
class MergedRows:
    records: tuple


def _read_page(segments, schema, pages, target, column, position):
    key = target.id if not isinstance(target, str) else target
    cache = pages.setdefault(key, {})
    if column not in cache:
        cache.update(segments.read_columns(target, [column]))
    return cache[column][position]


def _full_values(segments, schema, pages, file_id, entry):
    return {column: _read_page(segments, schema, pages, file_id,
                               column, entry.position)
            for column in schema.columns}


def _coalesce_patches(segments, schema, pages, entries):
    """Fold a consecutive run of patch operations for one row.

    Only the last patch touching each column survives, so the run never
    grows with the number of repeated overwrites.
    """
    chosen = {}
    for file_id, entry in entries:
        for index in entry.columns:
            chosen[index] = (file_id, entry)
    values = {}
    for index in sorted(chosen):
        column = schema.columns[index]
        file_id, entry = chosen[index]
        values[column] = _read_page(segments, schema, pages, file_id,
                                    column, entry.position)
    _, last_entry = entries[-1]
    return {"id": last_entry.id, "seq": last_entry.seq,
            "op": "patch", "values": values}


def merge_selected_segments(segments, files, snapshot_files=None):
    """Reduce selected files while preserving meaning against the unselected
    files that remain in the snapshot.

    Unselected complete operations (put/delete) are inheritance barriers:
    selected patch history on either side cannot be joined across them, and
    selected files must never be synthesized into a full row carrying a
    later unselected operation's sequence. Only directories of unselected
    files are inspected (operation metadata); their column payloads are
    never read.
    """
    schema = segments.schema
    selected_set = set(files)
    if snapshot_files is None:
        # Standalone callers (tests) treat every registered segment as part
        # of the universe.
        snapshot_files = tuple(
            row[0] for row in segments.catalog.fetchall(
                "SELECT id FROM segments ORDER BY rowid"))
    histories = {}
    for file_id in snapshot_files:
        for entry in segments.directory(file_id):
            histories.setdefault(entry.id, []).append(
                (file_id, entry, file_id in selected_set))
    records = []
    pages = {}
    for row_id in sorted(histories):
        history = sorted(histories[row_id], key=lambda item: item[1].seq)
        run = []

        def flush():
            if not run:
                return
            complete = [item for item in run
                        if item[1].op in ("put", "delete")]
            if complete:
                base_file, base_entry, _ = complete[-1]
                record = {"id": row_id, "seq": base_entry.seq,
                          "op": base_entry.op}
                if base_entry.op == "put":
                    record["values"] = _full_values(
                        segments, schema, pages, base_file, base_entry)
                records.append(record)
                trailing = [item for item in run
                            if item[1].seq > base_entry.seq]
                if trailing:
                    records.append(_coalesce_patches(
                        segments, schema, pages,
                        [(f, e) for f, e, _ in trailing]))
            else:
                records.append(_coalesce_patches(
                    segments, schema, pages,
                    [(f, e) for f, e, _ in run]))

        for item in history:
            if item[2]:
                run.append(item)
                continue
            # An unselected operation closes the current selected run; it
            # stays in place in another snapshot member.
            flush()
            run = []
        flush()
    return MergedRows(tuple(records))
