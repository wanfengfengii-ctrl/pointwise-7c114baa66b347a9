from dataclasses import dataclass


@dataclass(frozen=True)
class MergedRows:
    records: tuple


def merge_selected_segments(segments, files):
    winners = {}
    for file_id in files:
        for entry in segments.directory(file_id):
            previous = winners.get(entry.id)
            if previous is None or entry.seq > previous[1].seq:
                winners[entry.id] = (file_id, entry)
    pages = {}
    records = []
    for key, (file_id, entry) in sorted(winners.items()):
        record = {"id": key, "seq": entry.seq, "op": entry.op}
        if entry.op == "put":
            if file_id not in pages:
                pages[file_id] = segments.read_columns(file_id, segments.schema.columns)
            record["values"] = {column: pages[file_id][column][entry.position]
                                for column in segments.schema.columns}
        records.append(record)
    return MergedRows(tuple(records))
