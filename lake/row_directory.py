import json
from dataclasses import dataclass
from .schema import Schema
from .integrity import IntegrityError


@dataclass(frozen=True)
class RowEntry:
    id: str
    seq: int
    op: str
    position: int
    # None for complete operations (put/delete); a tuple of schema column
    # indices for sparse patch operations, sorted and deduplicated.
    columns: object = None


def validate_directory(entries):
    last_id = None
    last_seq = None
    for position, entry in enumerate(entries):
        Schema.validate_id(entry.id)
        if last_id is not None:
            if entry.id < last_id:
                raise ValueError("directory IDs must be sorted")
            if entry.id == last_id and entry.seq <= last_seq:
                raise ValueError("same-ID entries need increasing sequences")
        if type(entry.seq) is not int or entry.seq <= 0:
            raise ValueError("invalid business sequence")
        if entry.op not in ("put", "delete", "patch"):
            raise ValueError("invalid operation")
        if type(entry.position) is not int or entry.position != position:
            raise ValueError("invalid row position")
        if entry.op == "patch":
            mask = entry.columns
            if (not isinstance(mask, (list, tuple)) or not mask
                    or len(set(mask)) != len(mask)
                    or any(type(index) is not int or index < 0 for index in mask)
                    or tuple(mask) != tuple(sorted(mask))):
                raise ValueError("invalid patch column mask")
        elif entry.columns is not None:
            raise ValueError("complete operations cannot carry a column mask")
        last_id = entry.id
        last_seq = entry.seq
    return entries


def encode_directory(entries):
    validate_directory(entries)
    rows = []
    for entry in entries:
        row = [entry.id, entry.seq, entry.op, entry.position]
        if entry.op == "patch":
            row.append(list(entry.columns))
        rows.append(row)
    return json.dumps(rows, ensure_ascii=True,
                      separators=(",", ":")).encode("ascii")


def decode_directory(data, count):
    try:
        rows = json.loads(data)
        if not isinstance(rows, list) or len(rows) != count:
            raise ValueError("directory count mismatch")
        entries = []
        for row in rows:
            if not isinstance(row, list) or len(row) not in (4, 5):
                raise ValueError("bad entry")
            if len(row) == 5:
                if row[2] != "patch":
                    raise ValueError("only patch entries carry a mask")
                entries.append(RowEntry(row[0], row[1], row[2], row[3],
                                        tuple(row[4])))
            else:
                entries.append(RowEntry(*row))
        return tuple(validate_directory(entries))
    except (ValueError, TypeError, UnicodeError) as exc:
        raise IntegrityError("invalid row directory") from exc
