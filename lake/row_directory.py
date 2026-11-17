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


def validate_directory(entries):
    previous = None
    for position, entry in enumerate(entries):
        Schema.validate_id(entry.id)
        if previous is not None and entry.id <= previous:
            raise ValueError("directory IDs must be strictly sorted")
        if type(entry.seq) is not int or entry.seq <= 0:
            raise ValueError("invalid business sequence")
        if entry.op not in ("put", "delete"):
            raise ValueError("invalid operation")
        if type(entry.position) is not int or entry.position != position:
            raise ValueError("invalid row position")
        previous = entry.id
    return entries


def encode_directory(entries):
    validate_directory(entries)
    return json.dumps([[e.id, e.seq, e.op, e.position] for e in entries],
                      ensure_ascii=True, separators=(",", ":")).encode("ascii")


def decode_directory(data, count):
    try:
        rows = json.loads(data)
        if not isinstance(rows, list) or len(rows) != count:
            raise ValueError("directory count mismatch")
        entries = []
        for row in rows:
            if not isinstance(row, list) or len(row) != 4:
                raise ValueError("bad entry")
            entries.append(RowEntry(*row))
        return tuple(validate_directory(entries))
    except (ValueError, TypeError, UnicodeError) as exc:
        raise IntegrityError("invalid row directory") from exc
