import json
import uuid
from dataclasses import dataclass
from .integrity import IntegrityError, digest_bytes, verify_digest
from .row_directory import RowEntry, encode_directory, decode_directory
from .value_pages import encode_values, ValuePageReader


@dataclass(frozen=True)
class SegmentDescriptor:
    id: str
    directory_hash: str
    page_hashes: tuple
    count: int

    def to_descriptor(self):
        return {"id": self.id, "directory_hash": self.directory_hash,
                "page_hashes": list(self.page_hashes), "count": self.count}


class SegmentStore:
    def __init__(self, catalog, schema, port):
        self.catalog = catalog
        self.schema = schema
        self.port = port
        self.root = catalog.root / "segments"
        self.root.mkdir(exist_ok=True)
        self.page_reader = ValuePageReader(port)

    def descriptor(self, file_id):
        row = self.catalog.fetchone("SELECT descriptor FROM segments WHERE id=?",
                                   (file_id,))
        if row is None:
            raise KeyError(file_id)
        value = json.loads(row[0])
        value["page_hashes"] = tuple(value["page_hashes"])
        return SegmentDescriptor(**value)

    def register(self, descriptor):
        if not self.catalog.connection.in_transaction:
            raise RuntimeError("segment registration requires transaction")
        self.catalog.execute("INSERT INTO segments VALUES(?,?)",
                             (descriptor.id, json.dumps(descriptor.to_descriptor())))

    def payload_path(self, file_id, column):
        self.schema.validate_columns([column])
        self.descriptor(file_id)
        return self.root / file_id / f"c{self.schema.columns.index(column)}.json"

    def write(self, records):
        records = sorted(list(records), key=lambda row: row["id"])
        if not records:
            raise ValueError("empty segment")
        entries = []
        pages = {column: [] for column in self.schema.columns}
        for position, record in enumerate(records):
            if record["op"] == "put":
                values = self.schema.normalize_full_row(record["values"])
            elif record["op"] == "delete":
                values = dict.fromkeys(self.schema.columns)
                if "values" in record:
                    raise ValueError("delete cannot carry values")
            else:
                raise ValueError("unknown operation")
            entries.append(RowEntry(record["id"], record["seq"],
                                    record["op"], position))
            for column in self.schema.columns:
                pages[column].append(values[column])
        directory = encode_directory(entries)
        file_id = uuid.uuid4().hex
        folder = self.root / file_id
        folder.mkdir()
        (folder / "directory.json").write_bytes(directory)
        hashes = []
        for index, column in enumerate(self.schema.columns):
            data = encode_values(pages[column])
            (folder / f"c{index}.json").write_bytes(data)
            hashes.append(digest_bytes(data))
        descriptor = SegmentDescriptor(file_id, digest_bytes(directory),
                                       tuple(hashes), len(entries))
        self.verify(descriptor)
        return descriptor

    def _descriptor(self, value):
        return self.descriptor(value) if isinstance(value, str) else value

    def directory(self, file_or_descriptor):
        descriptor = self._descriptor(file_or_descriptor)
        try:
            data = (self.root / descriptor.id / "directory.json").read_bytes()
        except FileNotFoundError as exc:
            raise IntegrityError("missing operation directory") from exc
        verify_digest(data, descriptor.directory_hash)
        return decode_directory(data, descriptor.count)

    def read_columns(self, file_or_descriptor, columns):
        descriptor = self._descriptor(file_or_descriptor)
        columns = self.schema.validate_columns(columns)
        return {column: self.page_reader.read(
                    descriptor.id, column,
                    self.root / descriptor.id / f"c{self.schema.columns.index(column)}.json",
                    descriptor.page_hashes[self.schema.columns.index(column)],
                    descriptor.count)
                for column in columns}

    def verify(self, file_or_descriptor):
        descriptor = self._descriptor(file_or_descriptor)
        if len(descriptor.page_hashes) != len(self.schema.columns):
            raise IntegrityError("page descriptor count mismatch")
        entries = self.directory(descriptor)
        pages = self.read_columns(descriptor, self.schema.columns)
        for entry in entries:
            if entry.op == "delete" and any(
                    pages[c][entry.position] is not None for c in self.schema.columns):
                raise IntegrityError("tombstone contains payload values")
        return descriptor
