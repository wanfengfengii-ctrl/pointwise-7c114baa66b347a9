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
        records = sorted(list(records), key=lambda row: (row["id"], row["seq"]))
        if not records:
            raise ValueError("empty segment")
        entries = []
        pages = {column: [] for column in self.schema.columns}
        for position, record in enumerate(records):
            if record["op"] == "put":
                values = self.schema.normalize_full_row(record["values"])
                mask = None
            elif record["op"] == "delete":
                values = dict.fromkeys(self.schema.columns)
                if "values" in record:
                    raise ValueError("delete cannot carry values")
                mask = None
            elif record["op"] == "patch":
                if not isinstance(record.get("values"), dict) or not record["values"]:
                    raise ValueError("patch requires a nonempty column mapping")
                if set(record["values"]) - set(self.schema.columns):
                    raise ValueError("unknown patch column")
                # Explicit NULL still occupies a cell; omission is encoded as
                # NULL on the page and recorded as absent from the mask.
                values = dict.fromkeys(self.schema.columns)
                indices = []
                for column in self.schema.columns:
                    if column in record["values"]:
                        values[column] = self.schema.validate_value(
                            record["values"][column])
                        indices.append(self.schema.columns.index(column))
                if not indices:
                    raise ValueError("empty patch column mapping")
                mask = tuple(indices)
            else:
                raise ValueError("unknown operation")
            entries.append(RowEntry(record["id"], record["seq"],
                                    record["op"], position, mask))
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
        # Writing is local: validate operation metadata and structure without
        # re-reading the payload pages that were just written. Publication
        # callers run the payload-level verify() when historical availability
        # guarantees allow it.
        self.verify_directory(descriptor)
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
        width = len(self.schema.columns)
        for entry in entries:
            if entry.op == "delete" and any(
                    pages[c][entry.position] is not None for c in self.schema.columns):
                raise IntegrityError("tombstone contains payload values")
            elif entry.op == "patch":
                mask = entry.columns
                if mask is None or any(index >= width for index in mask):
                    raise IntegrityError("patch mask outside fixed schema")
                masked = set(mask)
                for index, column in enumerate(self.schema.columns):
                    if index not in masked and pages[column][entry.position] is not None:
                        raise IntegrityError("patch carries unmasked payload")
        return descriptor

    def verify_directory(self, file_or_descriptor):
        """Validate operation metadata and page digests without reading any
        column payload. Patch admission publishes without touching history,
        so its self-check must not require payload availability."""
        descriptor = self._descriptor(file_or_descriptor)
        if len(descriptor.page_hashes) != len(self.schema.columns):
            raise IntegrityError("page descriptor count mismatch")
        entries = self.directory(descriptor)
        width = len(self.schema.columns)
        for entry in entries:
            if entry.op == "patch":
                mask = entry.columns
                if mask is None or any(index >= width for index in mask):
                    raise IntegrityError("patch mask outside fixed schema")
        return descriptor
