import json
from .schema import Schema
from .integrity import IntegrityError, verify_digest


def encode_values(values):
    encoded = [Schema.encode_value(value) for value in values]
    return json.dumps(encoded, separators=(",", ":")).encode("ascii")


def decode_values(data, count):
    try:
        values = json.loads(data)
        if not isinstance(values, list) or len(values) != count:
            raise ValueError("page count mismatch")
        return [Schema.decode_value(value) for value in values]
    except (ValueError, TypeError, UnicodeError) as exc:
        raise IntegrityError("invalid value page") from exc


class ValuePageReader:
    def __init__(self, port):
        self.port = port

    def read(self, file_id, column, path, digest, count):
        data = self.port.read_bytes(file_id, column, path)
        verify_digest(data, digest)
        return decode_values(data, count)
