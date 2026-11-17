from dataclasses import dataclass


@dataclass(frozen=True)
class Schema:
    columns: tuple

    def __post_init__(self):
        if not isinstance(self.columns, (list, tuple)):
            raise ValueError("schema must be an ordered list of columns")
        columns = tuple(self.columns)
        if not columns or any(not isinstance(c, str) or not c for c in columns):
            raise ValueError("nonempty column names required")
        if len(set(columns)) != len(columns):
            raise ValueError("duplicate columns")
        object.__setattr__(self, "columns", columns)

    @staticmethod
    def validate_id(value):
        if not isinstance(value, str) or not value:
            raise ValueError("nonempty string required")
        return value

    @staticmethod
    def validate_value(value):
        if value is not None and type(value) is not int:
            raise ValueError("expected integer or NULL")
        return value

    @staticmethod
    def encode_value(value):
        Schema.validate_value(value)
        return None if value is None else format(value, "x")

    @staticmethod
    def decode_value(value):
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError("invalid integer encoding")
        decoded = int(value, 16)
        if format(decoded, "x") != value:
            raise ValueError("noncanonical integer encoding")
        return decoded

    def validate_columns(self, columns):
        if not isinstance(columns, (list, tuple)):
            raise ValueError("projection must be a list or tuple")
        result = tuple(columns)
        if any(not isinstance(c, str) or c not in self.columns for c in result):
            raise ValueError("unknown column")
        if len(set(result)) != len(result):
            raise ValueError("duplicate projection column")
        return result

    def normalize_full_row(self, values):
        if not isinstance(values, dict) or set(values) != set(self.columns):
            raise ValueError("put requires exactly all schema columns")
        return {c: self.validate_value(values[c]) for c in self.columns}

    def to_descriptor(self):
        return list(self.columns)
