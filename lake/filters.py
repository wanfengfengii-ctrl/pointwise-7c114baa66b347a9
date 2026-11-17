from dataclasses import dataclass


@dataclass(frozen=True)
class FilterSpec:
    op: str
    column: str = None
    value: object = None
    minimum: object = None
    maximum: object = None

    def to_descriptor(self):
        if self.op == "all":
            return None
        if self.op == "eq":
            return {"op": "eq", "column": self.column, "value": self.value}
        return {"op": "range", "column": self.column,
                "min": self.minimum, "max": self.maximum}


def normalize_filter(schema, spec):
    if spec is None:
        return FilterSpec("all")
    if not isinstance(spec, dict):
        raise ValueError("filter must be a mapping or None")
    op = spec.get("op")
    column = spec.get("column")
    schema.validate_columns([column])
    if op == "eq" and set(spec) == {"op", "column", "value"}:
        return FilterSpec("eq", column, schema.validate_value(spec["value"]))
    if op == "range" and set(spec) <= {"op", "column", "min", "max"}:
        minimum, maximum = spec.get("min"), spec.get("max")
        schema.validate_value(minimum)
        schema.validate_value(maximum)
        if minimum is None and maximum is None:
            raise ValueError("range requires at least one integer bound")
        if minimum is not None and maximum is not None and minimum > maximum:
            raise ValueError("reversed range")
        return FilterSpec("range", column, minimum=minimum, maximum=maximum)
    raise ValueError("invalid filter")


def required_columns(spec):
    return () if spec.op == "all" else (spec.column,)


def matches(spec, values):
    if spec.op == "all":
        return True
    value = values[spec.column]
    if spec.op == "eq":
        return value == spec.value
    return (value is not None
            and (spec.minimum is None or value >= spec.minimum)
            and (spec.maximum is None or value <= spec.maximum))
