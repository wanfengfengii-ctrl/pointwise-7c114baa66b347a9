from .filters import normalize_filter, required_columns, matches
from .group_totals import aggregate_totals


class AccountingService:
    def __init__(self, schema, bindings, scanner, artifacts):
        self.schema = schema
        self.bindings = bindings
        self.scanner = scanner
        self.artifacts = artifacts

    def run_totals(self, snapshot_id, filter_spec, group_column, value_column):
        self.schema.validate_columns([group_column])
        self.schema.validate_columns([value_column])
        spec = normalize_filter(self.schema, filter_spec)
        binding = self.bindings.prepare(snapshot_id, spec, group_column, value_column)
        dependencies = set(required_columns(spec)) | {group_column, value_column}
        columns = [column for column in self.schema.columns if column in dependencies]
        rows = self.scanner.read(snapshot_id, columns)
        filtered = (row for row in rows if matches(spec, row["values"]))
        groups = aggregate_totals(filtered, group_column, value_column)
        return self.artifacts.publish(binding, groups)
