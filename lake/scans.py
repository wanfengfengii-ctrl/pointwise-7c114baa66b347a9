class SnapshotScanner:
    def __init__(self, schema, snapshots, segments):
        self.schema = schema
        self.snapshots = snapshots
        self.segments = segments

    def _histories(self, snapshot_id):
        """All directory entries per row in ascending business sequence.

        Directories are operation metadata; no column payload is read here.
        Snapshot file order is the commit order, and within a file the
        sequence is constant for a batch, so per-file order followed by the
        directory's (id, seq) order yields the logical replay order.
        """
        snapshot = self.snapshots.get(snapshot_id)
        histories = {}
        for file_id in snapshot.files:
            for entry in self.segments.directory(file_id):
                histories.setdefault(entry.id, []).append((file_id, entry))
        for history in histories.values():
            history.sort(key=lambda source: source[1].seq)
        return histories

    def visible_rows(self, snapshot_id):
        """Backward-compatible view of rows whose latest complete operation
        is a put. Rows rebuilt only by patches after a delete are absent;
        sparse reads use read() instead."""
        winners = {}
        for row_id, history in self._histories(snapshot_id).items():
            file_id, entry = history[-1]
            if entry.op == "put":
                winners[row_id] = (file_id, entry)
        return winners

    def read(self, snapshot_id, columns):
        columns = self.schema.validate_columns(columns)
        histories = self._histories(snapshot_id)
        column_index = {column: index for index, column in enumerate(self.schema.columns)}
        # One payload page per (file, projected column); nothing is read for
        # empty projections or rows hidden by a complete delete.
        pages = {}

        def page(file_id, column):
            cache = pages.setdefault(file_id, {})
            if column not in cache:
                cache.update(self.segments.read_columns(file_id, [column]))
            return cache[column]

        result = []
        for row_id in sorted(histories):
            history = histories[row_id]
            # Latest complete operation is the inheritance barrier. A row is
            # visible unless the barrier is a delete with no later patch; a
            # patch on a missing row starts from an all-NULL virtual baseline.
            barrier = None
            for source in reversed(history):
                if source[1].op in ("put", "delete"):
                    barrier = source
                    break
            if barrier is not None and barrier[1].op == "delete" and not any(
                    entry.op == "patch" and entry.seq > barrier[1].seq
                    for _, entry in history):
                continue
            values = {}
            for column in columns:
                index = column_index[column]
                chosen = None
                if barrier is not None and barrier[1].op == "put":
                    chosen = barrier
                # Patches after the latest complete barrier override it per
                # column; earlier patches are shadowed column by column.
                for file_id, entry in history:
                    if entry.op != "patch" or index not in (entry.columns or ()):
                        continue
                    if barrier is not None and entry.seq <= barrier[1].seq:
                        continue
                    chosen = (file_id, entry)
                if chosen is None:
                    # No baseline ever wrote this column: it is genuinely NULL.
                    values[column] = None
                else:
                    file_id, entry = chosen
                    values[column] = page(file_id, column)[entry.position]
            result.append({"id": row_id, "values": values})
        return result
