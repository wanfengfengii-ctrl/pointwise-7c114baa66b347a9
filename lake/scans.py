class SnapshotScanner:
    def __init__(self, schema, snapshots, segments):
        self.schema = schema
        self.snapshots = snapshots
        self.segments = segments

    def visible_rows(self, snapshot_id):
        snapshot = self.snapshots.get(snapshot_id)
        winners = {}
        for file_id in snapshot.files:
            for entry in self.segments.directory(file_id):
                previous = winners.get(entry.id)
                if previous is None or entry.seq > previous[1].seq:
                    winners[entry.id] = (file_id, entry)
        return {key: source for key, source in winners.items()
                if source[1].op == "put"}

    def read(self, snapshot_id, columns):
        columns = self.schema.validate_columns(columns)
        winners = self.visible_rows(snapshot_id)
        pages = {}
        for file_id, entry in winners.values():
            if file_id not in pages:
                pages[file_id] = self.segments.read_columns(file_id, columns)
        return [{"id": key,
                 "values": {column: pages[file_id][column][entry.position]
                            for column in columns}}
                for key, (file_id, entry) in sorted(winners.items())]
