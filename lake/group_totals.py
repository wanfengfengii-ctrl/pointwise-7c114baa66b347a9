from dataclasses import dataclass


@dataclass(frozen=True)
class GroupTotal:
    group: object
    rows: int
    non_null: int
    sum: object

    def to_descriptor(self):
        return {"group": self.group, "rows": self.rows,
                "non_null": self.non_null, "sum": self.sum}


def sort_group_keys(keys):
    return sorted(keys, key=lambda value: (value is not None,
                                          0 if value is None else value))


def aggregate_totals(rows, group_column, value_column):
    groups = {}
    for row in rows:
        values = row["values"]
        key, value = values[group_column], values[value_column]
        state = groups.setdefault(key, [0, 0, 0])
        state[0] += 1
        if value is not None:
            state[1] += 1
            state[2] += value
    result = []
    for key in sort_group_keys(groups):
        count, non_null, total = groups[key]
        result.append(GroupTotal(key, count, non_null,
                                 total if non_null else None).to_descriptor())
    return result
