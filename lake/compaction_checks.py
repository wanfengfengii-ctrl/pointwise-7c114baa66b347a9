from .integrity import IntegrityError


def verify_compaction_candidate(segments, plan, candidate):
    segments.verify(candidate)
    output = segments.directory(candidate)
    expected = {}
    for file_id in plan.files:
        for entry in segments.directory(file_id):
            if entry.id not in expected or entry.seq > expected[entry.id][1].seq:
                expected[entry.id] = (file_id, entry)
    if {entry.id for entry in output} != set(expected):
        raise IntegrityError("candidate changed ID coverage")
    candidate_pages = segments.read_columns(candidate, segments.schema.columns)
    source_pages = {}
    for actual in output:
        file_id, original = expected[actual.id]
        if (actual.seq, actual.op) != (original.seq, original.op):
            raise IntegrityError("candidate changed original operation or sequence")
        if actual.op == "delete":
            continue
        if file_id not in source_pages:
            source_pages[file_id] = segments.read_columns(file_id, segments.schema.columns)
        for column in segments.schema.columns:
            if (candidate_pages[column][actual.position]
                    != source_pages[file_id][column][original.position]):
                raise IntegrityError("candidate changed full-row value")
    return candidate
