import json
from .compaction_merge import merge_selected_segments
from .compaction_checks import verify_compaction_candidate


class CompactionService:
    def __init__(self, planner, segments, publisher):
        self.planner = planner
        self.segments = segments
        self.publisher = publisher

    def compact(self, files):
        plan = self.planner.prepare(files)
        try:
            members = self.segments.catalog.fetchone(
                "SELECT members FROM snapshots WHERE id=?",
                (plan.snapshot_id,))[0]
            snapshot_files = tuple(json.loads(members))
            merged = merge_selected_segments(
                self.segments, plan.files, snapshot_files)
            candidate = self.segments.write(merged.records)
            plan = self.planner.mark_candidate(plan.id, candidate)
            verify_compaction_candidate(self.segments, plan, candidate)
            self.planner.mark_verified(plan.id)
            return self.publisher.publish(plan.id)
        except Exception as exc:
            try:
                self.planner.mark_failed(plan.id, exc)
            except Exception as state_error:
                exc.add_note(f"could not record failed plan: {state_error}")
            raise
