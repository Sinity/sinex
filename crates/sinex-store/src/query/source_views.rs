//! Source counts and paged evidence use the same relations at the selected position.

pub(super) const GAPS: &str =
    "(SELECT source, id AS run, prior_end AS start, started_at AS \"end\",
    'between recorded live runs'::text AS reason FROM (
      SELECT source, id, started_at,
        max(ended_at) OVER (PARTITION BY source ORDER BY started_at, ended_at, id
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prior_end FROM (
        SELECT e.source, r.id, r.started_at, coalesce(r.ended_at, max(e.ts_orig)) AS ended_at
        FROM core.visible_events e JOIN core.runs r ON r.id=e.run_id
        JOIN registry.sources s ON s.name=e.source AND s.shapes=ARRAY['live-stream']
        WHERE e.commit_seq <= {seq}
        GROUP BY e.source, r.id, r.started_at, r.ended_at
      ) runs
    ) spans WHERE prior_end < started_at)";

pub(super) const DRIFT: &str = "jsonb_typeof({t}.payload->'unknown_fields')='array'
    AND {t}.payload->'unknown_fields' <> '[]'::jsonb";

/// A source's continuity evidence, over source row `{t}`.
pub(super) fn continuity() -> String {
    "(CASE WHEN {t}.shapes=ARRAY['live-stream'] AND EXISTS (SELECT 1 FROM core.visible_events e JOIN core.runs r ON r.id=e.run_id WHERE e.source={t}.name AND e.commit_seq <= {seq}) THEN 'run_lifetimes' ELSE 'unknown' END)".into()
}

/// The gaps between a source's recorded live runs.
pub(super) fn gaps() -> String {
    format!("(SELECT count(*) FROM {GAPS} gaps WHERE gaps.source={{t}}.name)")
}

/// The payload schema changes a source's parsers recorded.
pub(super) fn drift() -> String {
    format!("(SELECT jsonb_build_object('status',CASE WHEN count(*)=0 THEN 'unknown' ELSE 'changes_recorded' END,
            'reason','only payload field changes explicitly recorded by parsers are known', 'count',count(*))
            FROM core.visible_events e WHERE e.source={{t}}.name AND {}
            AND e.commit_seq <= {{seq}} AND NOT audit.is_archived_at(e.id, {{seq}}))", DRIFT.replace("{t}", "e"))
}

/// The records of a source that failed to parse.
pub(super) fn record_failures() -> String {
    "(SELECT count(*)
            FROM raw.failures f JOIN raw.materials m ON m.id=f.material_id JOIN raw.inputs i ON i.id=m.input_id
            JOIN raw.bindings b ON b.binding=i.binding WHERE b.source={t}.name)".into()
}
