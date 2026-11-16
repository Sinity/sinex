//! Source counts and paged rows use the same relations at the selected position.

pub(super) const SOURCES: &str = r#"(
    SELECT s.*,
      CASE
        WHEN s.exclusion IS NOT NULL THEN 'excluded'
        WHEN c.missing OR f.silent OR f.overdue THEN 'stalled'
        WHEN f.absent THEN 'unavailable'
        WHEN f.recent THEN 'capturing'
        ELSE 'quiet'
      END AS state,
      CASE
        WHEN s.exclusion IS NOT NULL THEN s.exclusion
        WHEN c.missing THEN c.last_error
        WHEN f.silent THEN 'no reading within ' || s.silent_after::text
        WHEN f.overdue THEN 'no coverage read within ' || s.every::text
        WHEN f.absent THEN 'not_present'
      END AS reason,
      c.last_error
    FROM (
      SELECT r.name, r.shapes, r.version, r.event_types, r.purpose, r.expectation,
        r.silent_after, r.every, r.expires_after, r.exclusion FROM registry.sources r
      UNION ALL
      SELECT sc.source, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
      FROM core.source_counts sc
      WHERE NOT EXISTS (SELECT 1 FROM registry.sources r WHERE r.name = sc.source)
    ) s
    LEFT JOIN core.source_counts sc ON sc.source = s.name
    CROSS JOIN LATERAL (
      SELECT count(*) AS readings,
        bool_or(e.payload->>'state' = 'missing') AS missing,
        (array_agg((e.payload->>'ended_at')::timestamptz ORDER BY e.commit_seq DESC))[1] AS ended_at,
        max(e.ts_coided) AS last_seen,
        (array_agg(e.payload->>'reason' ORDER BY e.commit_seq DESC)
          FILTER (WHERE e.payload->>'state' = 'missing'))[1] AS last_error
      FROM core.visible_events e
      WHERE e.event_type = 'source.coverage'
        AND e.kind_version = (SELECT max(version) FROM registry.event_types WHERE name = 'source.coverage')
        AND e.payload->>'observed_source' = s.name
        AND e.commit_seq <= {seq} AND NOT audit.is_archived_at(e.id, {seq})
    ) c
    CROSS JOIN LATERAL (
      SELECT
        s.expectation = 'continuous' AND sc.last_seen < now() - s.silent_after AS silent,
        s.expectation = 'cadence' AND c.ended_at < now() - s.every AS overdue,
        coalesce(sc.events, 0) = 0 AND c.readings = 0 AS absent,
        greatest(sc.last_seen, c.last_seen) >= now() - interval '1 hour' AS recent
    ) f
) {t}"#;

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

/// A source's continuity rows (gaps and coverage), over source row `{t}`.
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
