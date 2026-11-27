//! Stored fields needed by status. Coverage readings keep their event's
//! original position for saved reads. Fusion counts summarize live events
//! by class, so the reader sees only its permitted classes.

pub(crate) const COVERAGE_TABLE: &str = "CREATE TABLE core.coverage_readings (
  event_id uuid PRIMARY KEY,
  source text NOT NULL,
  kind_version bigint NOT NULL,
  class text NOT NULL,
  commit_seq bigint NOT NULL,
  seen_at timestamptz,
  ended_at timestamptz,
  missing boolean NOT NULL,
  reason text,
  live boolean NOT NULL
)";

pub(crate) const COVERAGE_INDEXES: &[&str] = &[
    "CREATE INDEX coverage_at_position ON core.coverage_readings (source, kind_version, commit_seq DESC)",
    "CREATE INDEX live_coverage_ended ON core.coverage_readings (source, kind_version, class, commit_seq DESC) INCLUDE (ended_at) WHERE live",
    "CREATE INDEX live_coverage_seen ON core.coverage_readings (source, kind_version, class, seen_at DESC NULLS LAST) WHERE live",
    "CREATE INDEX live_coverage_missing ON core.coverage_readings (source, kind_version, class, commit_seq DESC) INCLUDE (reason) WHERE live AND missing",
];

pub(crate) const COVERAGE_VIEW: &str = "CREATE VIEW core.visible_coverage_readings AS
  SELECT event_id, source, kind_version, class, commit_seq, seen_at, ended_at, missing, reason, live
  FROM core.coverage_readings WHERE core.class_visible(class)";

pub(crate) const COVERAGE_FIELDS: &str = "CREATE OR REPLACE FUNCTION core.coverage_fields(p jsonb)
RETURNS TABLE (source text, ended_at timestamptz, missing boolean, reason text)
LANGUAGE sql STABLE AS $$
  SELECT p->>'observed_source', core.payload_time(p->>'ended_at'),
         coalesce(p->>'state' = 'missing', false), p->>'reason'
  WHERE p->>'observed_source' IS NOT NULL
$$";

pub(crate) const FUSION_READINGS: &str = "CREATE TABLE core.fusion_readings (
  event_id uuid PRIMARY KEY,
  name text NOT NULL,
  figure text NOT NULL CHECK (figure IN ('undecided', 'decided', 'ambiguous')),
  class text NOT NULL,
  live boolean NOT NULL
)";

pub(crate) const FUSION_COUNTS: &str = "CREATE TABLE core.fusion_counts (
  name text NOT NULL,
  figure text NOT NULL CHECK (figure IN ('undecided', 'decided', 'ambiguous')),
  class text NOT NULL,
  count bigint NOT NULL,
  PRIMARY KEY (name, figure, class)
)";

pub(crate) const FUSION_VIEW: &str = "CREATE VIEW core.visible_fusion_counts AS
  SELECT name, figure, class, count FROM core.fusion_counts WHERE core.class_visible(class)";

pub(crate) const FUSION_FIGURE: &str =
    "CREATE OR REPLACE FUNCTION core.fusion_figure(kind text, p jsonb)
RETURNS TABLE (name text, figure text)
LANGUAGE sql IMMUTABLE AS $$
  SELECT p->>'family',
         CASE WHEN p->>'state' = 'undecided' THEN 'undecided' ELSE 'decided' END
  WHERE kind = 'fusion.considered_pair' AND p->>'family' IS NOT NULL AND p->>'state' IS NOT NULL
  UNION ALL
  SELECT kind, 'ambiguous'
  WHERE kind IN ('command.fused', 'mail.fused')
    AND CASE WHEN btrim(p->>'ambiguous') ~ '^[+-]?[0-9]+$' THEN (p->>'ambiguous')::numeric > 1 END
$$";

pub(crate) fn upgrade() -> Vec<String> {
    let mut statements = [
        COVERAGE_TABLE,
        FUSION_READINGS,
        FUSION_COUNTS,
        COVERAGE_FIELDS,
        FUSION_FIGURE,
    ]
    .map(String::from)
    .to_vec();
    statements.push(
        "INSERT INTO core.coverage_readings (event_id, source, kind_version, class, commit_seq, seen_at, ended_at, missing, reason, live)
         SELECT e.id, fields.source, e.kind_version, e.class, e.commit_seq, e.ts_coided,
                fields.ended_at, fields.missing, fields.reason,
                (SELECT a.event_id FROM audit.archived a WHERE a.event_id = e.id) IS NULL
         FROM core.events e CROSS JOIN LATERAL core.coverage_fields(e.payload) fields
         WHERE e.event_type = 'source.coverage'"
            .into(),
    );
    statements.push(
        "INSERT INTO core.fusion_readings (event_id, name, figure, class, live)
         SELECT e.id, f.name, f.figure, e.class,
                (SELECT a.event_id FROM audit.archived a WHERE a.event_id = e.id) IS NULL
         FROM core.events e CROSS JOIN LATERAL core.fusion_figure(e.event_type, e.payload) f
         WHERE e.event_type IN ('fusion.considered_pair', 'command.fused', 'mail.fused')"
            .into(),
    );
    statements.push(
        "INSERT INTO core.fusion_counts (name, figure, class, count)
         SELECT name, figure, class, count(*) FROM core.fusion_readings
         WHERE live GROUP BY name, figure, class"
            .into(),
    );
    statements.extend(COVERAGE_INDEXES.iter().map(|sql| (*sql).into()));
    statements
}
