//! The time each live output of a keyed derivation covers. An input can
//! change a key whose stored output covers part of the input's range; the
//! range an output covers is recorded only in its payload, and no index
//! reaches a payload value. Admission therefore keeps the range here for
//! every live output of the types a derivation declares, under indexes that
//! answer "which outputs of this derivation overlap this range" and "which
//! keys have an output with no end" at the cost of what they return.

/// One row per key and live output. `ended_at` is null for an output with
/// no recorded end and no fence, and for one its payload declares open. An
/// archived output has no row.
pub(crate) const TABLE: &str = "CREATE TABLE derivation.output_ranges (
  derivation text NOT NULL,
  key jsonb NOT NULL,
  event_id uuid NOT NULL,
  event_type text NOT NULL,
  started_at timestamptz NOT NULL,
  ended_at timestamptz,
  PRIMARY KEY (derivation, key, event_id)
)";

/// Ended outputs by the range they cover, open ones by key and start, and
/// every row by its event for the archive. The range is built from the
/// lesser and greater bound, so an output recorded as ending before it
/// starts never fails the index; the lookup compares the stored bounds.
pub(crate) const INDEXES: &[&str] = &[
    "CREATE INDEX output_ranges_ended ON derivation.output_ranges USING gist (derivation, event_type, tstzrange(least(started_at, ended_at), greatest(started_at, ended_at), '[]')) WHERE ended_at IS NOT NULL",
    "CREATE INDEX output_ranges_open ON derivation.output_ranges (derivation, event_type, key, started_at) WHERE ended_at IS NULL",
    "CREATE INDEX output_ranges_event ON derivation.output_ranges (event_id)",
];

/// The output ranges declared when this step landed, as `(derivation,
/// output type, start field, end field, open field, fence field)`. Admission
/// writes a row when an output is admitted, so a range declared later for
/// stored outputs is filled by its own step, or by the switch that
/// recomputes its derivation's history.
const RANGES_AT_UPGRADE: &[(&str, &str, &str, &str, Option<&str>, Option<&str>)] = &[
    (
        "activity.days",
        "activity.day",
        "started_at",
        "ended_at",
        Some("provisional"),
        None,
    ),
    (
        "activity.sessions",
        "activity.session",
        "started_at",
        "ended_at",
        None,
        None,
    ),
    (
        "analysis.products",
        "analysis.panel",
        "started_at",
        "ended_at",
        None,
        None,
    ),
    (
        "state.intervals",
        "state.interval",
        "started_at",
        "ended_at",
        None,
        Some("fenced_at"),
    ),
    (
        "work.episodes",
        "work.episode",
        "started_at",
        "last_activity",
        None,
        None,
    ),
];

/// Create the table and fill it from the live outputs of the declared
/// derivations. Only those derivations' events of those types are read, by
/// their segments in compressed chunks; no event row is written. The indexes
/// are built after the fill.
pub(crate) fn upgrade() -> Vec<String> {
    let quoted = |text: &str| format!("'{text}'");
    let optional = |text: Option<&str>| text.map_or("NULL".into(), quoted);
    let rows: Vec<String> = RANGES_AT_UPGRADE
        .iter()
        .map(|(derivation, output_type, start, end, open, fence)| {
            format!(
                "({}, {}, {}, {}, {}, {})",
                quoted(derivation),
                quoted(output_type),
                quoted(start),
                quoted(end),
                optional(*open),
                optional(*fence)
            )
        })
        .collect();
    let derivations: Vec<String> = RANGES_AT_UPGRADE
        .iter()
        .map(|range| quoted(range.0))
        .collect();
    let output_types: Vec<String> = RANGES_AT_UPGRADE
        .iter()
        .map(|range| quoted(range.1))
        .collect();
    // The inner query ends at OFFSET 0 so that the memberships and liveness
    // are joined to the five computed columns, not to whole event rows.
    let fill = format!(
        "INSERT INTO derivation.output_ranges (derivation, key, event_id, event_type, started_at, ended_at)
         SELECT o.derivation, o.key, s.id, s.event_type, s.started_at, s.ended_at
         FROM (
           SELECT e.id, e.source, e.event_type,
                  core.payload_time(e.payload->>d.start_field) AS started_at,
                  CASE WHEN e.payload->d.open_field = 'true'::jsonb THEN NULL
                       ELSE coalesce(core.payload_time(e.payload->>d.end_field),
                                     core.payload_time(e.payload->>d.fence_field)) END AS ended_at
           FROM core.events e
           JOIN (VALUES {rows}) AS d(derivation, output_type, start_field, end_field, open_field, fence_field)
             ON d.derivation = e.source AND d.output_type = e.event_type
           WHERE e.source IN ({derivations}) AND e.event_type IN ({output_types})
           OFFSET 0) s
         JOIN derivation.outputs o ON o.event_id = s.id AND o.derivation = s.source
         WHERE s.started_at IS NOT NULL
           AND EXISTS (SELECT 1 FROM core.live_equivalence l WHERE l.event_id = s.id)",
        rows = rows.join(", "),
        derivations = derivations.join(", "),
        output_types = output_types.join(", "),
    );
    let mut statements = vec![TABLE.into(), fill];
    statements.extend(INDEXES.iter().map(|sql| (*sql).into()));
    statements
}
