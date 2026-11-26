//! How far live intervals reach. An interval kind's events last from
//! `ts_orig` to an end their payload records; a window read wants the earlier
//! ones that still reach it. Compressed event chunks carry no index over a
//! payload value, so how far every live interval that lasts longer than an
//! instant reaches is kept here, beside the events, under indexes that
//! answer "which intervals of this kind contain this instant" and "which are
//! open" at the cost of the rows they return. An interval reaches to its
//! end, or, when its end is unknown, to the time its payload says bounds it.

/// The end of an interval that started at `started_at`, from the value its
/// payload records under its kind's end field. A missing or unreadable time
/// leaves an `at` interval open (null) and an `end_or_instant` one an
/// instant; a duration is whole microseconds, rounded up, and absent, wrong
/// or negative durations are zero. It never raises, because admission runs
/// it on every interval it stores.
pub(crate) const INTERVAL_END: &str =
    "CREATE FUNCTION core.interval_end(rule text, started_at timestamptz, recorded jsonb)
RETURNS timestamptz LANGUAGE plpgsql STABLE AS $$
BEGIN
  IF rule = 'after_ns' THEN
    RETURN started_at + interval '1 microsecond' * ceil(
      CASE WHEN jsonb_typeof(recorded) = 'number'
           THEN least(greatest((recorded #>> '{}')::numeric, 0), 9000000000000000000)
           ELSE 0 END / 1000)::double precision;
  END IF;
  RETURN coalesce(core.payload_time(recorded #>> '{}'),
                  CASE WHEN rule = 'end_or_instant' THEN started_at END);
END $$";

/// One row per live interval: an open one (`ended_at` null) or one that
/// reaches past its start. `ended_at` is how far the interval reaches: its
/// end, or the bound of an end left unknown (the fence of a state interval),
/// so it is not the interval's duration. `narrowing` is the text of the
/// payload field its kind's readers narrow by, null when the kind names none
/// or the payload has none.
pub(crate) const TABLE: &str = "CREATE TABLE core.live_intervals (
  event_id uuid PRIMARY KEY,
  event_type text NOT NULL,
  narrowing text,
  started_at timestamptz NOT NULL,
  ended_at timestamptz,
  CONSTRAINT lasts CHECK (ended_at > started_at)
)";

/// Ended intervals by the instants they contain, and open ones by start.
/// A range index finds the intervals containing an instant without visiting
/// those that ended before it or began after it.
pub(crate) const INDEXES: &[&str] = &[
    "CREATE INDEX live_intervals_ended ON core.live_intervals USING gist (event_type, narrowing, tstzrange(started_at, ended_at, '[]')) WHERE ended_at IS NOT NULL",
    "CREATE INDEX live_intervals_open ON core.live_intervals (event_type, narrowing, started_at) WHERE ended_at IS NULL",
];

/// The interval kinds declared when this step landed, as
/// `(event type, end field, end rule, narrowing field)`. A kind declared
/// later is filled by its own step.
const KINDS_AT_UPGRADE: &[(&str, &str, &str, Option<&str>)] = &[
    ("activity.session", "ended_at", "at", None),
    ("activity.span", "ended_at", "at", None),
    ("activity.window", "ended_at", "at", None),
    ("command.executed", "duration_ns", "after_ns", None),
    ("command.fused", "duration_ns", "after_ns", None),
    ("command.reported", "duration_ns", "after_ns", None),
    ("health.record", "end", "at", Some("category")),
    ("input.coverage", "ended_at", "at", None),
    ("music.played", "ended_at", "at", None),
    ("presence.interval", "ended_at", "at", None),
    ("presence.span", "ended_at", "at", None),
    ("shell.session", "ended_at", "at", None),
    ("state.interval", "ended_at", "at", Some("state_kind")),
    ("window.span", "ended_at", "at", None),
    ("work.act", "ended_at", "end_or_instant", None),
];

/// Create the table and fill it from the live events of the interval kinds.
/// Only those kinds' rows are read, by their segments in compressed chunks;
/// no event row is written. The indexes are built after the fill.
pub(crate) fn upgrade() -> Vec<String> {
    let quoted = |text: &str| format!("'{text}'");
    let rows: Vec<String> = KINDS_AT_UPGRADE
        .iter()
        .map(|(event_type, field, rule, narrowed_by)| {
            format!(
                "({}, {}, {}, {})",
                quoted(event_type),
                quoted(field),
                quoted(rule),
                narrowed_by.map_or("NULL".into(), quoted)
            )
        })
        .collect();
    let event_types: Vec<String> = KINDS_AT_UPGRADE
        .iter()
        .map(|(event_type, ..)| quoted(event_type))
        .collect();
    // The inner query ends at OFFSET 0 so that liveness is joined to the five
    // stored columns: joined to the events it would carry every payload
    // through the join's work files.
    let fill = format!(
        "INSERT INTO core.live_intervals (event_id, event_type, narrowing, started_at, ended_at)
         SELECT s.id, s.event_type, s.narrowing, s.ts_orig, s.ended_at
         FROM (
           SELECT e.id, e.event_type, e.payload->>k.narrowed_by AS narrowing, e.ts_orig, i.ended_at
           FROM core.events e
           JOIN (VALUES {rows}) AS k(event_type, end_field, end_rule, narrowed_by)
             ON k.event_type = e.event_type
           CROSS JOIN LATERAL (
             SELECT core.interval_end(k.end_rule, e.ts_orig, e.payload->k.end_field) AS ended_at) i
           WHERE e.event_type IN ({event_types}) AND e.ts_orig IS NOT NULL
             AND (i.ended_at IS NULL OR i.ended_at > e.ts_orig)
           OFFSET 0) s
         WHERE NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = s.id)",
        rows = rows.join(", "),
        event_types = event_types.join(", "),
    );
    let mut statements = vec![
        // The range index needs text equality in a GiST index, which the
        // superuser's database preparation installs.
        "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'btree_gist') THEN
  RAISE EXCEPTION 'the btree_gist extension is missing; run sinexd admin prepare-database as the database superuser, then upgrade again';
END IF; END $$"
            .into(),
        INTERVAL_END.into(),
        TABLE.into(),
        fill,
    ];
    statements.extend(INDEXES.iter().map(|sql| (*sql).into()));
    statements
}

/// State intervals gained a bound: one whose end is unknown reaches only to
/// its fence. Bring the rows of the stored fenced intervals up to date, as
/// admission now writes them: the fence where it is after the start, no row
/// where it is not. Only `state.interval` rows are read, by their segments
/// in compressed chunks; no event row is written.
pub(crate) fn bound_fenced_intervals() -> Vec<String> {
    vec![
        "WITH fenced AS MATERIALIZED (
           SELECT e.id, core.payload_time(e.payload->>'fenced_at') AS fenced_at
           FROM core.events e
           WHERE e.event_type = 'state.interval' AND e.payload->>'fenced_at' IS NOT NULL
         ), instants AS (
           DELETE FROM core.live_intervals l USING fenced f
           WHERE l.event_id = f.id AND l.ended_at IS NULL AND f.fenced_at <= l.started_at
         )
         UPDATE core.live_intervals l SET ended_at = f.fenced_at
         FROM fenced f
         WHERE l.event_id = f.id AND l.ended_at IS NULL AND f.fenced_at > l.started_at"
            .into(),
    ]
}
