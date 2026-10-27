//! Forward upgrades of a deployed database. `upgrades()[n]` takes a database
//! at version `n` to `n + 1` and keeps every row; the declarations carry the
//! same change, so a fresh database is created at [`crate::VERSION`]
//! directly and matches an upgraded one.

/// The statements of each upgrade, by the version it starts from.
pub(crate) const LANDED: i32 = 4;
pub(crate) const PENDING: &[&str] = &["versioned-kind-declarations"];

pub(crate) fn upgrades() -> Vec<Vec<String>> {
    let mut steps = vec![
        // 0 to 1: per-source and per-run counts, filled from the events
        // already stored in one read of them.
        vec![
            crate::tables::SOURCE_COUNTS.into(),
            "ALTER TABLE core.runs ADD COLUMN events bigint NOT NULL DEFAULT 0".into(),
            "CREATE TEMPORARY TABLE stored_counts ON COMMIT DROP AS
             SELECT e.source, e.run_id, count(*) AS events,
               count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)) AS live,
               min(e.ts_orig) AS first_event, max(e.ts_orig) AS last_event, max(e.ts_coided) AS last_seen
             FROM core.events e GROUP BY e.source, e.run_id"
                .into(),
            "INSERT INTO core.source_counts (source, events, first_event, last_event, last_seen)
             SELECT source, sum(live), min(first_event), max(last_event), max(last_seen)
             FROM stored_counts GROUP BY source"
                .into(),
            "UPDATE core.runs r SET events = c.events
             FROM (SELECT run_id, sum(events) AS events FROM stored_counts GROUP BY run_id) c
             WHERE r.id = c.run_id"
                .into(),
            "GRANT SELECT ON core.source_counts TO sinex_reader, sinex_writer".into(),
            "GRANT INSERT, UPDATE, DELETE ON core.source_counts TO sinex_writer".into(),
        ],
        // 1 to 2: excision removes whole materials and keeps only their
        // digests. Materials an earlier excision blanked to spaces stay as
        // they are.
        vec![
            crate::tables::EXCISED_MATERIALS.into(),
            crate::tables::EXCISED_MATERIALS_BLOB.into(),
            "GRANT SELECT ON core.excised_materials TO sinex_reader, sinex_writer".into(),
            "GRANT INSERT, UPDATE, DELETE ON core.excised_materials TO sinex_writer".into(),
        ],
        // 2 to 3: coverage probes each run through one covering index.
        vec![
            "DROP INDEX IF EXISTS core.events_coverage".into(),
            crate::tables::COVERAGE_RUNS.into(),
        ],
        query_records(),
    ];
    for name in PENDING {
        steps.push(match *name {
            "versioned-kind-declarations" => versioned_kind_declarations(),
            _ => unreachable!("declared schema step"),
        });
    }
    steps
}

fn versioned_kind_declarations() -> Vec<String> {
    vec![
            "ALTER TABLE core.events ADD COLUMN kind_version bigint NOT NULL DEFAULT 1 CHECK (kind_version > 0), ADD COLUMN validation text NOT NULL DEFAULT 'unvalidated' CHECK (validation IN ('validated', 'unvalidated'))".into(),
            "ALTER TABLE telemetry.events ADD COLUMN kind_version bigint NOT NULL DEFAULT 1 CHECK (kind_version > 0), ADD COLUMN validation text NOT NULL DEFAULT 'unvalidated' CHECK (validation IN ('validated', 'unvalidated'))".into(),
            "ALTER TABLE core.admission_stage ADD COLUMN kind_version bigint NOT NULL DEFAULT 1, ADD COLUMN validation text NOT NULL DEFAULT 'unvalidated'".into(),
            "ALTER TABLE core.admission_stage ALTER COLUMN kind_version DROP DEFAULT, ALTER COLUMN validation DROP DEFAULT".into(),
            "ALTER TABLE registry.event_types RENAME TO unversioned_event_types".into(),
            crate::tables::KIND_PREFIXES.into(),
            crate::tables::KIND_DECLARATIONS.into(),
            crate::tables::REPLAY_REFUSALS.into(),
            crate::tables::KIND_IMMUTABILITY.into(),
    ]
}

// The deployed unversioned registry gains these names before it is upgraded.
fn query_records() -> Vec<String> {
    use sinex_model::event_types::query::{Mark, Saved};
    use sinex_model::EventPayload;
    let literal = |s: &str| format!("'{}'", s.replace('\'', "''"));
    [("operator.mark", Mark::json_schema(), "location", "ARRAY[]::text[]", sinex_model::ReplaceRule::SuppressRepeat.as_db()),
     ("query.saved", Saved::json_schema(), "fields", "ARRAY['name','by']::text[]", sinex_model::ReplaceRule::ReplaceWhenNewer.as_db())]
        .into_iter()
        .map(|(name, schema, key, fields, rule)| format!(
            "INSERT INTO registry.event_types (name,payload_schema,key_kind,key_fields,replace_rule) VALUES ({},{}::jsonb,{}, {}, {})",
            literal(name), literal(&schema.to_string()), literal(key), fields, literal(rule)))
        .collect()
}
