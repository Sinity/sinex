//! Forward upgrades preserve every row. The declarations carry the same
//! changes, so fresh and upgraded databases have the same schema.

/// The statements of each upgrade, by the version it starts from.
fn landed() -> Vec<Vec<String>> {
    vec![
        // 0 to 1: per-source and per-run counts, filled from the events
        // already stored in one read of them.
        vec![
            crate::description::version_four::SOURCE_COUNTS.into(),
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
            crate::description::version_four::EXCISED_MATERIALS.into(),
            crate::description::version_four::EXCISED_MATERIALS_BLOB.into(),
            "GRANT SELECT ON core.excised_materials TO sinex_reader, sinex_writer".into(),
            "GRANT INSERT, UPDATE, DELETE ON core.excised_materials TO sinex_writer".into(),
        ],
        // 2 to 3: coverage probes each run through one covering index.
        vec![
            "DROP INDEX IF EXISTS core.events_coverage".into(),
            crate::description::version_four::COVERAGE_RUNS.into(),
        ],
        // Version four added kind rows and views, installed from the current description.
        vec![],
    ]
}

/// Unlanded transformations have names; their position is assigned at landing.
pub(crate) const NAMED: &[&str] = &["blob_change_tracking", "library_scans", "kind_declarations"];

pub(crate) fn upgrades() -> Vec<Vec<String>> {
    let mut steps = landed();
    steps.push(crate::tables::blob_changes());
    steps.push(vec!["DO $$ BEGIN IF EXISTS (SELECT 1 FROM core.events WHERE event_type LIKE 'library.%') THEN RAISE EXCEPTION 'library records use request-created item IDs; decide how their references should follow deterministic item IDs before upgrading'; END IF; END $$".into()]);
    let before = crate::description::Description::version_four();
    let after = crate::description::Description::current();
    let mut statements =
        vec!["ALTER TABLE registry.event_types RENAME TO unversioned_event_types".into()];
    statements.extend(after.changes_from(&before, &["registry.event_types"]));
    steps.push(statements);
    steps
}

pub(crate) const LANDED_VERSION: i32 = 4;
