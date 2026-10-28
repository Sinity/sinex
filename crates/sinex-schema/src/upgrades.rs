//! Forward upgrades preserve every row. Named transformations take their order from this composed stack.
fn landed() -> Vec<Vec<String>> {
    vec![
        // 0 to 1: per-source and per-run counts, filled from the events
        // already stored in one read of them.
        vec![
            crate::description::version_nine::SOURCE_COUNTS.into(),
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
            crate::description::version_nine::EXCISED_MATERIALS.into(),
            crate::description::version_nine::EXCISED_MATERIALS_BLOB.into(),
            "GRANT SELECT ON core.excised_materials TO sinex_reader, sinex_writer".into(),
            "GRANT INSERT, UPDATE, DELETE ON core.excised_materials TO sinex_writer".into(),
        ],
        // 2 to 3: coverage probes each run through one covering index.
        vec![
            "DROP INDEX IF EXISTS core.events_coverage".into(),
            crate::description::version_nine::COVERAGE_RUNS.into(),
        ],
        // Version four added kind rows and views, installed from the current description.
        vec![],
    ]
}

pub(crate) const LANDED_VERSION: i32 = 4;
pub(crate) const NAMED: &[&str] = &[
    "blob_change_tracking",
    "library_scans",
    "compact_equivalence_keys",
    "search_text",
    "publication_pairs",
    "kind_declarations",
    "foundations",
];

pub(crate) fn upgrades() -> Vec<Vec<String>> {
    let mut steps = landed();
    steps.push(crate::tables::blob_changes());
    steps.push(vec!["DO $$ BEGIN IF EXISTS (SELECT 1 FROM core.events WHERE event_type LIKE 'library.%') THEN RAISE EXCEPTION 'library records use request-created item IDs; decide how their references should follow deterministic item IDs before upgrading'; END IF; END $$".into()]);
    steps.push(compact_equivalence_keys());
    steps.push(search_text());
    steps.push(publication_pairs());
    let before = crate::description::Description::before_kind_declarations();
    let after = crate::description::Description::current();
    let mut statements = vec![
        "ALTER TABLE registry.event_types RENAME TO unversioned_event_types".into(),
        "DO $$ DECLARE c record; BEGIN
           FOR c IN SELECT oid,conname FROM pg_constraint WHERE conrelid='registry.unversioned_event_types'::regclass LOOP
             EXECUTE format('ALTER TABLE registry.unversioned_event_types RENAME CONSTRAINT %I TO %I',c.conname,'unversioned_event_types_'||c.oid);
           END LOOP;
         END $$".into(),
    ];
    statements.extend(after.changes_from(&before, &["registry.event_types"]));
    steps.push(statements);
    steps.push(foundations());
    steps
}

fn compact_equivalence_keys() -> Vec<String> {
    vec![
            crate::tables::ENCODE_EQUIVALENCE_KEY.into(),
            crate::tables::DECODE_EQUIVALENCE_KEY.into(),
            "CREATE TEMPORARY TABLE live_keys_before ON COMMIT DROP AS TABLE core.live_equivalence".into(),
            "DO $$ BEGIN
               IF EXISTS (SELECT 1 FROM core.live_equivalence
                 WHERE core.decode_equivalence_key(core.encode_equivalence_key(equivalence_key)) IS DISTINCT FROM equivalence_key)
               THEN RAISE EXCEPTION 'equivalence key encoding does not round trip'; END IF;
               IF EXISTS (SELECT 1 FROM core.live_equivalence GROUP BY source, event_type, core.encode_equivalence_key(equivalence_key) HAVING count(*) > 1)
               THEN RAISE EXCEPTION 'equivalence key encoding changes identity'; END IF;
             END $$".into(),
            "ALTER TABLE core.live_equivalence ALTER COLUMN equivalence_key TYPE bytea USING core.encode_equivalence_key(equivalence_key)".into(),
            "DO $$ BEGIN
               IF EXISTS (
                 (SELECT source, event_type, core.decode_equivalence_key(equivalence_key), event_id, reading_order, record_hash FROM core.live_equivalence
                  EXCEPT ALL SELECT * FROM live_keys_before)
                 UNION ALL
                 (SELECT * FROM live_keys_before EXCEPT ALL
                  SELECT source, event_type, core.decode_equivalence_key(equivalence_key), event_id, reading_order, record_hash FROM core.live_equivalence))
               THEN RAISE EXCEPTION 'equivalence key conversion changed stored rows'; END IF;
             END $$".into(),
        ]
}

fn publication_pairs() -> Vec<String> {
    vec![
        "ALTER TABLE core.published DROP CONSTRAINT published_pkey".into(),
        "ALTER TABLE core.published ADD CONSTRAINT published_pairs_pkey PRIMARY KEY (event_id, stream_seq)".into(),
    ]
}

fn search_text() -> Vec<String> {
    let excluded = sinex_model::event_types::ALL
        .iter()
        .filter(|kind| !kind.trigram_indexed)
        .map(|kind| format!("'{}'", kind.event_type.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ");
    vec![
        "ALTER TABLE core.search_text ADD COLUMN trigram_indexed boolean NOT NULL DEFAULT true".into(),
        format!("UPDATE core.search_text s SET trigram_indexed = false
                 FROM core.events e WHERE e.id = s.event_id AND e.event_type IN ({excluded})"),
        "DO $$ DECLARE missing bigint; BEGIN
           SELECT count(*) INTO missing FROM core.search_text s
           WHERE NOT EXISTS (SELECT 1 FROM core.events e WHERE e.id = s.event_id);
           IF missing > 0 THEN
             RAISE NOTICE '% search rows have no event; their trigram eligibility stays true', missing;
           END IF;
         END $$".into(),
        "CREATE OR REPLACE VIEW core.visible_search_text AS
         SELECT s.event_id, s.text, s.tsv, s.trigram_indexed
         FROM core.search_text s JOIN core.events e ON e.id = s.event_id
         WHERE core.class_visible(e.class)".into(),
        "CREATE INDEX search_text_trigram_partial ON core.search_text USING gin (text gin_trgm_ops) WHERE trigram_indexed".into(),
        "DROP INDEX core.search_text_trigram".into(),
        "ALTER INDEX core.search_text_trigram_partial RENAME TO search_text_trigram".into(),
    ]
}

/// Move archive edges and counters into ordinary store tables without dropping captured telemetry.
fn foundations() -> Vec<String> {
    vec![
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM telemetry.events) THEN RAISE EXCEPTION 'the telemetry table holds records; preserve them before upgrading'; END IF; END $$".into(),
        "DROP TABLE telemetry.events".into(),
        "DROP SCHEMA telemetry".into(),
        "INSERT INTO core.counters (name, value) SELECT 'excised', count(*) FROM core.event_tombstones ON CONFLICT DO NOTHING".into(),
        "INSERT INTO core.event_children (parent, child) SELECT DISTINCT unnest(parents), id FROM core.events WHERE parents IS NOT NULL ON CONFLICT DO NOTHING".into(),
    ]
}
