//! Forward upgrades of a deployed database. `upgrades()[n]` takes a database
//! at version `n` to `n + 1` and keeps every row; the declarations carry the
//! same change, so a fresh database is created at [`crate::VERSION`]
//! directly and matches an upgraded one.

/// The statements of each upgrade, by the version it starts from.
pub(crate) fn upgrades() -> Vec<Vec<String>> {
    vec![
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
        ],
        search_text(),
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
