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
            "ALTER TABLE core.published RENAME TO published_events".into(),
            "ALTER TABLE core.published_events RENAME CONSTRAINT published_pkey TO published_events_pkey".into(),
            crate::tables::PUBLISHED.into(),
            crate::tables::PUBLISHED_BOUNDS.into(),
            "INSERT INTO core.published (first_event, last_event, event_ids, stream_seqs)
             SELECT (array_agg(event_id ORDER BY event_id))[1],
                    (array_agg(event_id ORDER BY event_id DESC))[1],
                    array_agg(event_id ORDER BY event_id), array_agg(stream_seq ORDER BY event_id)
             FROM (SELECT event_id, stream_seq, (row_number() OVER (ORDER BY event_id) - 1) / 1000 AS page
                   FROM core.published_events) p GROUP BY page ORDER BY page".into(),
            "DO $$ BEGIN
               IF EXISTS (
                 (SELECT event_id, stream_seq FROM core.published_events
                  EXCEPT ALL
                  SELECT m.event_id, m.stream_seq FROM core.published p,
                    LATERAL unnest(p.event_ids, p.stream_seqs) m(event_id, stream_seq))
                 UNION ALL
                 (SELECT m.event_id, m.stream_seq FROM core.published p,
                    LATERAL unnest(p.event_ids, p.stream_seqs) m(event_id, stream_seq)
                  EXCEPT ALL
                  SELECT event_id, stream_seq FROM core.published_events)
               ) THEN RAISE EXCEPTION 'published event/stream pairs changed during upgrade'; END IF;
             END $$".into(),
            "GRANT SELECT ON core.published TO sinex_reader, sinex_writer".into(),
            "GRANT INSERT, UPDATE, DELETE ON core.published TO sinex_writer".into(),
            "GRANT USAGE ON SEQUENCE core.published_id_seq TO sinex_writer".into(),
            "DROP TABLE core.published_events".into(),
        ],
    ]
}
