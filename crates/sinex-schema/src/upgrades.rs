//! Forward upgrades preserve every row. The declarations carry the same
//! changes, so fresh and upgraded databases have the same schema.

pub(crate) const STEPS: &[fn() -> Vec<String>] = &[
    source_counts,
    whole_material_excision,
    coverage_runs,
    marks_and_saved_questions,
    blob_change_tracking,
    library_scans,
    compact_equivalence_keys,
    search_text,
    publication_pages,
];

/// Landing order determines each step's schema version.
pub(crate) fn upgrades() -> Vec<Vec<String>> {
    STEPS.iter().map(|step| step()).collect()
}

fn source_counts() -> Vec<String> {
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
    ]
}

fn whole_material_excision() -> Vec<String> {
    vec![
        crate::tables::EXCISED_MATERIALS.into(),
        crate::tables::EXCISED_MATERIALS_BLOB.into(),
        "GRANT SELECT ON core.excised_materials TO sinex_reader, sinex_writer".into(),
        "GRANT INSERT, UPDATE, DELETE ON core.excised_materials TO sinex_writer".into(),
    ]
}

fn coverage_runs() -> Vec<String> {
    vec![
        "DROP INDEX IF EXISTS core.events_coverage".into(),
        crate::tables::COVERAGE_RUNS.into(),
    ]
}

fn marks_and_saved_questions() -> Vec<String> {
    register_kinds(&["operator.mark", "query.saved"], false)
}

fn blob_change_tracking() -> Vec<String> {
    crate::tables::blob_changes()
}

/// Register payload schemas and views together. Replacing a kind also updates its registry row.
pub(crate) fn register_kinds(names: &[&str], replace: bool) -> Vec<String> {
    let mut statements = Vec::new();
    for name in names {
        assert!(
            sinex_model::event_types::ALL
                .iter()
                .any(|kind| kind.event_type == *name),
            "unknown event type {name}"
        );
    }
    for kind in sinex_model::event_types::ALL
        .iter()
        .filter(|k| names.contains(&k.event_type))
    {
        if replace {
            statements.push(format!(
                "DROP VIEW IF EXISTS event_types.{}",
                crate::quote_ident(&kind.event_type.replace('.', "_"))
            ));
        }
        statements.push(crate::kind_view(kind));
        statements.push(format!(
            "GRANT SELECT ON event_types.{} TO sinex_reader, sinex_writer",
            crate::quote_ident(&kind.event_type.replace('.', "_"))
        ));
    }
    for (name, schema, key_kind, fields, rule) in crate::registry_rows()
        .into_iter()
        .filter(|(name, ..)| names.contains(&name.as_str()))
    {
        let literal = |s: &str| format!("'{}'", s.replace('\'', "''"));
        let fields = fields
            .iter()
            .map(|s| literal(s))
            .collect::<Vec<_>>()
            .join(",");
        let conflict = if replace {
            " ON CONFLICT (name) DO UPDATE SET payload_schema=EXCLUDED.payload_schema, key_kind=EXCLUDED.key_kind, key_fields=EXCLUDED.key_fields, replace_rule=EXCLUDED.replace_rule"
        } else {
            ""
        };
        statements.push(format!("INSERT INTO registry.event_types (name,payload_schema,key_kind,key_fields,replace_rule) VALUES ({},{}::jsonb,{},ARRAY[{fields}]::text[],{}){conflict}",literal(&name),literal(&schema.to_string()),literal(key_kind),literal(rule)));
    }
    statements
}

/// Request-created identifiers need an explicit decision about their references.
fn library_scans() -> Vec<String> {
    let mut statements = vec![
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM core.events WHERE event_type LIKE 'library.%') THEN
         RAISE EXCEPTION 'library records use request-created item IDs; decide how their references should follow deterministic item IDs before upgrading';
         END IF; END $$".into(),
    ];
    let names: Vec<_> = sinex_model::event_types::ALL
        .iter()
        .filter(|kind| kind.event_type.starts_with("library."))
        .map(|kind| kind.event_type)
        .collect();
    statements.extend(register_kinds(&names, true));
    statements
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

fn publication_pages() -> Vec<String> {
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
