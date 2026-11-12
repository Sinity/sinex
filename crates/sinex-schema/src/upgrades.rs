//! Forward upgrades preserve every row. Named transformations take their order from this composed stack.
mod output_ordinals;
mod source_cursors;

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
        // 2 to 3 used a coverage index. Current coverage reads counters;
        // retain the version position without building a retired index.
        vec![],
        // Version four added kind rows and views, installed from the current description.
        vec![],
    ]
}

pub(crate) const LANDED_VERSION: i32 = 4;

/// The distinct event types stored, read by one probe of the
/// `(event_type, ts_orig)` index per type on uncompressed chunks instead of a
/// scan of every event.
pub(crate) const STORED_EVENT_TYPES: &str = "WITH RECURSIVE stored(event_type) AS (
   (SELECT event_type FROM core.events ORDER BY event_type LIMIT 1)
   UNION ALL
   SELECT (SELECT e.event_type FROM core.events e WHERE e.event_type > stored.event_type ORDER BY e.event_type LIMIT 1)
   FROM stored WHERE stored.event_type IS NOT NULL)
 SELECT event_type FROM stored WHERE event_type IS NOT NULL";
pub(crate) const NAMED: &[&str] = &[
    "blob_change_tracking",
    "library_scans",
    "compact_equivalence_keys",
    "search_text",
    "publication_pairs",
    "kind_declarations",
    "foundations",
    "segments",
    "confirmed_lifecycle_positions",
    "producer_reading_orders",
    "output_ordinals",
    "producer_families",
    "query_cursor_key",
    "embedding_projection",
    "signal_host_checkpoint",
    "withdrawn_privacy_outputs",
    "owner_protocol_key",
    "source_declarations",
    "withdrawn_model_accounting",
    "lifecycle",
    "event_indexes",
    "remove_privacy_outputs_and_model_accounting",
    "source_expectations",
    "stage_without_parser",
    "library_curation",
    "online_backup_holds",
    "search_refusals",
    "derivation_read_indexes",
    "search_position",
    "frame_outcomes",
    "blob_chunks",
    "append_file_stat_metadata",
    "search_trigram_text",
    "closed_tombstone_history",
];

#[cfg(test)]
pub(crate) fn upgrades() -> Vec<Vec<String>> {
    upgrades_with(false)
}

/// The upgrades, swapping in the equivalence-key copy `sinexd admin upgrade
/// --prepare` built when it is `prepared` (`prepare.rs`), and otherwise
/// removing any incomplete preparation and converting the keys in place.
pub(crate) fn upgrades_with(prepared: bool) -> Vec<Vec<String>> {
    let mut steps = landed();
    steps.push(crate::tables::blob_changes());
    steps.push(vec![format!("DO $$ BEGIN IF EXISTS (SELECT 1 FROM ({STORED_EVENT_TYPES}) stored WHERE event_type LIKE 'library.%') THEN RAISE EXCEPTION 'library records use request-created item IDs; decide how their references should follow deterministic item IDs before upgrading'; END IF; END $$")]);
    steps.push(if prepared {
        crate::prepare::swap()
    } else {
        [crate::prepare::removal(), compact_equivalence_keys()].concat()
    });

    steps.push(search_text());
    steps.push(publication_pairs());
    let before = crate::description::Description::before_kind_declarations();
    let after = crate::description::Description::before_foundations();
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
    steps.push(segments());
    steps.push(vec![
        "ALTER TABLE core.event_tombstones ADD COLUMN commit_seq bigint".into(),
        "ALTER TABLE core.event_tombstones ADD COLUMN original_commit_seq bigint".into(),
        "ALTER TABLE core.event_tombstones ADD COLUMN class text".into(),
        "CREATE INDEX archive_changes_feed ON audit.archive_changes (commit_seq,change_seq,event_id)".into(),
        "CREATE INDEX tombstones_feed ON core.event_tombstones (commit_seq,event_id)".into(),
        "CREATE INDEX tombstones_original_position ON core.event_tombstones (original_commit_seq)".into(),
        "ALTER TABLE core.published ADD COLUMN stream_instance text NOT NULL DEFAULT ''".into(),
    ]);
    // The feed index and the publication key are built after start (`online.rs`).
    let positions = steps.last_mut().expect("the step was just added");
    positions.extend(crate::online::begin(crate::online::Task::FeedIndex));
    positions.extend(crate::online::begin(crate::online::Task::PublishedKey));
    steps.push(vec![
        crate::tables::PRODUCER_ORDERS.into(),
        "GRANT SELECT, INSERT, UPDATE, DELETE ON core.producer_orders TO sinex_writer".into(),
    ]);
    steps.push(output_ordinals::statements());
    steps.push(vec![
        "ALTER TABLE core.ingress_frames ADD COLUMN family text".into(),
        "ALTER TABLE core.ingress_frames ADD COLUMN write_kind_prefixes text[]".into(),
    ]);
    let mut cursor_step = vec![
        crate::tables::QUERY_CURSOR_KEY.into(),
        crate::tables::QUERY_CURSOR_KEY_INIT.into(),
        crate::query_cursors::INIT.into(),
        "GRANT SELECT ON core.query_cursor_key TO sinex_writer".into(),
        "REVOKE ALL ON core.query_cursor_key FROM sinex_reader".into(),
    ];
    cursor_step.extend(crate::query_cursors::triggers());
    steps.push(cursor_step);
    steps.push(crate::vectors::statements());
    steps.push(crate::signals::statements());
    // Empty positions keep later version numbers; the last step removes what they created.
    steps.push(vec![]);
    steps.push(crate::owner_protocol::statements());
    steps.push(crate::source_declarations::statements());
    steps.push(vec![]);
    steps.push(crate::lifecycle::statements());
    steps.push(crate::event_indexes::statements());
    steps.push(remove_privacy_outputs_and_model_accounting());
    steps.push(crate::source_expectations::statements());
    steps.push(crate::stage_without_parser::statements());
    steps.push(crate::library_curation::statements());
    steps.push(crate::backups::statements());
    steps.push(vec![crate::tables::SEARCH_REFUSALS.into()]);
    steps.push(vec![
        crate::tables::REVISIONS_BY_INPUT.into(),
        crate::tables::EVENTS_COMMIT_SEQ_SKIPPING.into(),
    ]);
    steps.push(vec![
        crate::tables::SEARCH_POSITION.into(),
        "INSERT INTO core.search_position (commit_seq)
         SELECT commit_seq FROM core.consumer_positions WHERE consumer = 'read.search_text'"
            .into(),
        "DELETE FROM core.consumer_positions WHERE consumer = 'read.search_text'".into(),
    ]);
    steps.push(crate::frame_outcomes::statements());
    steps.push(crate::blob_chunks::statements());
    steps.push(source_cursors::statements());
    steps.push(crate::search_trigram::statements());
    steps.push(crate::tombstone_history::statements());
    steps
}

/// The conversion's correctness is a property of the two functions, tested
/// against them: decoding an encoded key returns it, and each encoding
/// starts with its form's tag byte, so distinct keys stay distinct. The
/// rewrite rebuilds the key's primary key, which would refuse a collision.
fn compact_equivalence_keys() -> Vec<String> {
    vec![
        crate::tables::ENCODE_EQUIVALENCE_KEY.into(),
        crate::tables::DECODE_EQUIVALENCE_KEY.into(),
        "ALTER TABLE core.live_equivalence ALTER COLUMN equivalence_key TYPE bytea USING core.encode_equivalence_key(equivalence_key)".into(),
    ]
}

/// The key over publication pairs is built once, after start, together with
/// the stream instance the later confirmed-stream step adds (`online.rs`).
fn publication_pairs() -> Vec<String> {
    vec![]
}

/// The historical search-column step. The later trigram table transformation
/// removes this column without rewriting the full-text heap.
fn search_text() -> Vec<String> {
    vec![
        "ALTER TABLE core.search_text ADD COLUMN trigram_indexed boolean NOT NULL DEFAULT true"
            .into(),
        "CREATE OR REPLACE VIEW core.visible_search_text AS
         SELECT s.event_id, s.text, s.tsv, s.trigram_indexed
         FROM core.search_text s JOIN core.events e ON e.id = s.event_id
         WHERE core.class_visible(e.class)"
            .into(),
    ]
}

/// Move archive edges and counters into ordinary store tables without dropping captured telemetry.
fn foundations() -> Vec<String> {
    let mut steps = vec![
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM telemetry.events) THEN RAISE EXCEPTION 'the telemetry table holds records; preserve them before upgrading'; END IF; END $$".into(),
        "DROP TABLE telemetry.events".into(),
        "DROP SCHEMA telemetry".into(),
        "INSERT INTO core.counters (name, value) SELECT 'excised', count(*) FROM core.event_tombstones ON CONFLICT DO NOTHING".into(),
    ];
    // The edges of stored events are filled after start (`online.rs`).
    steps.extend(crate::online::begin(crate::online::Task::EventChildren));
    steps.extend(crate::description::Description::producer_metadata_changes());
    steps
}

fn segments() -> Vec<String> {
    vec![
        crate::tables::BLOB_SEGMENTS.into(),
        crate::tables::BLOB_EXTENTS.into(),
        "GRANT SELECT ON core.blob_segments, core.blob_extents TO sinex_reader, sinex_writer"
            .into(),
        "GRANT INSERT, UPDATE, DELETE ON core.blob_segments, core.blob_extents TO sinex_writer"
            .into(),
    ]
}

/// A database that ran the two emptied steps drops what they created. The
/// visibility views are first redefined by class alone, so nothing that reads
/// them depends on the removed function; `Description::apply` re-creates them
/// afterwards as declared.
fn remove_privacy_outputs_and_model_accounting() -> Vec<String> {
    vec![
        crate::VISIBLE_EVENTS.replacen("CREATE VIEW", "CREATE OR REPLACE VIEW", 1),
        crate::search_trigram::PREVIOUS_VIEW.replacen("CREATE VIEW", "CREATE OR REPLACE VIEW", 1),
        crate::VISIBLE_EMBEDDING_VECTORS.replacen("CREATE VIEW", "CREATE OR REPLACE VIEW", 1),
        "DROP VIEW IF EXISTS core.derivation_events CASCADE".into(),
        "DROP TRIGGER IF EXISTS rules_policy_epoch ON privacy.rules".into(),
        "DROP TRIGGER IF EXISTS floors_policy_epoch ON privacy.source_classes".into(),
        "DROP FUNCTION IF EXISTS privacy.invalidate_outputs() CASCADE".into(),
        "DROP FUNCTION IF EXISTS privacy.require_active_epoch() CASCADE".into(),
        "DROP TABLE IF EXISTS privacy.event_outputs, privacy.original_events, privacy.output_builds, privacy.derivation_contracts, privacy.derivation_epochs, privacy.policy_state CASCADE".into(),
        "DELETE FROM core.counters WHERE name = 'privacy_outputs'".into(),
        "DROP TABLE IF EXISTS derivation.model_calls, derivation.model_policies CASCADE".into(),
    ]
}
