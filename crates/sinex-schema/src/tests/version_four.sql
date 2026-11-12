-- Undo the later receipt access paths.
DROP INDEX core.ingress_frames_material, core.ingress_frames_refused_at, core.ingress_frames_malformed_producer;
-- Reconstruct the predecessor before the closed-history constraint.
ALTER TABLE core.event_tombstones DROP CONSTRAINT IF EXISTS tombstone_history_complete;
-- Undo the separate trigram read model before the older synthetic downgrades.
DROP VIEW core.visible_search_trigram;
DROP TABLE core.search_trigram;
DROP VIEW core.visible_search_text;
ALTER TABLE core.search_text ADD COLUMN trigram_indexed boolean NOT NULL DEFAULT true;
CREATE INDEX search_text_trigram ON core.search_text USING gin (text gin_trgm_ops) WHERE trigram_indexed;
CREATE VIEW core.visible_search_text AS SELECT s.event_id,s.text,s.tsv,s.trigram_indexed
FROM core.search_text s JOIN core.events e ON e.id=s.event_id WHERE core.class_visible(e.class);
GRANT SELECT ON core.visible_search_text TO sinex_reader,sinex_writer;
-- Undo the blob chunks upgrade.
DROP FUNCTION core.blob_descriptors(text);
DROP TABLE core.blob_chunks;
-- Undo the search position upgrade.
DROP TABLE core.search_position;
-- Undo the search refusals upgrade.
DROP TABLE core.search_refusals;
-- Undo the online backup upgrade before exercising its forward application.
DROP FUNCTION core.backup_covers(text,bigint);
DROP TABLE core.backup_segments, core.backup_holds;
-- Undo the library curation upgrade.
DROP TABLE core.library_shown;
-- Undo parser-free staging before exercising the forward upgrade.
ALTER TABLE raw.materials DROP CONSTRAINT parser_with_version,
  ALTER COLUMN parser SET NOT NULL, ALTER COLUMN parser_version SET NOT NULL;
-- Undo the lifecycle upgrade.
DROP TABLE raw.material_changes;
ALTER TABLE raw.materials DROP COLUMN status, DROP COLUMN status_operation;
ALTER TABLE core.operations DROP COLUMN start_seq, DROP COLUMN end_seq, DROP COLUMN result, DROP COLUMN counts, DROP COLUMN progress, DROP COLUMN refusals;
ALTER TABLE derivation.dirty DROP COLUMN held_by;
ALTER TABLE core.event_tombstones DROP COLUMN ts_quality, DROP COLUMN reason, DROP COLUMN material_id, DROP COLUMN anchor, DROP COLUMN output_index, DROP COLUMN key_hash;
DROP INDEX audit.archived_operation, audit.archive_changes_restored;
ALTER TABLE audit.archived DROP CONSTRAINT archived_reason_check;
UPDATE audit.archived SET reason = CASE reason WHEN 'archive' THEN 'hide' ELSE 'recomputed' END WHERE reason IN ('archive', 'version_change');
ALTER TABLE audit.archived ADD CONSTRAINT archived_reason_check CHECK (reason IN ('replay', 'newer_reading', 'hide', 'recomputed'));
ALTER TABLE audit.archive_changes DROP CONSTRAINT archive_changes_reason_check, DROP CONSTRAINT archive_changes_check;
UPDATE audit.archive_changes SET reason = CASE reason WHEN 'archive' THEN 'hide' WHEN 'restore' THEN 'unhide' ELSE 'recomputed' END WHERE reason IN ('archive', 'restore', 'version_change');
ALTER TABLE audit.archive_changes ADD CONSTRAINT archive_changes_reason_check CHECK (reason IN ('replay', 'newer_reading', 'hide', 'recomputed', 'unhide')), ADD CONSTRAINT archive_changes_check CHECK (archived = (reason <> 'unhide'));
ALTER TABLE core.blobs DROP CONSTRAINT blobs_status_check, ADD CONSTRAINT blobs_status_check CHECK (status IN ('present', 'deleting'));
UPDATE core.counters SET name = 'excised' WHERE name = 'tombstoned';
CREATE TABLE core.hidden_records (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  source text NOT NULL,
  event_type text NOT NULL,
  equivalence_key text,
  material_id uuid,
  anchor bigint,
  output_index bigint,
  operation_id uuid NOT NULL,
  hidden_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT output_index_in_range CHECK (output_index >= 0),
  CONSTRAINT keyed_or_located CHECK ((equivalence_key IS NOT NULL AND material_id IS NULL AND anchor IS NULL AND output_index IS NULL) OR (equivalence_key IS NULL AND material_id IS NOT NULL AND anchor IS NOT NULL AND output_index IS NOT NULL))
);
CREATE UNIQUE INDEX hidden_by_key ON core.hidden_records (source, event_type, equivalence_key) WHERE equivalence_key IS NOT NULL;
CREATE UNIQUE INDEX hidden_by_location ON core.hidden_records (material_id, anchor, event_type, output_index) WHERE material_id IS NOT NULL;
CREATE TABLE core.excised_materials (
  material_id uuid PRIMARY KEY,
  blob text NOT NULL,
  operation_id uuid NOT NULL
);
CREATE INDEX excised_materials_blob ON core.excised_materials (blob);
CREATE OR REPLACE FUNCTION core.event_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'UPDATE' THEN
    RAISE EXCEPTION 'event rows are immutable' USING ERRCODE = '23514';
  END IF;
  IF TG_OP = 'DELETE' AND nullif(current_setting('sinex.excise_operation', true), '') IS NULL THEN
    RAISE EXCEPTION 'events are deleted only by an excision' USING ERRCODE = '23514';
  END IF;
  RETURN OLD;
END $$;
ALTER TABLE core.events DROP CONSTRAINT events_producer_frame;
DROP TABLE registry.source_declarations;
DROP FUNCTION registry.keep_source_declaration();
DROP TABLE core.owner_protocol_key;
DROP TABLE core.signal_checkpoints;
DROP FUNCTION core.advance_query_metadata() CASCADE;
DELETE FROM core.counters WHERE name='query_metadata';
DROP VIEW core.visible_embedding_vectors;
DROP TABLE core.embedding_vectors,core.embedding_spaces,core.query_cursor_key;
ALTER TABLE core.ingress_frames DROP COLUMN family, DROP COLUMN write_kind_prefixes;
-- Reconstruct the deployed predecessor before exercising the forward chain.
DROP INDEX core.events_record_location;
CREATE INDEX ON core.events (ts_orig);
CREATE INDEX ON core.events (source, ts_orig);
CREATE INDEX ON core.events (material_id, anchor);
CREATE INDEX events_coverage ON core.events (source, run_id, ts_orig) INCLUDE (commit_seq);
ALTER TABLE core.commit_log ADD COLUMN event_id uuid, ADD COLUMN event_type text,
  ADD CONSTRAINT commit_log_check CHECK ((event_id IS NULL) = (event_type IS NULL));
CREATE SCHEMA telemetry;
CREATE TABLE telemetry.events (LIKE core.events INCLUDING ALL);
DROP FUNCTION core.event_children_of(uuid), core.event_parents_of(uuid);
DROP TABLE core.event_children;
ALTER TABLE core.event_tombstones DROP COLUMN commit_seq, DROP COLUMN original_commit_seq, DROP COLUMN class;
DROP TABLE core.producer_orders;
DROP INDEX core.events_feed, audit.archive_changes_feed;
ALTER TABLE derivation.waiting DROP COLUMN refresh_at;
ALTER TABLE registry.derivations ALTER COLUMN version TYPE integer, DROP COLUMN build;
ALTER TABLE derivation.comparisons ALTER COLUMN version TYPE integer;
ALTER TABLE derivation.comparisons DROP COLUMN context;
DROP TABLE core.blob_extents, core.blob_segments;
DELETE FROM core.counters WHERE name='excised';
DROP VIEW core.visible_events CASCADE;
ALTER TABLE core.events DROP COLUMN producer_family,DROP COLUMN producer_identity,DROP COLUMN producer_host,DROP COLUMN producer_frame,DROP COLUMN source_version,DROP COLUMN source_fingerprint;
ALTER TABLE core.ingress_frames DROP COLUMN source_name,DROP COLUMN source_version,DROP COLUMN source_fingerprint,DROP COLUMN producer_host;
ALTER TABLE core.events ALTER COLUMN output_index TYPE smallint;
ALTER TABLE telemetry.events ALTER COLUMN output_index TYPE smallint;
ALTER TABLE core.live_locations ALTER COLUMN output_index TYPE smallint, DROP CONSTRAINT output_index_in_range;
ALTER TABLE core.hidden_records ALTER COLUMN output_index TYPE smallint, DROP CONSTRAINT output_index_in_range;
ALTER TABLE core.admission_stage ALTER COLUMN output_index TYPE smallint, ALTER COLUMN ord TYPE integer,
  DROP CONSTRAINT admission_stage_output_index_check, DROP CONSTRAINT admission_stage_ord_check;
ALTER TABLE core.events DROP COLUMN kind_version, DROP COLUMN validation;
ALTER TABLE telemetry.events DROP COLUMN kind_version, DROP COLUMN validation;
ALTER TABLE core.admission_stage DROP COLUMN kind_version, DROP COLUMN validation;
DROP TABLE core.replay_refusals;
DROP TABLE registry.kind_prefixes;
CREATE TEMP TABLE test_declarations AS SELECT * FROM registry.event_types;
DROP TABLE registry.event_types;
CREATE TABLE registry.event_types (name text PRIMARY KEY, payload_schema jsonb, key_fields text[], replace_rule text NOT NULL);

INSERT INTO registry.event_types SELECT DISTINCT ON (name) name,payload_schema,ARRAY[]::text[],replace_rule FROM test_declarations ORDER BY name,version DESC;
DROP TABLE test_declarations;
DROP FUNCTION registry.keep_definition();
ALTER TABLE core.live_equivalence ALTER COLUMN equivalence_key TYPE text USING core.decode_equivalence_key(equivalence_key);
DROP FUNCTION core.encode_equivalence_key(text), core.decode_equivalence_key(bytea);
DROP VIEW core.visible_search_text;
ALTER TABLE core.search_text RENAME TO search_text_current;
ALTER TABLE core.search_text_current RENAME CONSTRAINT search_text_pkey TO search_text_current_pkey;
ALTER TABLE core.search_text_current RENAME CONSTRAINT search_text_event_id_not_null TO search_text_current_event_id_not_null;
ALTER TABLE core.search_text_current RENAME CONSTRAINT search_text_text_not_null TO search_text_current_text_not_null;
DROP INDEX core.search_text_trigram;
DROP INDEX core.search_text_tsv;
CREATE TABLE core.search_text (event_id uuid PRIMARY KEY, text text NOT NULL, tsv tsvector GENERATED ALWAYS AS (to_tsvector('simple',text)) STORED);
INSERT INTO core.search_text (event_id,text) SELECT event_id,text FROM core.search_text_current;
DROP TABLE core.search_text_current;
CREATE INDEX search_text_tsv ON core.search_text USING gin (tsv);
CREATE INDEX search_text_trigram ON core.search_text USING gin (text gin_trgm_ops);
GRANT SELECT, INSERT, UPDATE, DELETE ON core.search_text TO sinex_writer;
CREATE VIEW core.visible_search_text AS SELECT s.event_id, s.text, s.tsv FROM core.search_text s JOIN core.events e ON e.id=s.event_id WHERE core.class_visible(e.class);
GRANT SELECT ON core.visible_search_text TO sinex_reader, sinex_writer;
GRANT INSERT, UPDATE, DELETE ON core.visible_search_text TO sinex_writer;
ALTER TABLE core.published DROP CONSTRAINT published_pairs_pkey;
ALTER TABLE core.published DROP COLUMN stream_instance;
ALTER TABLE core.published ADD CONSTRAINT published_pkey PRIMARY KEY (event_id);
DROP TRIGGER blob_changes ON core.blobs;
DROP FUNCTION core.note_blob_change();
DELETE FROM core.counters WHERE name='blob_changes';
