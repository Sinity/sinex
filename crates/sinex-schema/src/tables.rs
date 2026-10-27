//! Table declarations: the whole current schema, which a fresh database is
//! created with.
use crate::Checks;

pub(crate) const SCHEMAS: &[&str] = &[
    "sinex_schema",
    "core",
    "audit",
    "raw",
    "registry",
    "derivation",
    "privacy",
    "telemetry",
    "event_types",
];

/// Per source: its live events, and the oldest and newest event time and the
/// newest reading time of every event admitted from it. Admission, archival,
/// unhiding and excision keep it current in their own transactions, so status
/// reads one row per source instead of counting events.
pub(crate) const SOURCE_COUNTS: &str = "CREATE TABLE core.source_counts (
  source text PRIMARY KEY,
  events bigint NOT NULL CHECK (events >= 0),
  first_event timestamptz,
  last_event timestamptz,
  last_seen timestamptz NOT NULL
)";

/// The materials excisions removed, by the digest of their bytes, so the same
/// bytes are refused if they are registered again. Nothing else of them is
/// kept.
pub(crate) const EXCISED_MATERIALS: &str = "CREATE TABLE core.excised_materials (
  material_id uuid PRIMARY KEY,
  blob text NOT NULL,
  operation_id uuid NOT NULL
)";
pub(crate) const EXCISED_MATERIALS_BLOB: &str =
    "CREATE INDEX excised_materials_blob ON core.excised_materials (blob)";

pub(crate) fn tables(c: &Checks) -> Vec<String> {
    let event_columns = format!(
        "id uuid NOT NULL,
  event_type text NOT NULL,
  source text NOT NULL,
  run_id uuid NOT NULL,
  material_id uuid,
  anchor bigint,
  output_index smallint NOT NULL DEFAULT 0,
  parents uuid[],
  ts_orig timestamptz,
  ts_quality text NOT NULL {time_quality},
  ts_coided timestamptz GENERATED ALWAYS AS (uuid_extract_timestamp(id)) STORED,
  ts_persisted timestamptz NOT NULL DEFAULT now(),
  commit_seq bigint NOT NULL,
  payload jsonb NOT NULL,
  equivalence_key text,
  reading_order bigint NOT NULL,
  record_hash bytea NOT NULL,
  record_state text NOT NULL DEFAULT 'present' {record_state},
  class text NOT NULL DEFAULT 'open' {class},
  CONSTRAINT one_origin CHECK ((material_id IS NOT NULL AND anchor IS NOT NULL AND parents IS NULL) OR (material_id IS NULL AND anchor IS NULL AND parents IS NOT NULL AND cardinality(parents) > 0)),
  CONSTRAINT id_is_uuidv7 CHECK (uuid_extract_version(id) = 7),
  CONSTRAINT time_pair CHECK ((ts_orig IS NULL) = (ts_quality = 'unknown')),
  CONSTRAINT anchor_in_range CHECK (anchor IS NULL OR anchor >= 0),
  CONSTRAINT output_index_in_range CHECK (output_index >= 0),
  CONSTRAINT payload_object CHECK (jsonb_typeof(payload) = 'object'),
  CONSTRAINT hash_length CHECK (length(record_hash) = 32)",
        time_quality = c.time_quality,
        record_state = c.record_state,
        class = c.class,
    );
    vec![
        // Events. No foreign key points into a hypertable.
        format!("CREATE TABLE core.events (\n  {event_columns},\n  PRIMARY KEY (id)\n)"),
        format!("CREATE TABLE telemetry.events (\n  {event_columns},\n  PRIMARY KEY (id)\n)"),
        // One live reading per record location, event type and output number.
        "CREATE TABLE core.live_locations (
  material_id uuid NOT NULL,
  anchor bigint NOT NULL,
  event_type text NOT NULL,
  output_index smallint NOT NULL,
  event_id uuid NOT NULL,
  reading_order bigint NOT NULL,
  record_hash bytea NOT NULL,
  PRIMARY KEY (material_id, anchor, event_type, output_index)
)".into(),
        // One live reading per equivalence key.
        "CREATE TABLE core.live_equivalence (
  source text NOT NULL,
  event_type text NOT NULL,
  equivalence_key text NOT NULL,
  event_id uuid NOT NULL,
  reading_order bigint NOT NULL,
  record_hash bytea NOT NULL,
  PRIMARY KEY (source, event_type, equivalence_key)
)".into(),
        // Found by event: removal of live readings, and readings at a location.
        "CREATE INDEX live_equivalence_event ON core.live_equivalence (event_id)".into(),
        "CREATE INDEX live_locations_event ON core.live_locations (event_id)".into(),
        // Hidden records, remembered so neither replay nor re-import brings
        // them back.
        "CREATE TABLE core.hidden_records (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  source text NOT NULL,
  event_type text NOT NULL,
  equivalence_key text,
  material_id uuid,
  anchor bigint,
  output_index smallint,
  operation_id uuid NOT NULL,
  hidden_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT keyed_or_located CHECK ((equivalence_key IS NOT NULL AND material_id IS NULL AND anchor IS NULL AND output_index IS NULL) OR (equivalence_key IS NULL AND material_id IS NOT NULL AND anchor IS NOT NULL AND output_index IS NOT NULL))
)".into(),
        "CREATE UNIQUE INDEX hidden_by_key ON core.hidden_records (source, event_type, equivalence_key) WHERE equivalence_key IS NOT NULL".into(),
        "CREATE UNIQUE INDEX hidden_by_location ON core.hidden_records (material_id, anchor, event_type, output_index) WHERE material_id IS NOT NULL".into(),
        // What an excision removed: identity and time, never the payload.
        "CREATE TABLE core.event_tombstones (
  event_id uuid PRIMARY KEY,
  source text NOT NULL,
  event_type text NOT NULL,
  ts_orig timestamptz,
  operation_id uuid NOT NULL,
  removed_at timestamptz NOT NULL DEFAULT now()
)".into(),
        // Replay runs one transaction per part sequence; a finished sequence is
        // recorded here, so a resumed replay skips it.
        "CREATE TABLE core.replayed_sequences (
  operation_id uuid NOT NULL,
  first_part uuid NOT NULL,
  completed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (operation_id, first_part)
)".into(),
        // Commit order, read by derivations inside the daemon and by the publisher.
        "CREATE TABLE core.commit_log (
  commit_seq bigint PRIMARY KEY,
  event_id uuid,
  event_type text,
  persisted_at timestamptz DEFAULT now(),
  CHECK ((event_id IS NULL) = (event_type IS NULL))
)".into(),
        // The writer's counters. One writer, so a row update allocates a range.
        "CREATE TABLE core.counters (name text PRIMARY KEY, value bigint NOT NULL)".into(),
        "INSERT INTO core.counters (name, value) VALUES ('commit_seq', 0)".into(),
        SOURCE_COUNTS.into(),
        EXCISED_MATERIALS.into(),
        EXCISED_MATERIALS_BLOB.into(),
        // Positions of the commit-log consumers that are not derivations: the
        // confirmed-stream publisher and the read-model maintainers. Each
        // commits its position with the work it covers.
        "CREATE TABLE core.consumer_positions (
  consumer text PRIMARY KEY,
  commit_seq bigint NOT NULL,
  change_seq bigint NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now()
)".into(),
        // Where each published event sits in the confirmed stream, so an
        // excision can erase it there too: excision reaches every surface. The
        // row goes once the message is erased.
        "CREATE TABLE core.published (
  event_id uuid PRIMARY KEY,
  stream_seq bigint NOT NULL
)".into(),
        // Frames outside producers sent over the NATS ingress and what Sinex
        // did with each, written by admission with the frame's events, so a
        // producer can ask what Sinex holds and a redelivered frame is answered
        // from here instead of admitted twice. `number` orders a producer's
        // pages of its registered materials.
        "CREATE TABLE core.ingress_frames (
  number bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
  producer text NOT NULL,
  frame text NOT NULL,
  material_id uuid,
  admitted uuid[] NOT NULL DEFAULT '{}',
  outcome jsonb NOT NULL,
  refusal text,
  commit_seq bigint,
  received_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (producer, frame)
)".into(),
        // The archive is logical: archived rows stay in core.events. This is
        // the current state; archive_changes holds every transition.
        format!("CREATE TABLE audit.archived (
  event_id uuid PRIMARY KEY,
  change_seq bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
  archived_seq bigint NOT NULL,
  reason text NOT NULL {archive_reason},
  replaced_by uuid,
  operation_id uuid,
  archived_at timestamptz NOT NULL DEFAULT now()
)", archive_reason = c.archive_reason),
        // Every visibility change at its writer position, so an as-of read
        // sees the visibility that held at that position.
        format!("CREATE TABLE audit.archive_changes (
  change_seq bigint PRIMARY KEY,
  event_id uuid NOT NULL,
  commit_seq bigint NOT NULL,
  archived boolean NOT NULL,
  reason text NOT NULL {archive_change_reason},
  replaced_by uuid,
  operation_id uuid,
  recorded_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (event_id, commit_seq),
  CHECK (archived = (reason <> 'unhide'))
)", archive_change_reason = c.archive_change_reason),
        "CREATE INDEX archive_changes_position ON audit.archive_changes (event_id, commit_seq DESC)".into(),
        "CREATE TABLE core.blobs (
  hash text PRIMARY KEY CHECK (hash ~ '^blake3:[0-9a-f]{64}$'),
  size bigint NOT NULL CHECK (size >= 0),
  -- The file is written before the admission that inserts this row.
  -- `deleting`: an excision left the bytes unreferenced; the writer deletes
  -- the file under this row's lock, then the row, after commit or when the
  -- daemon starts. The writer admits no row whose file is missing.
  status text NOT NULL CHECK (status IN ('present', 'deleting')),
  registered_at timestamptz NOT NULL DEFAULT now()
)".into(),
        // Stored bytes an event owns beyond its material (a library file
        // kept by the scan): excising the event deletes them once nothing
        // else refers to them. No foreign key into the events hypertable.
        "CREATE TABLE core.event_blobs (
  event_id uuid NOT NULL,
  blob text NOT NULL REFERENCES core.blobs(hash),
  PRIMARY KEY (event_id, blob)
)".into(),
        "CREATE INDEX event_blobs_blob ON core.event_blobs (blob)".into(),
        "CREATE TABLE raw.inputs (
  id uuid PRIMARY KEY,
  binding text NOT NULL,
  kind text NOT NULL,
  locator text NOT NULL,
  host text NOT NULL,
  UNIQUE (binding, locator, host)
)".into(),
        // One row per look that found a change; unchanged looks advance a counter.
        "CREATE TABLE raw.observations (
  id uuid PRIMARY KEY,
  input_id uuid NOT NULL REFERENCES raw.inputs(id),
  observed_at timestamptz NOT NULL,
  size numeric,
  mtime_ns numeric,
  ctime_ns numeric,
  inode numeric,
  device numeric,
  unavailable text,
  unchanged_looks bigint NOT NULL DEFAULT 0,
  last_looked_at timestamptz NOT NULL
)".into(),
        "CREATE INDEX observations_input ON raw.observations (input_id, observed_at)".into(),
        "CREATE TABLE raw.revisions (
  id uuid PRIMARY KEY,
  input_id uuid NOT NULL REFERENCES raw.inputs(id),
  previous_id uuid REFERENCES raw.revisions(id),
  kind text NOT NULL CHECK (kind IN ('appended', 'rewritten', 'replaced', 'truncated', 'snapshot')),
  started_at timestamptz NOT NULL DEFAULT now()
)".into(),
        "CREATE TABLE raw.materials (
  id uuid PRIMARY KEY,
  input_id uuid REFERENCES raw.inputs(id),
  revision_id uuid REFERENCES raw.revisions(id),
  part_index integer NOT NULL DEFAULT 0,
  previous_part_id uuid REFERENCES raw.materials(id),
  blob text NOT NULL REFERENCES core.blobs(hash),
  length bigint NOT NULL CHECK (length >= 0),
  manifest jsonb NOT NULL,
  parser text NOT NULL,
  parser_version integer NOT NULL
)".into(),
        "CREATE INDEX materials_input ON raw.materials (input_id, part_index)".into(),
        // One row per record a parser could not read; the bytes stay in the
        // material, so a retry is a replay of exactly those bytes.
        "CREATE TABLE raw.failures (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  material_id uuid NOT NULL REFERENCES raw.materials(id),
  anchor bigint NOT NULL,
  reason text NOT NULL,
  run_id uuid NOT NULL,
  failed_at timestamptz NOT NULL DEFAULT now()
)".into(),
        // Each binding's source and the settings it is read with, so a replay
        // reads its materials exactly as the capture did.
        "CREATE TABLE raw.bindings (
  binding text PRIMARY KEY,
  source text NOT NULL,
  input text NOT NULL,
  settings jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
)".into(),
        "CREATE TABLE raw.positions (
  binding text PRIMARY KEY,
  cursor jsonb NOT NULL,
  carry jsonb NOT NULL,
  committed_seq bigint NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
)".into(),
        // The latest memory a reader saved with its cursor at the end of
        // `material`: a restart loads it and reads only the parts after it.
        // Replaced in the transaction that saves the cursor.
        "CREATE TABLE raw.reader_memory (
  binding text PRIMARY KEY,
  material_id uuid NOT NULL REFERENCES raw.materials(id),
  memory bytea NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
)".into(),
        "CREATE TABLE core.manifests (
  id uuid PRIMARY KEY,
  name text NOT NULL,
  kind text NOT NULL CHECK (kind IN ('source', 'derivation', 'producer', 'capture_node', 'api')),
  version text NOT NULL,
  build text NOT NULL,
  emits text[] NOT NULL DEFAULT '{}',
  reads text[] NOT NULL DEFAULT '{}',
  UNIQUE (name, version, build)
)".into(),
        "CREATE TABLE core.runs (
  id uuid PRIMARY KEY,
  manifest_id uuid REFERENCES core.manifests(id),
  name text NOT NULL,
  host text NOT NULL,
  started_at timestamptz NOT NULL DEFAULT now(),
  ended_at timestamptz,
  -- The newest privacy rule row in force when the run's events were admitted.
  privacy_rules_seq bigint,
  -- How many of the run's events are stored, kept by admission and
  -- excision.
  events bigint NOT NULL DEFAULT 0
)".into(),
        "CREATE TABLE core.operations (
  id uuid PRIMARY KEY,
  kind text NOT NULL,
  actor text NOT NULL,
  scope jsonb NOT NULL,
  at timestamptz NOT NULL DEFAULT now()
)".into(),
        format!("CREATE TABLE registry.event_types (
  name text PRIMARY KEY,
  payload_schema jsonb NOT NULL,
  key_kind text NOT NULL CHECK (key_kind IN ('location', 'fields')),
  key_fields text[] NOT NULL,
  replace_rule text NOT NULL {replace}
)", replace = c.replace_rule),
        "CREATE TABLE registry.sources (
  name text PRIMARY KEY,
  driver text NOT NULL,
  version integer NOT NULL,
  event_types text[] NOT NULL,
  purpose text NOT NULL
)".into(),
        "CREATE TABLE registry.derivations (
  name text PRIMARY KEY,
  kind text NOT NULL CHECK (kind IN ('per_event', 'keyed')),
  version integer NOT NULL,
  inputs text[] NOT NULL,
  outputs text[] NOT NULL
)".into(),
        // Keys a keyed derivation must recompute, marked by admission.
        "CREATE TABLE derivation.dirty (
  derivation text NOT NULL,
  key jsonb NOT NULL,
  first_dirty_seq bigint NOT NULL,
  last_dirty_seq bigint NOT NULL,
  PRIMARY KEY (derivation, key)
)".into(),
        "CREATE TABLE derivation.positions (derivation text PRIMARY KEY, commit_seq bigint NOT NULL)".into(),
        // Each key's current outputs, so a recompute replaces the key's
        // outputs as a set: outputs no longer produced are archived.
        "CREATE TABLE derivation.outputs (
  derivation text NOT NULL,
  key jsonb NOT NULL,
  event_id uuid NOT NULL,
  PRIMARY KEY (derivation, key, event_id)
)".into(),
        "CREATE INDEX outputs_event ON derivation.outputs (event_id)".into(),
        // Keys whose outputs wait for evidence outside their window (an open
        // interval); recomputed whenever the derivation runs.
        "CREATE TABLE derivation.waiting (
  derivation text NOT NULL,
  key jsonb NOT NULL,
  since timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (derivation, key)
)".into(),
        // A comparison: what a recompute of a derivation would change,
        // computed without writing outputs, kept so a version change can be
        // judged before it is applied.
        "CREATE TABLE derivation.comparisons (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  derivation text NOT NULL,
  version integer NOT NULL,
  range_from timestamptz,
  range_until timestamptz,
  made_at timestamptz NOT NULL DEFAULT now(),
  keys bigint NOT NULL,
  unchanged bigint NOT NULL,
  added bigint NOT NULL,
  changed bigint NOT NULL,
  removed bigint NOT NULL,
  examples jsonb NOT NULL
)".into(),
        // Privacy rules: insert-only. A change is a new row with the same name;
        // a removal is a row with `ended`. The newest row per name is in force;
        // `seq` orders the history, and each admitted run records the newest
        // `seq` it was admitted under.
        format!("CREATE TABLE privacy.rules (
  seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  id uuid NOT NULL UNIQUE,
  name text NOT NULL,
  source text NOT NULL,
  event_type text,
  field text CHECK (field IS NULL OR field LIKE '/%'),
  matcher text NOT NULL CHECK (matcher IN ('all', 'pattern', 'terms', 'luhn', 'iban')),
  matcher_arg text,
  class text NOT NULL {class},
  action text NOT NULL CHECK (action IN ('classify', 'mask', 'hash', 'drop_field')),
  author text NOT NULL,
  written_at timestamptz NOT NULL DEFAULT now(),
  ended boolean NOT NULL DEFAULT false
)", class = c.class),
        "CREATE INDEX rules_name ON privacy.rules (name, seq DESC)".into(),
        // A source's class floor, set by the operator; newest row wins.
        format!("CREATE TABLE privacy.source_classes (
  seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  source text NOT NULL,
  class text NOT NULL {class},
  author text NOT NULL,
  written_at timestamptz NOT NULL DEFAULT now()
)", class = c.class),
        // The key that makes hashed values join without revealing them.
        "CREATE TABLE privacy.settings (
  id boolean PRIMARY KEY CHECK (id),
  hash_key bytea NOT NULL CHECK (length(hash_key) = 32)
)".into(),
        // A sealed material's data key, wrapped with the class key's public half.
        "CREATE TABLE privacy.data_keys (
  material_id uuid PRIMARY KEY,
  key_ref text NOT NULL UNIQUE,
  class text NOT NULL,
  wrapped_key bytea NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
)".into(),
        // Full-text search read model: each event's payload text, normalized
        // (case-folded, NFC, words separated by single spaces), kept
        // uncompressed apart from the hypertable. Rebuildable from the events;
        // maintained from the commit log by its reader, never by a trigger.
        "CREATE TABLE core.search_text (
  event_id uuid PRIMARY KEY,
  text text NOT NULL,
  tsv tsvector GENERATED ALWAYS AS (to_tsvector('simple', text)) STORED,
  trigram_indexed boolean NOT NULL DEFAULT true
)".into(),
        "CREATE INDEX search_text_tsv ON core.search_text USING gin (tsv)".into(),
        "CREATE INDEX search_text_trigram ON core.search_text USING gin (text gin_trgm_ops) WHERE trigram_indexed".into(),
        // Admission's staging table. Permanent and unlogged so compile-time
        // checked queries can name it; only the one writer uses it, inside its
        // transaction, and truncates it first.
        "CREATE UNLOGGED TABLE core.admission_stage (
  ord integer PRIMARY KEY,
  id uuid NOT NULL,
  event_type text NOT NULL,
  source text NOT NULL,
  material_id uuid,
  anchor bigint,
  output_index smallint NOT NULL,
  parents uuid[],
  ts_orig timestamptz,
  ts_quality text NOT NULL,
  payload jsonb NOT NULL,
  equivalence_key text,
  replace_rule text NOT NULL,
  reading_order bigint NOT NULL,
  record_hash bytea NOT NULL,
  record_state text NOT NULL,
  class text NOT NULL
)".into(),
    ]
}

/// Hypertables, indexes and guards. Events are partitioned by `id` (UUIDv7, so
/// chunks follow minting time) with chunk skipping on `ts_orig`, because
/// `ts_orig` can be unknown and historical imports put old times in new chunks.
pub(crate) fn after_tables() -> Vec<String> {
    let mut out = vec![archive_history_function().into(), event_guard().into()];
    for table in ["core.events", "telemetry.events"] {
        for template in [
            "SELECT create_hypertable('$T', by_range('id', INTERVAL '1 day'), create_default_indexes => false)",
            "CREATE INDEX ON $T (ts_orig)",
            "CREATE INDEX ON $T (source, ts_orig)",
            "CREATE INDEX ON $T (event_type, ts_orig)",
            "CREATE INDEX ON $T (material_id, anchor)",
            // Only derived events have parents. Without the pending list a
            // search never scans an unsorted backlog of bulk inserts (which
            // made each probe cost 0.6 ms after a backfill).
            "CREATE INDEX ON $T USING gin (parents) WITH (fastupdate = off) WHERE parents IS NOT NULL",
            "CREATE TRIGGER event_guard BEFORE UPDATE OR DELETE ON $T FOR EACH ROW EXECUTE FUNCTION core.event_guard()",
            "ALTER TABLE $T SET (timescaledb.compress, timescaledb.compress_segmentby = 'source, event_type', timescaledb.compress_orderby = 'ts_orig, id')",
            "SELECT enable_chunk_skipping('$T', 'ts_orig')",
            "SELECT add_compression_policy('$T', INTERVAL '7 days')",
        ] {
            out.push(template.replace("$T", table));
        }
    }
    out
}

fn archive_history_function() -> &'static str {
    "CREATE FUNCTION audit.is_archived_at(target_event uuid, at_seq bigint) RETURNS boolean LANGUAGE SQL STABLE PARALLEL SAFE AS $$
SELECT coalesce((SELECT archived FROM audit.archive_changes
  WHERE event_id = target_event AND commit_seq <= at_seq ORDER BY commit_seq DESC LIMIT 1), false)
$$"
}

pub(crate) fn event_guard() -> &'static str {
    "CREATE OR REPLACE FUNCTION core.event_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'UPDATE' THEN
    RAISE EXCEPTION 'event rows are immutable' USING ERRCODE = '23514';
  END IF;
  IF TG_OP = 'DELETE' AND nullif(current_setting('sinex.excise_operation', true), '') IS NULL THEN
    RAISE EXCEPTION 'events are deleted only by an excision' USING ERRCODE = '23514';
  END IF;
  RETURN OLD;
END $$"
}
