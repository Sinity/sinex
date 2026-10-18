//! Table declarations for version 0 (architecture §3.6; spec `storage.md` §3).
use crate::Checks;

pub(crate) const SCHEMAS: &[&str] = &[
    "sinex_schema",
    "core",
    "audit",
    "raw",
    "registry",
    "derivation",
    "privacy",
    "reflection",
    "kinds",
];

pub(crate) fn tables(c: &Checks) -> Vec<String> {
    let event_columns = format!(
        "id uuid NOT NULL,
  event_type text NOT NULL,
  source text NOT NULL,
  run_id uuid NOT NULL,
  material_id uuid,
  anchor bigint,
  ordinal smallint NOT NULL DEFAULT 0,
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
  support jsonb,
  CONSTRAINT one_origin CHECK ((material_id IS NOT NULL AND anchor IS NOT NULL AND parents IS NULL) OR (material_id IS NULL AND anchor IS NULL AND parents IS NOT NULL AND cardinality(parents) > 0)),
  CONSTRAINT id_is_uuidv7 CHECK (uuid_extract_version(id) = 7),
  CONSTRAINT time_pair CHECK ((ts_orig IS NULL) = (ts_quality = 'unknown')),
  CONSTRAINT anchor_in_range CHECK (anchor IS NULL OR anchor >= 0),
  CONSTRAINT ordinal_in_range CHECK (ordinal >= 0),
  CONSTRAINT payload_object CHECK (jsonb_typeof(payload) = 'object'),
  CONSTRAINT hash_length CHECK (length(record_hash) = 32)",
        time_quality = c.time_quality,
        record_state = c.record_state,
        class = c.class,
    );
    vec![
        // Events. No foreign key points into a hypertable (spec storage.md, sinex-h8no).
        format!("CREATE TABLE core.events (\n  {event_columns},\n  PRIMARY KEY (id)\n)"),
        format!("CREATE TABLE reflection.events (\n  {event_columns},\n  PRIMARY KEY (id)\n)"),
        // One live reading per record location, event type and output number.
        "CREATE TABLE core.live_coordinates (
  material_id uuid NOT NULL,
  anchor bigint NOT NULL,
  event_type text NOT NULL,
  ordinal smallint NOT NULL,
  event_id uuid NOT NULL,
  reading_order bigint NOT NULL,
  record_hash bytea NOT NULL,
  PRIMARY KEY (material_id, anchor, event_type, ordinal)
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
        // Hidden and excised occurrences, remembered so neither replay nor
        // re-import brings them back (identity rule 6).
        "CREATE TABLE core.hidden_occurrences (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  source text NOT NULL,
  event_type text NOT NULL,
  equivalence_key text,
  material_id uuid,
  anchor bigint,
  ordinal smallint,
  kind text NOT NULL CHECK (kind IN ('hide', 'excise')),
  operation_id uuid NOT NULL,
  hidden_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT keyed_or_located CHECK ((equivalence_key IS NOT NULL AND material_id IS NULL AND anchor IS NULL AND ordinal IS NULL) OR (equivalence_key IS NULL AND material_id IS NOT NULL AND anchor IS NOT NULL AND ordinal IS NOT NULL))
)".into(),
        "CREATE UNIQUE INDEX hidden_by_key ON core.hidden_occurrences (source, event_type, equivalence_key) WHERE equivalence_key IS NOT NULL".into(),
        "CREATE UNIQUE INDEX hidden_by_location ON core.hidden_occurrences (material_id, anchor, event_type, ordinal) WHERE material_id IS NOT NULL".into(),
        // What an excision removed: identity and time, never the payload
        // (spec storage.md §9).
        "CREATE TABLE core.event_tombstones (
  event_id uuid PRIMARY KEY,
  source text NOT NULL,
  event_type text NOT NULL,
  ts_orig timestamptz,
  operation_id uuid NOT NULL,
  removed_at timestamptz NOT NULL DEFAULT now()
)".into(),
        // Record hashes an excision removed, so re-staging the same bytes
        // (a new material, so a new location) does not bring them back.
        "CREATE TABLE core.excised_digests (
  source text NOT NULL,
  event_type text NOT NULL,
  record_hash bytea NOT NULL,
  operation_id uuid NOT NULL,
  PRIMARY KEY (source, event_type, record_hash)
)".into(),
        // Replay runs one transaction per material chain; a finished chain is
        // recorded here, so a resumed replay skips it.
        "CREATE TABLE core.replay_chains (
  operation_id uuid NOT NULL,
  chain_root uuid NOT NULL,
  completed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (operation_id, chain_root)
)".into(),
        // Commit order, read by derivations inside the daemon and by the publisher.
        "CREATE TABLE core.commit_log (
  commit_seq bigint PRIMARY KEY,
  event_id uuid NOT NULL,
  event_type text NOT NULL
)".into(),
        // The writer's counters. One writer, so a row update allocates a range.
        "CREATE TABLE core.counters (name text PRIMARY KEY, value bigint NOT NULL)".into(),
        "INSERT INTO core.counters (name, value) VALUES ('commit_seq', 0)".into(),
        // The archive is logical: archived rows stay in core.events.
        "CREATE TABLE audit.archived (
  event_id uuid PRIMARY KEY,
  archived_seq bigint NOT NULL,
  reason text NOT NULL CHECK (reason IN ('replay', 'newer_reading', 'hide', 'excise', 'version_change', 'recomputed')),
  replaced_by uuid,
  operation_id uuid,
  archived_at timestamptz NOT NULL DEFAULT now()
)".into(),
        "CREATE TABLE core.blobs (
  hash text PRIMARY KEY CHECK (hash ~ '^blake3:[0-9a-f]{64}$'),
  size bigint NOT NULL CHECK (size >= 0),
  status text NOT NULL CHECK (status IN ('present', 'quarantined')),
  registered_at timestamptz NOT NULL DEFAULT now()
)".into(),
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
        "CREATE TABLE raw.revision_extents (
  revision_id uuid NOT NULL REFERENCES raw.revisions(id),
  position integer NOT NULL,
  blob text NOT NULL REFERENCES core.blobs(hash),
  blob_offset bigint NOT NULL,
  length bigint NOT NULL,
  PRIMARY KEY (revision_id, position)
)".into(),
        "CREATE TABLE raw.materials (
  id uuid PRIMARY KEY,
  input_id uuid REFERENCES raw.inputs(id),
  revision_id uuid REFERENCES raw.revisions(id),
  part_index integer NOT NULL DEFAULT 0,
  previous_part_id uuid REFERENCES raw.materials(id),
  blob text NOT NULL REFERENCES core.blobs(hash),
  durable_bytes bigint NOT NULL CHECK (durable_bytes >= 0),
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
        "CREATE TABLE raw.positions (
  binding text PRIMARY KEY,
  cursor jsonb NOT NULL,
  carry jsonb NOT NULL,
  committed_seq bigint NOT NULL,
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
  ended_at timestamptz
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
  consumer text NOT NULL
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
        format!("CREATE TABLE privacy.rules (
  id uuid PRIMARY KEY,
  source text NOT NULL,
  event_type text,
  field text,
  class text NOT NULL {class},
  action text NOT NULL CHECK (action IN ('classify', 'mask', 'drop_field'))
)", class = c.class),
        "CREATE TABLE privacy.material_keys (material_id uuid PRIMARY KEY, class text NOT NULL, wrapped_key bytea NOT NULL)".into(),
        // Full-text search read model (spec `query.md` §5, `storage.md`
        // §3.11): each event's payload text, normalized (case-folded, NFC,
        // words separated by single spaces), kept uncompressed apart from the
        // hypertable. Rebuildable from the events; maintained from the commit
        // log by its reader, never by a trigger.
        "CREATE TABLE core.search_text (
  event_id uuid PRIMARY KEY,
  text text NOT NULL,
  tsv tsvector GENERATED ALWAYS AS (to_tsvector('simple', text)) STORED
)".into(),
        "CREATE INDEX search_text_tsv ON core.search_text USING gin (tsv)".into(),
        "CREATE INDEX search_text_trigram ON core.search_text USING gin (text gin_trgm_ops)".into(),
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
  ordinal smallint NOT NULL,
  parents uuid[],
  ts_orig timestamptz,
  ts_quality text NOT NULL,
  payload jsonb NOT NULL,
  equivalence_key text,
  replace_rule text NOT NULL,
  reading_order bigint NOT NULL,
  record_hash bytea NOT NULL,
  record_state text NOT NULL,
  class text NOT NULL,
  support jsonb
)".into(),
    ]
}

/// Hypertables, indexes and guards. Events are partitioned by `id` (UUIDv7, so
/// chunks follow minting time) with chunk skipping on `ts_orig`, because
/// `ts_orig` can be unknown and historical imports put old times in new chunks
/// (spec `storage.md` §3.1).
pub(crate) fn after_tables() -> Vec<String> {
    let mut out = vec![
        "CREATE FUNCTION core.event_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'UPDATE' THEN
    RAISE EXCEPTION 'event rows are immutable' USING ERRCODE = '23514';
  END IF;
  IF TG_OP = 'DELETE' AND nullif(current_setting('sinex.excise_operation', true), '') IS NULL THEN
    RAISE EXCEPTION 'events are deleted only by an excision' USING ERRCODE = '23514';
  END IF;
  RETURN OLD;
END $$"
            .to_string(),
    ];
    for table in ["core.events", "reflection.events"] {
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
