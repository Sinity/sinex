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
    "analysis",
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
        // Positions of the commit-log consumers that are not derivations: the
        // confirmed-stream publisher and the read-model maintainers. Each
        // commits its position with the work it covers.
        "CREATE TABLE core.consumer_positions (
  consumer text PRIMARY KEY,
  commit_seq bigint NOT NULL,
  notice_seq bigint NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now()
)".into(),
        // Where each published event sits in the confirmed stream, so an
        // excision can erase it there too: excision reaches every surface
        // (architecture §3.4). The row goes once the message is erased.
        "CREATE TABLE core.published (
  event_id uuid PRIMARY KEY,
  stream_seq bigint NOT NULL
)".into(),
        // Frames outside producers sent over the NATS ingress and what Sinex did
        // with each, written by admission with the frame's events, so a producer
        // can ask what Sinex holds (`mn8u`) and a redelivered frame is answered
        // from here instead of admitted twice.
        "CREATE TABLE core.ingress_frames (
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
        // The archive is logical: archived rows stay in core.events.
        // `notice_seq` orders archive notices on the confirmed stream: one
        // writer, so insertion order is commit order.
        "CREATE TABLE audit.archived (
  event_id uuid PRIMARY KEY,
  notice_seq bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
  archived_seq bigint NOT NULL,
  reason text NOT NULL CHECK (reason IN ('replay', 'newer_reading', 'hide', 'excise', 'version_change', 'recomputed')),
  replaced_by uuid,
  operation_id uuid,
  archived_at timestamptz NOT NULL DEFAULT now()
)".into(),
        "CREATE TABLE core.blobs (
  hash text PRIMARY KEY CHECK (hash ~ '^blake3:[0-9a-f]{64}$'),
  size bigint NOT NULL CHECK (size >= 0),
  -- `deleting`: no row refers to the bytes any more (an excision); the
  -- file is deleted after commit, then this row. A row left `deleting` by a
  -- crash is finished when the daemon starts.
  status text NOT NULL CHECK (status IN ('present', 'quarantined', 'deleting')),
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
  ended_at timestamptz,
  -- The newest privacy rule row in force when the run's events were admitted.
  privacy_rules_seq bigint
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
        // The comparison lane (architecture §6.3): what a recompute of a
        // derivation would change, computed without writing outputs, kept so a
        // version change can be judged before it is applied.
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
        // Privacy rules (spec `privacy.md` §1 rule 2): insert-only. A change is
        // a new row with the same name; a removal is a row with `ended`. The
        // newest row per name is in force; `seq` orders the history, and each
        // admitted run records the newest `seq` it was admitted under.
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
        // A sealed material's data key, wrapped with the class key's public
        // half; deleting the row is the excision of everything it sealed.
        "CREATE TABLE privacy.material_keys (
  material_id uuid PRIMARY KEY,
  key_ref text NOT NULL UNIQUE,
  class text NOT NULL,
  wrapped_key bytea NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
)".into(),
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
        // The analysis layer (spec `analysis.md` §14). Metric values are a
        // read model kept per unit, recomputed from dirty units; requests,
        // attempts, results, their dependencies and status, and findings are
        // the products. None of it is an event and all of it is rebuildable.
        "CREATE TABLE analysis.metric_values (
  metric text NOT NULL,
  metric_version integer NOT NULL,
  day_setting text NOT NULL,
  unit_start timestamptz NOT NULL,
  unit_end timestamptz NOT NULL,
  unit_label text NOT NULL,
  dimension text NOT NULL DEFAULT '',
  dimension_value text NOT NULL DEFAULT '',
  state text NOT NULL CHECK (state IN ('observed', 'partial', 'recorded', 'gap', 'unknown', 'not_applicable')),
  reason text,
  value double precision,
  observed_fraction double precision,
  numerator double precision,
  denominator double precision,
  n_rows bigint NOT NULL,
  observed_s double precision NOT NULL,
  computed_seq bigint NOT NULL,
  PRIMARY KEY (metric, metric_version, day_setting, unit_start, dimension, dimension_value),
  CHECK ((state IN ('observed', 'partial', 'recorded')) = (value IS NOT NULL))
)".into(),
        "CREATE TABLE analysis.requests (
  id bytea PRIMARY KEY CHECK (length(id) = 32),
  body jsonb NOT NULL,
  recipe text NOT NULL,
  recipe_version integer NOT NULL,
  intent text NOT NULL CHECK (intent IN ('exploratory', 'registered')),
  family jsonb,
  as_of bigint NOT NULL,
  standing text,
  track_staleness boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now()
)".into(),
        "CREATE TABLE analysis.attempts (
  id uuid PRIMARY KEY,
  request bytea NOT NULL REFERENCES analysis.requests(id),
  requested_by jsonb NOT NULL,
  producer text NOT NULL,
  started_at timestamptz NOT NULL,
  finished_at timestamptz,
  status text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed', 'cancelled')),
  error text
)".into(),
        "CREATE TABLE analysis.results (
  id uuid PRIMARY KEY,
  attempt uuid NOT NULL UNIQUE REFERENCES analysis.attempts(id),
  request bytea NOT NULL REFERENCES analysis.requests(id),
  status text NOT NULL CHECK (status IN ('estimated', 'not_estimable', 'failed')),
  reason text,
  readsets jsonb NOT NULL,
  tables jsonb,
  diagnostics jsonb NOT NULL,
  output_hash bytea NOT NULL CHECK (length(output_hash) = 32),
  created_at timestamptz NOT NULL DEFAULT now()
)".into(),
        "CREATE INDEX results_request ON analysis.results (request)".into(),
        "CREATE TABLE analysis.result_dependencies (
  result uuid NOT NULL REFERENCES analysis.results(id) ON DELETE CASCADE,
  relation text NOT NULL,
  span tstzrange NOT NULL
)".into(),
        "CREATE INDEX result_dependencies_relation ON analysis.result_dependencies (relation)".into(),
        "CREATE INDEX result_dependencies_span ON analysis.result_dependencies USING gist (span)".into(),
        "CREATE TABLE analysis.result_status (
  result uuid PRIMARY KEY REFERENCES analysis.results(id) ON DELETE CASCADE,
  state text NOT NULL CHECK (state IN ('current', 'stale', 'superseded', 'withdrawn')),
  reason text,
  changed_seq bigint NOT NULL,
  changed_at timestamptz NOT NULL DEFAULT now()
)".into(),
        "CREATE TABLE analysis.findings (
  id uuid PRIMARY KEY,
  result uuid NOT NULL REFERENCES analysis.results(id) ON DELETE CASCADE,
  template text NOT NULL,
  slots jsonb NOT NULL,
  role text NOT NULL,
  status text NOT NULL,
  caveats jsonb NOT NULL,
  evidence jsonb NOT NULL,
  checks jsonb NOT NULL
)".into(),
        "CREATE INDEX findings_result ON analysis.findings (result)".into(),
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
