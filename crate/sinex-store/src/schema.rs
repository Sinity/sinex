//! The deliberately small foundation schema. Future declarations extend this owner.
use crate::{Error, Result};
use sea_query::{Alias, ColumnDef, Expr, PostgresQueryBuilder, Table};
use sqlx::{Connection, Executor, PgConnection, Row};
use std::collections::BTreeMap;

pub const WRITER_ROLE: &str = "sinex_writer";
pub const READER_ROLE: &str = "sinex_reader";
pub const READ_MODEL_ROLE: &str = "sinex_read_model";

struct Column {
    name: &'static str,
    ty: &'static str,
    nullable: bool,
    default: Option<&'static str>,
    generated: Option<&'static str>,
}
struct Definition {
    schema: &'static str,
    name: &'static str,
    columns: &'static [Column],
    constraints: &'static [(&'static str, &'static str)],
}
macro_rules! col {
    ($name:literal, $ty:literal) => {
        Column {
            name: $name,
            ty: $ty,
            nullable: false,
            default: None,
            generated: None,
        }
    };
    ($name:literal, $ty:literal, null) => {
        Column {
            nullable: true,
            ..col!($name, $ty)
        }
    };
    ($name:literal, $ty:literal, default $expr:literal) => {
        Column {
            default: Some($expr),
            ..col!($name, $ty)
        }
    };
    ($name:literal, $ty:literal, generated $expr:literal) => {
        Column {
            nullable: true,
            generated: Some($expr),
            ..col!($name, $ty)
        }
    };
}

pub const TIMESCALEDB_VERSION: &str = "2.27.2";
pub use sinex_model::events::CAPTURED_FUTURE_SKEW_SECONDS;
use sinex_model::events::TIME_CLOCK_LABELS;
const EVENT_COLUMNS: &[Column] = &[
    col!("id", "uuid"),
    col!("source", "text"),
    col!("event_type", "text"),
    col!("host", "text", null),
    col!("payload", "jsonb"),
    col!("ts_orig", "timestamp with time zone", null),
    col!("ts_orig_tiebreak", "integer", null),
    col!("ts_quality", "text"),
    col!("time_suspect", "text", null),
    col!("ts_precision","text",default "'instant'::text"),
    col!("ts_orig_until", "timestamp with time zone", null),
    col!("ts_clock", "text"),
    col!("ts_coided","timestamp with time zone",generated "uuid_extract_timestamp(id)"),
    col!("ts_persisted","timestamp with time zone",default "now()"),
    col!("commit_seq", "bigint"),
    col!("source_material_id", "uuid", null),
    col!("anchor_byte", "bigint", null),
    col!("ordinal","smallint",default "0"),
    col!("record_state","text",default "'present'::text"),
    col!("offset_start", "bigint", null),
    col!("offset_end", "bigint", null),
    col!("offset_kind", "text", null),
    col!("record_hash", "bytea"),
    col!("source_event_ids", "uuid[]", null),
    col!("producer_version_id", "uuid", null),
    col!("run_id", "uuid"),
    col!("created_by_operation", "uuid", null),
    col!("written_by", "jsonb", null),
    col!("equivalence_key", "text", null),
    col!("payload_schema_id", "integer", null),
    col!("associated_blob_ids", "uuid[]", null),
];
const EVENT_CONSTRAINTS: &[(&str, &str)] = &[
    ("events_pkey", "PRIMARY KEY (id)"),
    (
        "one_origin",
        "CHECK ((source_material_id IS NOT NULL AND source_event_ids IS NULL) OR (source_material_id IS NULL AND source_event_ids IS NOT NULL AND cardinality(source_event_ids)>0))",
    ),
    (
        "no_null_parents",
        "CHECK (source_event_ids IS NULL OR cardinality(array_remove(source_event_ids,NULL))=cardinality(source_event_ids))",
    ),
    (
        "id_is_uuidv7",
        "CHECK (uuid_extract_version(id) IS NOT DISTINCT FROM 7)",
    ),
    ("captured_not_ahead", ""),
    ("event_quality", ""),
    (
        "event_time_pair",
        "CHECK ((ts_orig IS NULL) = (ts_quality = 'unknown'))",
    ),
    ("event_precision", ""),
    (
        "event_bounded_time",
        "CHECK ((ts_precision='bounded')=(ts_orig_until IS NOT NULL) AND (ts_orig_until IS NULL OR (ts_orig IS NOT NULL AND ts_orig_until>=ts_orig)))",
    ),
    ("event_clock", ""),
    (
        "event_anchor",
        "CHECK (anchor_byte>=0 AND ordinal>=0 AND (source_material_id IS NULL OR anchor_byte IS NOT NULL))",
    ),
    ("event_record_state", ""),
    (
        "event_producer",
        "CHECK (source_event_ids IS NULL OR (producer_version_id IS NOT NULL AND equivalence_key IS NOT NULL))",
    ),
    ("payload_object", "CHECK (jsonb_typeof(payload)='object')"),
    (
        "event_payload_schema",
        "FOREIGN KEY (payload_schema_id) REFERENCES sinex_schemas.payload_schemas(id)",
    ),
    (
        "registered_atuin_payload",
        "CHECK (event_type<>'command.executed' OR (payload_schema_id IS NOT DISTINCT FROM 2 AND source='terminal.atuin-history' AND equivalence_key IS NOT NULL AND jsonb_typeof(payload->'command') IS NOT DISTINCT FROM 'string' AND jsonb_typeof(payload->'native_id') IS NOT DISTINCT FROM 'string' AND payload ? 'exit_code' AND payload ? 'duration_ns'))",
    ),
    (
        "registered_browser_payload",
        "CHECK (event_type NOT IN ('page.visited','download.observed') OR (source='browser.history.chromium' AND equivalence_key IS NOT NULL AND jsonb_typeof(payload->'profile') IS NOT DISTINCT FROM 'string' AND jsonb_typeof(payload->'url') IS NOT DISTINCT FROM 'string' AND ((event_type='page.visited' AND payload_schema_id IS NOT DISTINCT FROM 7 AND payload ? 'visit_id' AND payload ? 'duration_us' AND payload ? 'redirect') OR (event_type='download.observed' AND payload_schema_id IS NOT DISTINCT FROM 8 AND payload ? 'guid' AND payload ? 'state'))))",
    ),
    (
        "event_material",
        "FOREIGN KEY (source_material_id) REFERENCES raw.source_material_registry(id)",
    ),
    ("event_run", "FOREIGN KEY (run_id) REFERENCES core.runs(id)"),
    (
        "event_producer_version",
        "FOREIGN KEY(producer_version_id) REFERENCES core.producer_versions(id)",
    ),
    (
        "registered_zsh_payload",
        "CHECK (event_type<>'command.imported' OR (payload_schema_id IS NOT DISTINCT FROM 4 AND source='terminal.zsh-history' AND equivalence_key IS NOT NULL AND jsonb_typeof(payload->'command') IS NOT DISTINCT FROM 'string' AND payload ? 'identity' AND payload ? 'continuity'))",
    ),
    (
        "registered_shell_payload",
        "CHECK (event_type<>'command.canonical' OR (source='fusion.shell-command' AND ((payload_schema_id IS NOT DISTINCT FROM 3 AND cardinality(source_event_ids)=1) OR (payload_schema_id IS NOT DISTINCT FROM 5 AND cardinality(source_event_ids) BETWEEN 1 AND 2)) AND jsonb_typeof(payload->'command') IS NOT DISTINCT FROM 'string' AND jsonb_typeof(payload->'observed_by') IS NOT DISTINCT FROM 'array'))",
    ),
];

const TABLES: &[Definition] = &[
    Definition {
        schema: "sinex_schemas",
        name: "applied_changes",
        columns: &[col!("number", "integer"), col!("description", "text")],
        constraints: &[("applied_changes_pkey", "PRIMARY KEY(number)")],
    },
    Definition {
        schema: "raw",
        name: "inputs",
        columns: &[
            col!("id", "uuid"),
            col!("binding", "text"),
            col!("locator", "text"),
            col!("kind", "text"),
        ],
        constraints: &[
            ("inputs_pkey", "PRIMARY KEY (id)"),
            ("input_locator", "UNIQUE (binding,locator)"),
            ("input_kind", "CHECK (kind IN ('sqlite','file'))"),
        ],
    },
    Definition {
        schema: "raw",
        name: "observations",
        columns: &[
            col!("id", "uuid"),
            col!("input_id", "uuid"),
            col!("seq", "bigint"),
            col!("observed_at", "timestamp with time zone"),
            col!("source_provenance", "jsonb"),
            col!("metadata", "jsonb"),
            col!("status", "text"),
            col!("reason", "text", null),
            col!("rows_read", "bigint", default "0"),
            col!("admitted", "bigint", default "0"),
            col!("repeated", "bigint", default "0"),
            col!("excluded", "bigint", default "0"),
            col!("removed", "bigint", default "0"),
            col!("record_errors", "bigint", default "0"),
            col!("commit_seq", "bigint", default "0"),
        ],
        constraints: &[
            ("observations_pkey", "PRIMARY KEY (id)"),
            (
                "observation_input",
                "FOREIGN KEY (input_id) REFERENCES raw.inputs(id)",
            ),
            ("observation_order", "UNIQUE (input_id,seq)"),
            (
                "observation_state",
                "CHECK (status IN ('reading','completed','unavailable','interrupted'))",
            ),
            (
                "observation_accounting",
                "CHECK (rows_read=admitted+repeated+excluded+record_errors AND excluded>=0 AND removed<=admitted AND removed>=0)",
            ),
        ],
    },
    Definition {
        schema: "sinex_schemas",
        name: "definition",
        columns: &[
            col!("version", "integer"),
            col!("fingerprint", "text"),
            col!("catalog", "jsonb"),
        ],
        constraints: &[
            ("definition_pkey", "PRIMARY KEY (version)"),
            ("one_version", "CHECK (version = 1)"),
        ],
    },
    Definition {
        schema: "core",
        name: "blobs",
        columns: &[
            col!("id", "uuid"),
            col!("blake3", "text"),
            col!("size_bytes", "bigint"),
            col!("root", "text"),
            col!("status", "text"),
        ],
        constraints: &[
            ("blobs_pkey", "PRIMARY KEY (id)"),
            ("blobs_name", "UNIQUE (blake3)"),
            (
                "blob_status",
                "CHECK (status = 'writing'::text OR status = 'present'::text)",
            ),
            ("blob_size", "CHECK (size_bytes >= 0)"),
        ],
    },
    Definition {
        schema: "raw",
        name: "source_material_registry",
        columns: &[
            col!("id", "uuid"),
            col!("material_kind", "text"),
            col!("source_identifier", "text"),
            col!("status", "text"),
            col!("ts_quality", "text"),
            col!("staged_at", "timestamp with time zone", default "now()"),
            col!("blob_id", "uuid", null),
            col!("input_id", "uuid", null),
            col!("observation_id", "uuid", null),
            col!("parser_provenance", "jsonb", default "'{}'::jsonb"),
            col!("total_bytes", "bigint"),
            col!("durable_bytes", "bigint"),
            col!("parsed_event_count", "bigint", default "0"),
        ],
        constraints: &[
            ("source_material_registry_pkey", "PRIMARY KEY (id)"),
            (
                "material_v7",
                "CHECK (uuid_extract_version(id) IS NOT DISTINCT FROM 7)",
            ),
            (
                "material_length",
                "CHECK (total_bytes >= 0 AND durable_bytes = total_bytes)",
            ),
            (
                "material_blob",
                "FOREIGN KEY (blob_id) REFERENCES core.blobs(id)",
            ),
            (
                "material_input",
                "FOREIGN KEY (input_id) REFERENCES raw.inputs(id)",
            ),
            (
                "material_observation",
                "FOREIGN KEY (observation_id) REFERENCES raw.observations(id)",
            ),
            (
                "row_material_custody",
                "CHECK (material_kind<>'stream_chunk' OR (input_id IS NOT NULL AND observation_id IS NOT NULL))",
            ),
            (
                "foundation_closed_material",
                "CHECK (status IN ('capturing','completed','recovered_partial') AND material_kind IN ('file','stream_chunk') AND ts_quality = 'unknown' AND ((status='capturing' AND blob_id IS NULL) OR (status<>'capturing' AND blob_id IS NOT NULL)))",
            ),
        ],
    },
    Definition {
        schema: "raw",
        name: "record_failures",
        columns: &[
            col!("observation_id", "uuid"),
            col!("material_id", "uuid"),
            col!("anchor_byte", "bigint"),
            col!("reason", "text"),
        ],
        constraints: &[
            (
                "record_failures_pkey",
                "PRIMARY KEY (material_id,anchor_byte)",
            ),
            (
                "failure_observation",
                "FOREIGN KEY (observation_id) REFERENCES raw.observations(id)",
            ),
            (
                "failure_material",
                "FOREIGN KEY (material_id) REFERENCES raw.source_material_registry(id)",
            ),
        ],
    },
    Definition {
        schema: "core",
        name: "operations_log",
        columns: &[
            col!("id", "uuid"),
            col!("operation_type", "text"),
            col!("actor", "jsonb"),
            col!("action_principal", "text"),
            col!("action_id", "uuid"),
            col!("request_format", "text"),
            col!("request_ref", "uuid"),
            col!("scope", "jsonb"),
            col!("admission_state", "text"),
            col!("outcome_format", "text", null),
            col!("outcome_ref", "uuid", null),
            col!("progress", "jsonb"),
        ],
        constraints: &[
            ("operations_log_pkey", "PRIMARY KEY (id)"),
            (
                "operation_action_identity",
                "UNIQUE (action_principal,action_id)",
            ),
            (
                "operation_request_material",
                "FOREIGN KEY (request_ref) REFERENCES raw.source_material_registry(id)",
            ),
            (
                "operation_outcome_material",
                "FOREIGN KEY (outcome_ref) REFERENCES raw.source_material_registry(id)",
            ),
            (
                "operation_scope",
                "CHECK (action_principal='foundation.operator' AND ((operation_type='replay' AND request_format='sinex.replay.request.v1') OR (operation_type IN ('delete','delete.undo') AND request_format='sinex.delete.request.v1')))",
            ),
            (
                "operation_state",
                "CHECK (admission_state IN ('recorded','admitted','refused'))",
            ),
            (
                "operation_outcome",
                "CHECK ((outcome_format IS NULL)=(outcome_ref IS NULL) AND (admission_state='recorded' OR outcome_ref IS NOT NULL))",
            ),
        ],
    },
    Definition {
        schema: "core",
        name: "deleted_occurrences",
        columns: &[
            col!("store", "smallint"),
            col!("source", "text"),
            col!("event_type", "text"),
            col!("equivalence_key", "text"),
            col!("material_id", "uuid"),
            col!("anchor", "bigint"),
            col!("kind", "text"),
            col!("operation_id", "uuid"),
            col!("deleted_at", "timestamp with time zone", default "now()"),
        ],
        constraints: &[
            (
                "deleted_occurrences_pkey",
                "PRIMARY KEY (store,source,event_type,equivalence_key)",
            ),
            (
                "deleted_kind",
                "CHECK (kind IN ('exclude','excise','purge'))",
            ),
            (
                "deleted_scope",
                "CHECK (store=0 AND source='terminal.atuin-history' AND event_type='command.executed' AND anchor>=0)",
            ),
            (
                "deleted_operation",
                "FOREIGN KEY (operation_id) REFERENCES core.operations_log(id)",
            ),
            (
                "deleted_material",
                "FOREIGN KEY (material_id) REFERENCES raw.source_material_registry(id)",
            ),
        ],
    },
    Definition {
        schema: "core",
        name: "operation_effects",
        columns: &[
            col!("operation_id", "uuid"),
            col!("step_no", "bigint"),
            col!("effect_format", "text"),
            col!("effects_ref", "uuid"),
        ],
        constraints: &[
            (
                "operation_effects_pkey",
                "PRIMARY KEY (operation_id,step_no)",
            ),
            (
                "effect_operation",
                "FOREIGN KEY (operation_id) REFERENCES core.operations_log(id)",
            ),
            (
                "effect_material",
                "FOREIGN KEY (effects_ref) REFERENCES raw.source_material_registry(id)",
            ),
            (
                "effect_step",
                "CHECK (step_no>=0 AND effect_format IN ('sinex.replay.effects.v1','sinex.delete.effects.v1'))",
            ),
        ],
    },
    Definition {
        schema: "core",
        name: "manifests",
        columns: &[
            col!("id", "integer"),
            col!("name", "text"),
            col!("manifest_type", "text"),
            col!("version", "text"),
            col!("artifact_kind", "text"),
            col!("artifact_digest", "text"),
            col!("artifact_descriptor", "jsonb"),
            col!("emitted_event_types", "text[]"),
            col!("consumed_event_types", "text[]"),
            col!("created_at", "timestamp with time zone", default "now()"),
        ],
        constraints: &[
            ("manifests_pkey", "PRIMARY KEY (id)"),
            (
                "foundation_native_artifact",
                "CHECK (artifact_kind = 'native_executable'::text)",
            ),
        ],
    },
    Definition {
        schema: "core",
        name: "runs",
        columns: &[
            col!("id", "uuid"),
            col!("manifest_id", "integer"),
            col!("service_name", "text"),
            col!("host", "text"),
            col!("started_at", "timestamp with time zone", default "now()"),
            col!("ended_at", "timestamp with time zone", null),
            col!("status", "text"),
            col!("effective_config_hash", "text"),
        ],
        constraints: &[
            ("runs_pkey", "PRIMARY KEY (id)"),
            (
                "run_manifest",
                "FOREIGN KEY (manifest_id) REFERENCES core.manifests(id)",
            ),
        ],
    },
    Definition {
        schema: "sinex_schemas",
        name: "payload_schemas",
        columns: &[
            col!("id", "integer"),
            col!("event_type", "text"),
            col!("version", "text"),
            col!("schema", "jsonb"),
        ],
        constraints: &[
            ("payload_schemas_pkey", "PRIMARY KEY (id)"),
            ("payload_schema_type", "UNIQUE (event_type,version)"),
        ],
    },
    Definition {
        schema: "core",
        name: "producer_versions",
        columns: &[
            col!("id", "uuid"),
            col!("name", "text"),
            col!("version", "text"),
            col!("run_id", "uuid"),
            col!("configuration", "jsonb"),
        ],
        constraints: &[
            ("producer_versions_pkey", "PRIMARY KEY (id)"),
            (
                "producer_run",
                "FOREIGN KEY (run_id) REFERENCES core.runs(id)",
            ),
        ],
    },
    Definition {
        schema: "core",
        name: "events",
        columns: EVENT_COLUMNS,
        constraints: EVENT_CONSTRAINTS,
    },
    Definition {
        schema: "reflection",
        name: "events",
        columns: EVENT_COLUMNS,
        constraints: EVENT_CONSTRAINTS,
    },
    Definition {
        schema: "core",
        name: "live_coordinates",
        columns: &[
            col!("store", "smallint"),
            col!("occ_hash", "bytea"),
            col!("event_id", "uuid"),
        ],
        constraints: &[
            ("live_coordinates_pkey", "PRIMARY KEY (store,occ_hash)"),
            (
                "coordinate_hash_length",
                "CHECK (octet_length(occ_hash)=16)",
            ),
        ],
    },
    Definition {
        schema: "core",
        name: "live_equivalence",
        columns: &[
            col!("store", "smallint"),
            col!("key_hash", "bytea"),
            col!("event_id", "uuid"),
            col!("read_order", "bigint"),
            col!("record_hash", "bytea"),
        ],
        constraints: &[
            ("live_equivalence_pkey", "PRIMARY KEY (store,key_hash)"),
            (
                "equivalence_hash_lengths",
                "CHECK (octet_length(key_hash)=16 AND octet_length(record_hash)=16)",
            ),
        ],
    },
    Definition {
        schema: "audit",
        name: "archived",
        columns: &[
            col!("id", "uuid"),
            col!("archived_seq", "bigint"),
            col!("archived_at","timestamp with time zone",default "now()"),
            col!("reason", "text"),
            col!("operation_id", "uuid", null),
            col!("replaced_by", "uuid", null),
        ],
        constraints: &[
            ("archived_pkey", "PRIMARY KEY (id)"),
            (
                "archive_reason",
                "CHECK (reason IN ('replay','supersede','delete','version_change'))",
            ),
        ],
    },
    Definition {
        schema: "core",
        name: "commit_counter",
        columns: &[col!("one", "boolean"), col!("seq", "bigint")],
        constraints: &[
            ("commit_counter_pkey", "PRIMARY KEY (one)"),
            ("one_counter", "CHECK (one)"),
            ("counter_nonnegative", "CHECK (seq >= 0)"),
        ],
    },
    Definition {
        schema: "core",
        name: "event_types",
        columns: &[col!("id", "integer"), col!("name", "text")],
        constraints: &[
            ("event_types_pkey", "PRIMARY KEY (id)"),
            ("event_types_name", "UNIQUE (name)"),
        ],
    },
    Definition {
        schema: "core",
        name: "commit_log",
        columns: &[
            col!("commit_seq", "bigint"),
            col!("event_id", "uuid"),
            col!("change", "smallint"),
            col!("store", "smallint"),
            col!("event_type_id", "integer"),
        ],
        constraints: &[
            ("commit_log_pkey", "PRIMARY KEY (commit_seq)"),
            ("commit_change", "CHECK (change IN (0,1,2))"),
            ("commit_store", "CHECK (store IN (0,1))"),
            (
                "commit_event_type",
                "FOREIGN KEY (event_type_id) REFERENCES core.event_types(id)",
            ),
        ],
    },
    Definition {
        schema: "raw",
        name: "source_cursors",
        columns: &[
            col!("binding", "text"),
            col!("input", "text"),
            col!("position", "jsonb"),
            col!("carry", "jsonb"),
            col!("updated_at", "timestamp with time zone", default "now()"),
        ],
        constraints: &[("source_cursors_pkey", "PRIMARY KEY (binding, input)")],
    },
    Definition {
        schema: "derivation",
        name: "shell_dirty",
        columns: &[col!("parent_id", "uuid"), col!("dirty_seq", "bigint")],
        constraints: &[("shell_dirty_pkey", "PRIMARY KEY (parent_id)")],
    },
    Definition {
        schema: "derivation",
        name: "shell_progress",
        columns: &[
            col!("one", "boolean"),
            col!("basis","bigint",default "0"),
            col!("last_evaluated","bigint",default "0"),
            col!("phase","text",default "'settled'::text"),
            col!("scanned","bigint",default "0"),
            col!("replaced","bigint",default "0"),
            col!("failure", "text", null),
        ],
        constraints: &[
            ("shell_progress_pkey", "PRIMARY KEY(one)"),
            ("shell_progress_one", "CHECK(one)"),
            (
                "shell_progress_nonnegative",
                "CHECK(basis>=0 AND last_evaluated>=0 AND scanned>=0 AND replaced>=0)",
            ),
            (
                "shell_progress_phase",
                "CHECK(phase IN ('settled','scan','count2','pair2','count60','pair60','single','publish','archive'))",
            ),
        ],
    },
    Definition {
        schema: "derivation",
        name: "session_dirty",
        columns: &[
            col!("day", "text"),
            col!("dirty_seq", "bigint"),
            col!("through_day", "text", null),
        ],
        constraints: &[("session_dirty_pkey", "PRIMARY KEY(day)")],
    },
    Definition {
        schema: "derivation",
        name: "session_progress",
        columns: &[
            col!("one", "boolean"),
            col!("basis","bigint",default "0"),
            col!("day", "text", null),
            col!("through_day", "text", null),
            col!("phase","text",default "'settled'::text"),
            col!("failure", "text", null),
        ],
        constraints: &[
            ("session_progress_pkey", "PRIMARY KEY(one)"),
            ("session_progress_one", "CHECK(one)"),
        ],
    },
    Definition {
        schema: "derivation",
        name: "read_model_position",
        columns: &[
            col!("name", "text"),
            col!("generation", "uuid"),
            col!("configuration", "text"),
            col!("position", "bigint"),
            col!("serving", "boolean"),
            col!("failure", "text", null),
        ],
        constraints: &[
            ("read_model_position_pkey", "PRIMARY KEY (name)"),
            ("read_model_position_nonnegative", "CHECK (position>=0)"),
        ],
    },
    Definition {
        schema: "core",
        name: "search_text",
        columns: &[
            col!("generation", "uuid"),
            col!("id", "uuid"),
            col!("ts_orig", "timestamp with time zone", null),
            col!("ts_orig_tiebreak", "integer", null),
            col!("source", "text"),
            col!("event_type", "text"),
            col!("tsv", "tsvector", null),
            col!("text", "text"),
            col!("command", "text"),
            col!("command_normalized", "text"),
            col!("cwd_normalized", "text", null),
            col!("cwd", "text", null),
            col!("exit_code", "bigint", null),
        ],
        constraints: &[("search_text_pkey", "PRIMARY KEY (generation,id)")],
    },
    Definition {
        schema: "core",
        name: "event_project",
        columns: &[
            col!("generation", "uuid"),
            col!("event_id", "uuid"),
            col!("ts_orig", "timestamp with time zone", null),
            col!("project", "text"),
        ],
        constraints: &[("event_project_pkey", "PRIMARY KEY (generation,event_id)")],
    },
];

fn enum_check(column: &str, labels: &[&str]) -> String {
    let values: Vec<String> = labels
        .iter()
        .map(|label| ["'", &label.replace("'", "''"), "'"].concat())
        .collect();
    ["CHECK (", column, " IN (", &values.join(","), "))"].concat()
}

fn record_state_check() -> Result<String> {
    use sinex_model::events::RecordState;
    let mut labels = Vec::new();
    for state in [RecordState::Present, RecordState::Removed] {
        match serde_json::to_value(state)
            .map_err(|_| Error::Schema("record-state catalog cannot serialize".into()))?
        {
            serde_json::Value::String(label) => labels.push(label),
            _ => {
                return Err(Error::Schema(
                    "record-state catalog must use string labels".into(),
                ));
            }
        }
    }
    Ok(enum_check(
        "record_state",
        &labels.iter().map(String::as_str).collect::<Vec<_>>(),
    ))
}

// This is the sole supported installed predecessor, published at 4b5bf0619.
// Reuse unchanged declarations; spell out only the released differences.
const OLD_SHELL_CHECK: &str = "CHECK (event_type<>'command.canonical' OR (payload_schema_id IS NOT DISTINCT FROM 3 AND source='fusion.shell-command' AND cardinality(source_event_ids)=1 AND jsonb_typeof(payload->'command') IS NOT DISTINCT FROM 'string' AND jsonb_typeof(payload->'observed_by') IS NOT DISTINCT FROM 'array'))";
fn installed_tables(stage: usize) -> Vec<Definition> {
    TABLES
        .iter()
        .filter(|d| {
            !(stage == 0 && d.name == "applied_changes"
                || stage < 2 && d.name == "shell_progress"
                || stage < 3 && d.name.starts_with("session_"))
        })
        .map(|d| Definition {
            schema: d.schema,
            name: d.name,
            columns: if stage < 2 && d.name == "shell_dirty" {
                &d.columns[..1]
            } else {
                d.columns
            },
            constraints: d.constraints,
        })
        .collect()
}
fn nullable_at(stage: usize, table: &Definition, column: &Column) -> bool {
    column.nullable && !(stage < 5 && table.name == "search_text" && column.name == "tsv")
}
fn storage_at(stage: usize) -> Vec<String> {
    crate::event_storage::declarations()
        .into_iter()
        .filter(|sql| stage >= 5 || sql != crate::event_storage::FULL_TEXT_UNAVAILABLE_INDEX)
        .collect()
}
fn declarations() -> Result<Vec<String>> {
    declarations_at(6)
}
fn declarations_at(stage: usize) -> Result<Vec<String>> {
    installed_tables(stage)
        .iter()
        .map(|d| {
            let mut table = Table::create();
            table.table((Alias::new(d.schema), Alias::new(d.name)));
            for c in d.columns {
                let mut column = ColumnDef::new(Alias::new(c.name));
                column.custom(Alias::new(c.ty));
                if !nullable_at(stage, d, c) {
                    column.not_null();
                }
                if let Some(expr) = c.default {
                    column.default(Expr::cust(expr));
                }
                if let Some(expr) = c.generated {
                    column.generated(Expr::cust(expr), true);
                }
                table.col(&mut column);
            }
            let mut sql = table.to_string(PostgresQueryBuilder);
            // Only constants from this module enter DDL, never operator input.
            sql.pop();
            for (name, body) in d.constraints {
                if stage==0 && *name=="registered_zsh_payload" || stage<4 && *name=="registered_browser_payload" { continue; }
                sql.push_str(", CONSTRAINT \"");
                sql.push_str(name);
                sql.push_str("\" ");
                let generated = match *name {
                    "registered_shell_payload" if stage==0 => OLD_SHELL_CHECK.to_owned(),
                    "input_kind" if stage==0 => "CHECK (kind='sqlite')".to_owned(),
                    "event_quality" => enum_check("ts_quality",sinex_model::events::TIME_QUALITY_LABELS),
                    "event_precision" => enum_check("ts_precision",sinex_model::events::TIME_PRECISION_LABELS),
                    "event_record_state" => record_state_check()?,
                    "event_clock" => enum_check("ts_clock",TIME_CLOCK_LABELS),
                    "captured_not_ahead" => ["CHECK (ts_quality IS DISTINCT FROM 'captured' OR ts_clock <> 'sinex' OR ts_orig IS NULL OR ts_orig <= uuid_extract_timestamp(id)+interval '",&CAPTURED_FUTURE_SKEW_SECONDS.to_string()," seconds')"].concat(),
                    _ => (*body).to_owned(),
                };
                sql.push_str(&generated);
            }
            sql.push(')');
            Ok(sql)
        })
        .collect()
}

async fn configure_events(conn: &mut PgConnection) -> Result<()> {
    for sql in crate::event_storage::declarations() {
        conn.execute(sql.as_str()).await?;
    }
    Ok(())
}

fn fingerprint() -> Result<String> {
    fingerprint_at(6)
}
fn fingerprint_at(stage: usize) -> Result<String> {
    Ok(blake3::hash(
        [
            declarations_at(stage)?,
            storage_at(stage),
            vec![TIMESCALEDB_VERSION.to_owned()],
            if stage >= 6 {
                // Attest the relaxed registered payload contract without rewriting
                // any predecessor stage's canonical catalog fingerprint.
                let mut schema = atuin_payload_schema_at(stage);
                schema.sort_all_objects();
                vec![schema.to_string()]
            } else {
                vec![]
            },
        ]
        .concat()
        .join("\n")
        .as_bytes(),
    )
    .to_hex()
    .to_string())
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SchemaStatus {
    pub version: i32,
    pub fingerprint: String,
    pub table_count: usize,
    pub pending_changes: Vec<SchemaChange>,
    pub checked_changes: Vec<SchemaChangeCheck>,
}
#[derive(Debug, Clone, serde::Serialize)]
pub struct SchemaChangeCheck {
    pub number: i32,
    pub outcome: &'static str,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SchemaChange {
    pub number: i32,
    pub description: &'static str,
}
pub fn schema_changes() -> Vec<SchemaChange> {
    vec![
        SchemaChange {
            number: 1,
            description: "register retained zsh and two-parent shell contracts",
        },
        SchemaChange {
            number: 2,
            description: "retain shell dirty frontier and initialize grouping progress",
        },
        SchemaChange {
            number: 3,
            description: "register shell sessions and durable logical-day reconciliation",
        },
        SchemaChange {
            number: 4,
            description: "register retained Chromium visits and persisted download states",
        },
        SchemaChange {
            number: 5,
            description: "retain exact metadata when full-text representation is unavailable",
        },
        SchemaChange {
            number: 6,
            description: "preserve signed Atuin provider exit statuses",
        },
    ]
}

/// Creates only an empty foundation database. Existing catalogs are checked, never reset.
/// The isolated runner must provision these two LOGIN roles and their credentials.
pub async fn bootstrap(admin_url: &str) -> Result<SchemaStatus> {
    let mut conn = PgConnection::connect(admin_url).await?;
    require_pg18(&mut conn).await?;
    let mut tx = conn.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(73018924011)")
        .execute(&mut *tx)
        .await?;
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_namespace WHERE nspname IN ('core','raw','sinex_schemas','reflection','audit','derivation')",
    )
    .fetch_one(&mut *tx)
    .await?;
    let mut checked_changes = Vec::new();
    if count == 0 {
        // The same real entry dispatch runs before creating any target objects.
        for entry in schema_changes() {
            checked_changes.push(apply_change_entry(&mut tx, &entry).await?);
        }
        let extension_sql = [
            "CREATE EXTENSION IF NOT EXISTS timescaledb VERSION '",
            TIMESCALEDB_VERSION,
            "'",
        ]
        .concat();
        tx.execute(extension_sql.as_str()).await?;
        tx.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm").await?;
        let installed: String =
            sqlx::query_scalar("SELECT extversion FROM pg_extension WHERE extname='timescaledb'")
                .fetch_one(&mut *tx)
                .await?;
        if installed != TIMESCALEDB_VERSION {
            return Err(Error::Schema("TimescaleDB 2.27.2 is required".into()));
        }
        sqlx::raw_sql("CREATE SCHEMA core; CREATE SCHEMA raw; CREATE SCHEMA sinex_schemas; CREATE SCHEMA reflection; CREATE SCHEMA audit; CREATE SCHEMA derivation; REVOKE ALL ON SCHEMA core, raw, sinex_schemas,reflection,audit,derivation FROM PUBLIC").execute(&mut *tx).await?;
        for ddl in declarations()? {
            tx.execute(ddl.as_str()).await?;
        }
        configure_events(&mut tx).await?;
        sqlx::query("INSERT INTO sinex_schemas.payload_schemas (id,event_type,version,schema) VALUES (2,'command.executed','atuin-v1',$1)")
            .bind(crate::atuin::payload_schema()).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO sinex_schemas.payload_schemas(id,event_type,version,schema) VALUES(3,'command.canonical','shell-command-v1-single-source',$1)").bind(predecessor_shell_payload()).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO sinex_schemas.payload_schemas(id,event_type,version,schema) VALUES(5,'command.canonical','shell-command-v1-atuin-zsh',$1)").bind(crate::shell::payload_schema()).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO sinex_schemas.payload_schemas(id,event_type,version,schema) VALUES(4,'command.imported','zsh-extended-v1',$1)").bind(crate::zsh::payload_schema()).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO sinex_schemas.payload_schemas(id,event_type,version,schema) VALUES(6,'activity.session.boundary','shell-session-v1',$1)").bind(crate::session::payload_schema()).execute(&mut *tx).await?;
        for (id, kind, download) in [(7, "page.visited", false), (8, "download.observed", true)] {
            sqlx::query("INSERT INTO sinex_schemas.payload_schemas(id,event_type,version,schema) VALUES($1,$2,'chromium-history-v1',$3)").bind(id).bind(kind).bind(sinex_model::browser::payload_schema(download)).execute(&mut *tx).await?;
        }
        sqlx::query("INSERT INTO core.event_types (id,name) VALUES (1,'synthetic.observed'),(2,'command.executed'),(3,'command.canonical'),(4,'command.imported'),(5,'activity.session.boundary'),(6,'page.visited'),(7,'download.observed')")
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO core.commit_counter (one,seq) VALUES (true,0)")
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO derivation.session_progress(one) VALUES(true)")
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO derivation.shell_progress(one) VALUES(true)")
            .execute(&mut *tx)
            .await?;
        for entry in schema_changes() {
            sqlx::query(
                "INSERT INTO sinex_schemas.applied_changes(number,description) VALUES($1,$2)",
            )
            .bind(entry.number)
            .bind(entry.description)
            .execute(&mut *tx)
            .await?;
        }
        let catalog = capture_catalog(&mut tx).await?;
        let catalog = serde_json::to_value(catalog)
            .map_err(|_| Error::Schema("cannot encode canonical catalog".into()))?;
        sqlx::query(
            "INSERT INTO sinex_schemas.definition (version,fingerprint,catalog) VALUES (1,$1,$2)",
        )
        .bind(fingerprint()?)
        .bind(catalog)
        .execute(&mut *tx)
        .await?;
        sqlx::raw_sql("DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='sinex_read_model') THEN CREATE ROLE sinex_read_model LOGIN; END IF; END $$; GRANT USAGE ON SCHEMA core,raw,audit,sinex_schemas,derivation,reflection TO sinex_read_model; GRANT SELECT ON ALL TABLES IN SCHEMA core,raw,audit,sinex_schemas,derivation,reflection TO sinex_read_model; GRANT INSERT,UPDATE,DELETE ON core.search_text,core.event_project,derivation.read_model_position TO sinex_read_model").execute(&mut *tx).await?;
        sqlx::raw_sql("GRANT USAGE ON SCHEMA core,raw,sinex_schemas TO sinex_writer,sinex_reader; GRANT USAGE ON SCHEMA reflection,audit,derivation TO sinex_writer,sinex_reader; GRANT SELECT ON ALL TABLES IN SCHEMA core,raw,sinex_schemas,reflection,audit,derivation TO sinex_writer,sinex_reader; GRANT INSERT ON core.deleted_occurrences,core.producer_versions,derivation.shell_dirty,derivation.session_dirty,core.operation_effects,core.events,reflection.events,core.live_coordinates,core.live_equivalence,audit.archived,core.commit_log,raw.source_material_registry,core.manifests,raw.inputs,raw.record_failures TO sinex_writer; GRANT INSERT,UPDATE ON core.operations_log,raw.source_cursors,core.blobs,core.runs,raw.observations TO sinex_writer; GRANT DELETE ON core.deleted_occurrences,core.live_equivalence,derivation.shell_dirty,derivation.session_dirty,core.live_coordinates TO sinex_writer; GRANT UPDATE ON core.commit_counter,core.live_equivalence,core.live_coordinates,derivation.shell_dirty,derivation.shell_progress,derivation.session_dirty,derivation.session_progress TO sinex_writer; GRANT UPDATE(parsed_event_count,blob_id,status,total_bytes,durable_bytes) ON raw.source_material_registry TO sinex_writer").execute(&mut *tx).await?;
    }
    let mut status = inspect(&mut tx).await?;
    tx.commit().await?;
    status.checked_changes = checked_changes;
    Ok(status)
}

pub async fn check_schema(admin_url: &str) -> Result<SchemaStatus> {
    let mut conn = PgConnection::connect(admin_url).await?;
    require_pg18(&mut conn).await?;
    inspect(&mut conn).await
}

async fn require_pg18(conn: &mut PgConnection) -> Result<()> {
    let version: String = sqlx::query_scalar("SHOW server_version_num")
        .fetch_one(conn)
        .await?;
    if version
        .parse::<u32>()
        .map_err(|_| Error::Schema("unreadable PostgreSQL version".into()))?
        < 180000
    {
        return Err(Error::Schema("PostgreSQL 18 or newer is required".into()));
    }
    Ok(())
}

// PostgreSQL's deparser canonicalizes expressions (for example IN becomes ANY).
// Capture that exact representation only while executing the original Rust DDL,
// within the same fresh-bootstrap transaction. Product roles cannot edit this row.
// Fingerprint mismatch is refused before trusting this protected catalog receipt.
#[derive(serde::Serialize, serde::Deserialize)]
struct Catalog {
    constraints: BTreeMap<String, String>,
    expressions: BTreeMap<String, Option<String>>,
    objects: BTreeMap<String, serde_json::Value>,
}

async fn capture_catalog(conn: &mut PgConnection) -> Result<Catalog> {
    let rows = sqlx::query("SELECT n.nspname || '.' || c.relname || '.' || con.conname AS key,pg_get_constraintdef(con.oid) AS definition FROM pg_constraint con JOIN pg_class c ON c.oid=con.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('core','raw','sinex_schemas','reflection','audit','derivation') AND con.contype <> 'n'").fetch_all(&mut *conn).await?;
    let mut constraints = BTreeMap::new();
    for r in rows {
        constraints.insert(r.try_get("key")?, r.try_get("definition")?);
    }
    let rows = sqlx::query("SELECT n.nspname || '.' || c.relname || '.' || a.attname AS key,pg_get_expr(ad.adbin,ad.adrelid) AS expression FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_attrdef ad ON ad.adrelid=c.oid AND ad.adnum=a.attnum WHERE n.nspname IN ('core','raw','sinex_schemas','reflection','audit','derivation') AND a.attnum>0 AND NOT a.attisdropped").fetch_all(&mut *conn).await?;
    let mut expressions = BTreeMap::new();
    for r in rows {
        expressions.insert(r.try_get("key")?, r.try_get("expression")?);
    }
    let mut objects = BTreeMap::new();
    for (kind, sql) in [
        (
            "collation",
            "SELECT n.nspname || '.' || c.relname || '.' || a.attname AS key,jsonb_build_object('schema',cn.nspname,'name',co.collname) AS value FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_collation co ON co.oid=a.attcollation LEFT JOIN pg_namespace cn ON cn.oid=co.collnamespace WHERE n.nspname IN ('core','raw','sinex_schemas','reflection','audit','derivation') AND c.relkind IN ('r','v') AND a.attnum>0 AND NOT a.attisdropped",
        ),
        (
            "index",
            "SELECT n.nspname || '.' || c.relname AS key,jsonb_build_object('definition',pg_get_indexdef(c.oid),'valid',i.indisvalid,'ready',i.indisready,'live',i.indislive,'options',c.reloptions,'tablespace',c.reltablespace) AS value FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('core','raw','sinex_schemas','reflection','audit','derivation')",
        ),
        (
            "function",
            "SELECT p.oid::regprocedure::text AS key,jsonb_build_object('definition',pg_get_functiondef(p.oid),'owner',pg_get_userbyid(p.proowner),'acl',p.proacl) AS value FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname IN ('core','raw','sinex_schemas','reflection','audit','derivation')",
        ),
        (
            "trigger",
            "SELECT n.nspname || '.' || c.relname || '.' || t.tgname AS key,jsonb_build_object('definition',pg_get_triggerdef(t.oid),'enabled',t.tgenabled::text) AS value FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('core','raw','sinex_schemas','reflection','audit','derivation') AND NOT t.tgisinternal",
        ),
        (
            "view",
            "SELECT n.nspname || '.' || c.relname AS key,jsonb_build_object('definition',pg_get_viewdef(c.oid),'options',c.reloptions) AS value FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('core','raw','sinex_schemas','reflection','audit','derivation') AND c.relkind='v'",
        ),
        (
            "policy",
            "SELECT n.nspname || '.' || c.relname || '.' || p.polname AS key,jsonb_build_object('roles',p.polroles,'command',p.polcmd::text,'permissive',p.polpermissive,'using',pg_get_expr(p.polqual,p.polrelid),'check',pg_get_expr(p.polwithcheck,p.polrelid)) AS value FROM pg_policy p JOIN pg_class c ON c.oid=p.polrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('core','raw','sinex_schemas','reflection','audit','derivation')",
        ),
        (
            "timescale",
            "SELECT h.schema_name || '.' || h.table_name AS key,jsonb_build_object('dimensions',h.num_dimensions,'compression_state',h.compression_state,'column',d.column_name,'type',d.column_type::text,'interval',d.interval_length,'partition_schema',d.partitioning_func_schema,'partition_function',d.partitioning_func,'compression',to_jsonb(cs)-'relid'-'compress_relid') AS value FROM _timescaledb_catalog.hypertable h JOIN _timescaledb_catalog.dimension d ON d.hypertable_id=h.id LEFT JOIN _timescaledb_catalog.compression_settings cs ON cs.relid=(quote_ident(h.schema_name)||'.'||quote_ident(h.table_name))::regclass WHERE h.schema_name IN ('core','reflection')",
        ),
        (
            "compression_policy",
            "SELECT h.schema_name || '.' || h.table_name || '.' || j.proc_name AS key,jsonb_build_object('schema',j.proc_schema,'config',j.config,'interval',j.schedule_interval::text,'scheduled',j.scheduled,'fixed',j.fixed_schedule,'timezone',j.timezone) AS value FROM _timescaledb_catalog.bgw_job j JOIN _timescaledb_catalog.hypertable h ON h.id=j.hypertable_id WHERE h.schema_name IN ('core','reflection')",
        ),
        (
            "chunk_skipping",
            "SELECT h.schema_name || '.' || h.table_name || '.' || s.column_name AS key,to_jsonb(s.valid) AS value FROM _timescaledb_catalog.chunk_column_stats s JOIN _timescaledb_catalog.hypertable h ON h.id=s.hypertable_id WHERE h.schema_name IN ('core','reflection') AND s.chunk_id IS NULL",
        ),
    ] {
        for row in sqlx::query(sql).fetch_all(&mut *conn).await? {
            let key: String = row.try_get("key")?;
            objects.insert([kind, ".", &key].concat(), row.try_get("value")?);
        }
    }
    Ok(Catalog {
        constraints,
        expressions,
        objects,
    })
}

pub(crate) async fn inspect(conn: &mut PgConnection) -> Result<SchemaStatus> {
    let extension: Option<String> =
        sqlx::query_scalar("SELECT extversion FROM pg_extension WHERE extname='timescaledb'")
            .fetch_optional(&mut *conn)
            .await?;
    if extension.as_deref() != Some(TIMESCALEDB_VERSION) {
        return Err(Error::Schema("TimescaleDB 2.27.2 is required".into()));
    }
    let stored: String =
        sqlx::query_scalar("SELECT fingerprint FROM sinex_schemas.definition WHERE version=1")
            .fetch_one(&mut *conn)
            .await?;
    let mut stage = None;
    for candidate in 0..=6 {
        if stored == fingerprint_at(candidate)? {
            stage = Some(candidate);
        }
    }
    let stage = stage.ok_or_else(|| {
        Error::Schema("unknown installed schema fingerprint; no repair performed".into())
    })?;
    let definitions = installed_tables(stage);
    if stage > 0 {
        let rows: Vec<(i32, String)> = sqlx::query_as(
            "SELECT number,description FROM sinex_schemas.applied_changes ORDER BY number",
        )
        .fetch_all(&mut *conn)
        .await?;
        let expected: Vec<_> = schema_changes()
            .into_iter()
            .take(stage)
            .map(|c| (c.number, c.description.to_owned()))
            .collect();
        if rows != expected {
            return Err(Error::Schema(
                "schema change progress is not the exact ordered prefix".into(),
            ));
        }
    }
    let tables: Vec<String> = sqlx::query_scalar("SELECT n.nspname || '.' || c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('core','raw','sinex_schemas','reflection','audit','derivation') AND c.relkind IN ('r','p','v','m','f','S') ORDER BY 1").fetch_all(&mut *conn).await?;
    let mut expected: Vec<String> = definitions
        .iter()
        .map(|t| [t.schema, ".", t.name].concat())
        .collect();
    expected.push("core.live_events".into());
    expected.sort();
    if tables != expected {
        return Err(Error::Schema(format!(
            "catalog tables differ: expected {expected:?}, found {tables:?}; no repair performed"
        )));
    }
    let receipt =
        sqlx::query("SELECT fingerprint,catalog FROM sinex_schemas.definition WHERE version=1")
            .fetch_one(&mut *conn)
            .await?;
    let stored: String = receipt.try_get("fingerprint")?;
    let catalog: Catalog = serde_json::from_value(receipt.try_get("catalog")?)
        .map_err(|_| Error::Schema("canonical catalog receipt is invalid".into()))?;
    let actual_catalog = capture_catalog(conn).await?;
    if catalog.constraints != actual_catalog.constraints
        || catalog.expressions != actual_catalog.expressions
        || catalog.objects != actual_catalog.objects
    {
        return Err(Error::Schema(
            "constraint or column expression differs from canonical bootstrap declaration".into(),
        ));
    }
    // The privileged, fixed-purpose helper is invoked only after its exact body,
    // owner and ACL have matched the protected bootstrap receipt above.
    let preloaded: bool = sqlx::query_scalar("SELECT sinex_schemas.timescale_preloaded()")
        .fetch_one(&mut *conn)
        .await?;
    if !preloaded {
        return Err(Error::Schema("TimescaleDB must be preloaded".into()));
    }
    for d in &definitions {
        let cols = sqlx::query("SELECT a.attname,format_type(a.atttypid,a.atttypmod) AS ty,a.attnotnull,a.attgenerated::text,a.attidentity::text,pg_get_expr(ad.adbin,ad.adrelid) AS default_expr FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_attrdef ad ON ad.adrelid=c.oid AND ad.adnum=a.attnum WHERE n.nspname=$1 AND c.relname=$2 AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum").bind(d.schema).bind(d.name).fetch_all(&mut *conn).await?;
        if cols.len() != d.columns.len() {
            return Err(Error::Schema(format!(
                "{}.{} column count differs",
                d.schema, d.name
            )));
        }
        for (actual, c) in cols.iter().zip(d.columns) {
            let default: Option<String> = actual.try_get("default_expr")?;
            if actual.try_get::<String, _>("attname")? != c.name
                || actual.try_get::<String, _>("ty")? != c.ty
                || actual.try_get::<bool, _>("attnotnull")? == nullable_at(stage, d, c)
                || catalog
                    .expressions
                    .get(&[d.schema, ".", d.name, ".", c.name].concat())
                    != Some(&default)
                || actual.try_get::<String, _>("attgenerated")?
                    != if c.generated.is_some() { "s" } else { "" }
                || !actual.try_get::<String, _>("attidentity")?.is_empty()
            {
                return Err(Error::Schema(format!(
                    "{}.{}.{} definition differs",
                    d.schema, d.name, c.name
                )));
            }
        }
        let constraints = sqlx::query("SELECT con.conname,pg_get_constraintdef(con.oid) AS definition FROM pg_constraint con JOIN pg_class c ON c.oid=con.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2 AND con.contype <> 'n'").bind(d.schema).bind(d.name).fetch_all(&mut *conn).await?;
        if constraints.len()
            != d.constraints.len()
                - usize::from(stage == 0 && matches!(d.name, "events"))
                - usize::from(stage < 4 && matches!(d.name, "events"))
        {
            return Err(Error::Schema(format!(
                "{}.{} constraints differ",
                d.schema, d.name
            )));
        }
        for (name, _body) in d.constraints {
            if stage == 0 && *name == "registered_zsh_payload"
                || stage < 4 && *name == "registered_browser_payload"
            {
                continue;
            }
            if !constraints.iter().any(|r| {
                r.get::<String, _>("conname") == *name
                    && Some(&r.get::<String, _>("definition"))
                        == catalog
                            .constraints
                            .get(&[d.schema, ".", d.name, ".", name].concat())
            }) {
                return Err(Error::Schema(format!(
                    "{}.{} constraint {name} differs",
                    d.schema, d.name
                )));
            }
        }
        let index_or_owner_drift: bool = sqlx::query_scalar("SELECT c.relrowsecurity OR c.relforcerowsecurity OR owner.rolname IN ('sinex_reader','sinex_writer','sinex_read_model') OR EXISTS(SELECT 1 FROM pg_index i WHERE i.indrelid=c.oid AND (NOT i.indisvalid OR NOT i.indisready OR NOT i.indislive)) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_roles owner ON owner.oid=c.relowner WHERE n.nspname=$1 AND c.relname=$2").bind(d.schema).bind(d.name).fetch_one(&mut *conn).await?;
        if index_or_owner_drift {
            return Err(Error::Schema(format!(
                "{}.{} index, owner or row-security declaration differs",
                d.schema, d.name
            )));
        }
    }
    let metadata_mutation_grant: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN LATERAL aclexplode(c.relacl) acl WHERE n.nspname='sinex_schemas' AND c.relname IN ('definition','applied_changes') AND acl.grantee<>c.relowner AND acl.privilege_type<>'SELECT') OR EXISTS(SELECT 1 FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN LATERAL aclexplode(a.attacl) acl WHERE n.nspname='sinex_schemas' AND c.relname IN ('definition','applied_changes') AND acl.grantee<>c.relowner AND acl.privilege_type<>'SELECT')").fetch_one(&mut *conn).await?;
    if metadata_mutation_grant {
        return Err(Error::Schema(
            "canonical catalog receipt has a non-owner mutation grant".into(),
        ));
    }
    let registered: Option<serde_json::Value> = sqlx::query_scalar("SELECT schema FROM sinex_schemas.payload_schemas WHERE id=2 AND event_type='command.executed' AND version='atuin-v1'").fetch_optional(&mut *conn).await?;
    if registered.as_ref() != Some(&atuin_payload_schema_at(stage)) {
        return Err(Error::Schema(
            "registered Atuin payload schema differs".into(),
        ));
    }
    if stage > 0 {
        let zsh:Option<serde_json::Value>=sqlx::query_scalar("SELECT schema FROM sinex_schemas.payload_schemas WHERE id=4 AND event_type='command.imported' AND version='zsh-extended-v1'").fetch_optional(&mut *conn).await?;
        if zsh.as_ref() != Some(&crate::zsh::payload_schema()) {
            return Err(Error::Schema(
                "registered zsh payload schema differs".into(),
            ));
        }
        let canonical:Option<serde_json::Value>=sqlx::query_scalar("SELECT schema FROM sinex_schemas.payload_schemas WHERE id=5 AND event_type='command.canonical' AND version='shell-command-v1-atuin-zsh'").fetch_optional(&mut *conn).await?;
        if canonical.as_ref() != Some(&crate::shell::payload_schema()) {
            return Err(Error::Schema(
                "registered canonical payload schema differs".into(),
            ));
        }
    }
    {
        let canonical:Option<serde_json::Value>=sqlx::query_scalar("SELECT schema FROM sinex_schemas.payload_schemas WHERE id=3 AND event_type='command.canonical' AND version='shell-command-v1-single-source'").fetch_optional(&mut *conn).await?;
        if canonical.as_ref() != Some(&predecessor_shell_payload()) {
            return Err(Error::Schema("released singleton payload differs".into()));
        }
    }
    let payload_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM sinex_schemas.payload_schemas")
            .fetch_one(&mut *conn)
            .await?;
    if payload_count
        != if stage == 0 {
            2
        } else if stage < 3 {
            4
        } else if stage < 4 {
            5
        } else {
            7
        }
    {
        return Err(Error::Schema(
            "payload schema catalog contains an unsupported registration".into(),
        ));
    }
    let event_types: Vec<(i32, String)> =
        sqlx::query_as("SELECT id,name FROM core.event_types ORDER BY id")
            .fetch_all(&mut *conn)
            .await?;
    let mut expected_types = vec![
        (1, "synthetic.observed".to_owned()),
        (2, "command.executed".to_owned()),
        (3, "command.canonical".to_owned()),
    ];
    if stage > 0 {
        expected_types.push((4, "command.imported".to_owned()));
    }
    if stage >= 3 {
        expected_types.push((5, "activity.session.boundary".to_owned()));
        let schema:Option<serde_json::Value>=sqlx::query_scalar("SELECT schema FROM sinex_schemas.payload_schemas WHERE id=6 AND event_type='activity.session.boundary' AND version='shell-session-v1'").fetch_optional(&mut *conn).await?;
        if schema.as_ref() != Some(&crate::session::payload_schema()) {
            return Err(Error::Schema("session payload registration differs".into()));
        }
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM derivation.session_progress WHERE one")
                .fetch_one(&mut *conn)
                .await?;
        if count != 1 {
            return Err(Error::Schema("session progress absent".into()));
        }
    }
    if stage >= 4 {
        expected_types.extend([
            (6, "page.visited".to_owned()),
            (7, "download.observed".to_owned()),
        ]);
        for (id, kind, download) in [(7, "page.visited", false), (8, "download.observed", true)] {
            let schema:Option<serde_json::Value>=sqlx::query_scalar("SELECT schema FROM sinex_schemas.payload_schemas WHERE id=$1 AND event_type=$2 AND version='chromium-history-v1'").bind(id).bind(kind).fetch_optional(&mut *conn).await?;
            if schema.as_ref() != Some(&sinex_model::browser::payload_schema(download)) {
                return Err(Error::Schema(
                    "Chromium payload registration differs".into(),
                ));
            }
        }
    }
    if event_types != expected_types {
        return Err(Error::Schema("registered event types differ".into()));
    }
    if stage >= 2 {
        let singleton: i64 =
            sqlx::query_scalar("SELECT count(*) FROM derivation.shell_progress WHERE one")
                .fetch_one(&mut *conn)
                .await?;
        if singleton != 1 {
            return Err(Error::Schema("shell progress singleton is absent".into()));
        }
    }
    let counters: i64 = sqlx::query_scalar("SELECT count(*) FROM core.commit_counter WHERE one")
        .fetch_one(&mut *conn)
        .await?;
    if counters != 1 {
        return Err(Error::Schema("commit counter singleton is absent".into()));
    }
    let dangerous: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname IN ('sinex_reader','sinex_writer','sinex_read_model') AND (rolsuper OR rolcreaterole OR rolcreatedb OR rolbypassrls OR oid=(SELECT datdba FROM pg_database WHERE datname=current_database()))) OR EXISTS(SELECT 1 FROM pg_auth_members m JOIN pg_roles r ON r.oid=m.member WHERE r.rolname IN ('sinex_reader','sinex_writer','sinex_read_model')) OR NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='sinex_reader') OR NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='sinex_writer')").fetch_one(&mut *conn).await?;
    if dangerous {
        return Err(Error::Schema(
            "dedicated unprivileged reader/writer roles required".into(),
        ));
    }
    for table in &expected {
        let bad: bool = sqlx::query_scalar("SELECT NOT has_table_privilege('sinex_reader',$1,'SELECT') OR has_table_privilege('sinex_reader',$1,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') OR has_any_column_privilege('sinex_reader',$1,'INSERT,UPDATE,REFERENCES')").bind(&table).fetch_one(&mut *conn).await?;
        if bad {
            return Err(Error::Schema(format!("reader grants unsafe for {table}")));
        }
        let writer_insert = matches!(
            table.as_str(),
            "core.events"
                | "core.operations_log"
                | "core.operation_effects"
                | "core.deleted_occurrences"
                | "core.commit_log"
                | "raw.source_material_registry"
                | "raw.source_cursors"
                | "core.blobs"
                | "core.manifests"
                | "core.runs"
                | "reflection.events"
                | "core.live_coordinates"
                | "core.live_equivalence"
                | "audit.archived"
                | "raw.inputs"
                | "raw.observations"
                | "raw.record_failures"
                | "core.producer_versions"
                | "derivation.shell_dirty"
                | "derivation.session_dirty"
        );
        let writer_update = (stage >= 2 || table != "derivation.shell_dirty")
            && matches!(
                table.as_str(),
                "core.commit_counter"
                    | "core.operations_log"
                    | "raw.source_cursors"
                    | "core.blobs"
                    | "core.runs"
                    | "raw.observations"
                    | "core.live_equivalence"
                    | "core.live_coordinates"
                    | "derivation.shell_dirty"
                    | "derivation.shell_progress"
                    | "derivation.session_dirty"
                    | "derivation.session_progress"
            );
        let good: bool = sqlx::query_scalar("SELECT has_table_privilege('sinex_writer',$1,'SELECT') AND has_table_privilege('sinex_writer',$1,'INSERT')=$2 AND has_table_privilege('sinex_writer',$1,'UPDATE')=$3 AND has_any_column_privilege('sinex_writer',$1,'INSERT')=$2 AND has_any_column_privilege('sinex_writer',$1,'UPDATE')=($3 OR $1='raw.source_material_registry') AND NOT has_any_column_privilege('sinex_writer',$1,'REFERENCES') AND has_table_privilege('sinex_writer',$1,'DELETE')=($1 IN ('core.deleted_occurrences','core.live_equivalence','core.live_coordinates','derivation.shell_dirty','derivation.session_dirty')) AND NOT has_table_privilege('sinex_writer',$1,'TRUNCATE,REFERENCES,TRIGGER')").bind(&table).bind(writer_insert).bind(writer_update).fetch_one(&mut *conn).await?;
        if !good {
            return Err(Error::Schema(format!("writer grants differ for {table}")));
        }
    }
    let material_column_grants_bad:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_attribute a WHERE a.attrelid='raw.source_material_registry'::regclass AND a.attnum>0 AND NOT a.attisdropped AND has_column_privilege('sinex_writer',a.attrelid,a.attname,'UPDATE')<>(a.attname IN ('parsed_event_count','blob_id','status','total_bytes','durable_bytes')))").fetch_one(&mut *conn).await?;
    if material_column_grants_bad {
        return Err(Error::Schema(
            "writer material UPDATE columns differ".into(),
        ));
    }
    for table in &expected {
        let mutable = matches!(
            table.as_str(),
            "core.search_text" | "core.event_project" | "derivation.read_model_position"
        );
        let good:bool=sqlx::query_scalar("SELECT has_table_privilege('sinex_read_model',$1,'SELECT') AND has_table_privilege('sinex_read_model',$1,'INSERT')=$2 AND has_table_privilege('sinex_read_model',$1,'UPDATE')=$2 AND has_table_privilege('sinex_read_model',$1,'DELETE')=$2 AND has_any_column_privilege('sinex_read_model',$1,'INSERT')=$2 AND has_any_column_privilege('sinex_read_model',$1,'UPDATE')=$2 AND NOT has_any_column_privilege('sinex_read_model',$1,'REFERENCES') AND NOT has_table_privilege('sinex_read_model',$1,'TRUNCATE,REFERENCES,TRIGGER')").bind(table).bind(mutable).fetch_one(&mut *conn).await?;
        if !good {
            return Err(Error::Schema(format!(
                "read-model grants differ for {table}"
            )));
        }
    }
    let schema_grants_bad: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_namespace n JOIN pg_roles r ON r.oid=n.nspowner WHERE n.nspname IN ('core','raw','sinex_schemas','reflection','audit','derivation') AND (r.rolname IN ('sinex_reader','sinex_writer','sinex_read_model') OR has_schema_privilege('sinex_reader',n.oid,'CREATE') OR has_schema_privilege('sinex_writer',n.oid,'CREATE') OR has_schema_privilege('sinex_read_model',n.oid,'CREATE') OR NOT has_schema_privilege('sinex_reader',n.oid,'USAGE') OR NOT has_schema_privilege('sinex_writer',n.oid,'USAGE')))").fetch_one(&mut *conn).await?;
    if schema_grants_bad {
        return Err(Error::Schema("schema owner or grants differ".into()));
    }
    let internal_trigger_disabled: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('core','raw','sinex_schemas','reflection','audit','derivation') AND t.tgisinternal AND t.tgenabled <> 'O')").fetch_one(&mut *conn).await?;
    if internal_trigger_disabled {
        return Err(Error::Schema(
            "internal constraint trigger is disabled".into(),
        ));
    }
    let inbound: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_constraint WHERE contype='f' AND confrelid IN ('core.events'::regclass,'reflection.events'::regclass)").fetch_one(&mut *conn).await?;
    if inbound != 0 {
        return Err(Error::Schema(
            "foreign key points into an event hypertable".into(),
        ));
    }
    Ok(SchemaStatus {
        version: 1,
        fingerprint: stored,
        table_count: definitions.len(),
        pending_changes: schema_changes().into_iter().skip(stage).collect(),
        checked_changes: Vec::new(),
    })
}

fn atuin_payload_schema_at(stage: usize) -> serde_json::Value {
    let mut schema = crate::atuin::payload_schema();
    if stage < 6 {
        schema["properties"]["exit_code"]["minimum"] = serde_json::json!(0);
    }
    schema
}

fn predecessor_shell_payload() -> serde_json::Value {
    use serde_json::json;
    let mut schema = atuin_payload_schema_at(5);
    let properties = schema["properties"].as_object_mut().unwrap();
    properties.insert("canonical".into(), json!({"type":["string","null"]}));
    properties.insert(
        "observed_by".into(),
        json!({"type":"array","items":{"const":crate::ATUIN_SOURCE},"minItems":1,"maxItems":1}),
    );
    properties.insert(
        "association".into(),
        json!({"type":"object","required":["basis","rule","version","ambiguous","candidates"]}),
    );
    properties.insert(
        "field_evidence".into(),
        json!({"type":"object","required":["all"]}),
    );
    for field in ["canonical", "observed_by", "association", "field_evidence"] {
        schema["required"]
            .as_array_mut()
            .unwrap()
            .push(json!(field));
    }
    schema
}

// libpq does not expand a URI supplied through PGDATABASE. Match the already
// verified SQLx connection through decoded libpq fields, keeping secrets out of argv.
fn dump_environment(admin_url: &str) -> Result<Vec<(&'static str, String)>> {
    use sqlx::{
        ConnectOptions,
        postgres::{PgConnectOptions, PgSslMode},
    };
    let options: PgConnectOptions = admin_url.parse()?;
    // SQLx has no password getter; this public representation includes its resolved
    // password (including defaults/pgpass), not just the URI's explicit password.
    let resolved = options.to_url_lossy();
    if percent_encoding::percent_decode_str(resolved.username())
        .decode_utf8()
        .map_err(|_| Error::Invalid("invalid administrative user encoding"))?
        .as_ref()
        != options.get_username()
    {
        return Err(Error::Invalid(
            "administrative user cannot be represented for a verified dump",
        ));
    }
    let mut env = vec![
        (
            "PGHOST",
            options
                .get_socket()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_else(|| options.get_host().to_owned()),
        ),
        ("PGPORT", options.get_port().to_string()),
        (
            "PGDATABASE",
            options
                .get_database()
                .unwrap_or(options.get_username())
                .to_owned(),
        ),
        ("PGUSER", options.get_username().to_owned()),
        ("PGPASSFILE", "/dev/null".to_owned()),
        (
            "PGSSLMODE",
            match options.get_ssl_mode() {
                PgSslMode::Disable => "disable",
                PgSslMode::Allow => "allow",
                PgSslMode::Prefer => "prefer",
                PgSslMode::Require => "require",
                PgSslMode::VerifyCa => "verify-ca",
                PgSslMode::VerifyFull => "verify-full",
            }
            .to_owned(),
        ),
    ];
    if let Some(password) = resolved.password() {
        env.push((
            "PGPASSWORD",
            percent_encoding::percent_decode_str(password)
                .decode_utf8()
                .map_err(|_| Error::Invalid("invalid administrative password encoding"))?
                .into_owned(),
        ));
    }
    if let Some(value) = options.get_options() {
        env.push(("PGOPTIONS", value.to_owned()));
    }
    if let Some(value) = options.get_application_name() {
        env.push(("PGAPPNAME", value.to_owned()));
    }
    for (key, value) in resolved.query_pairs() {
        let target = match key.as_ref() {
            "sslrootcert" => "PGSSLROOTCERT",
            "sslcert" => "PGSSLCERT",
            "sslkey" => "PGSSLKEY",
            _ => continue,
        };
        let path = value.strip_prefix("file: ").ok_or(Error::Invalid(
            "inline TLS material is unsupported for the guarded dump; use certificate/key files",
        ))?;
        env.push((target, path.to_owned()));
    }
    Ok(env)
}

fn dump_command(environment: Vec<(&'static str, String)>) -> std::process::Command {
    let mut command = std::process::Command::new("pg_dump");
    // libpq service files, hostaddr and future PG defaults must not redirect the
    // backup away from the SQLx connection whose catalog we have verified.
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("PG") {
            command.env_remove(key);
        }
    }
    command
        .args(["--format=custom", "--no-password"])
        .envs(environment);
    command
}

async fn apply_change_entry(
    conn: &mut PgConnection,
    entry: &SchemaChange,
) -> Result<SchemaChangeCheck> {
    match entry.number {
        1 => {
            let exists: bool = sqlx::query_scalar("SELECT to_regclass('core.events') IS NOT NULL")
                .fetch_one(&mut *conn)
                .await?;
            if !exists {
                return Ok(SchemaChangeCheck {
                    number: entry.number,
                    outcome: "old_objects_absent",
                });
            }
            let ddl = declarations_at(1)?;
            let metadata = installed_tables(1)
                .iter()
                .position(|d| d.name == "applied_changes")
                .unwrap();
            conn.execute(ddl[metadata].as_str()).await?;
            for table in ["core.events", "reflection.events"] {
                conn.execute(
                    format!("ALTER TABLE {table} DROP CONSTRAINT registered_shell_payload")
                        .as_str(),
                )
                .await?;
                for name in ["registered_shell_payload", "registered_zsh_payload"] {
                    let body = EVENT_CONSTRAINTS
                        .iter()
                        .find(|(n, _)| *n == name)
                        .unwrap()
                        .1;
                    conn.execute(
                        format!("ALTER TABLE {table} ADD CONSTRAINT {name} {body}").as_str(),
                    )
                    .await?;
                }
            }
            sqlx::raw_sql("ALTER TABLE raw.inputs DROP CONSTRAINT input_kind; ALTER TABLE raw.inputs ADD CONSTRAINT input_kind CHECK(kind IN ('sqlite','file')); GRANT SELECT ON sinex_schemas.applied_changes TO sinex_reader,sinex_writer,sinex_read_model; INSERT INTO core.event_types(id,name) VALUES(4,'command.imported')").execute(&mut *conn).await?;
            sqlx::query("INSERT INTO sinex_schemas.payload_schemas(id,event_type,version,schema) VALUES(5,'command.canonical','shell-command-v1-atuin-zsh',$1)").bind(crate::shell::payload_schema()).execute(&mut *conn).await?;
            sqlx::query("INSERT INTO sinex_schemas.payload_schemas(id,event_type,version,schema) VALUES(4,'command.imported','zsh-extended-v1',$1)").bind(crate::zsh::payload_schema()).execute(&mut *conn).await?;
        }
        2 => {
            let exists: bool =
                sqlx::query_scalar("SELECT to_regclass('derivation.shell_dirty') IS NOT NULL")
                    .fetch_one(&mut *conn)
                    .await?;
            if !exists {
                return Ok(SchemaChangeCheck {
                    number: entry.number,
                    outcome: "old_objects_absent",
                });
            }
            sqlx::raw_sql("ALTER TABLE derivation.shell_dirty ADD COLUMN dirty_seq bigint; UPDATE derivation.shell_dirty SET dirty_seq=(SELECT seq FROM core.commit_counter WHERE one); ALTER TABLE derivation.shell_dirty ALTER COLUMN dirty_seq SET NOT NULL").execute(&mut *conn).await?;
            let ddl = declarations_at(2)?;
            let progress = installed_tables(2)
                .iter()
                .position(|d| d.name == "shell_progress")
                .unwrap();
            conn.execute(ddl[progress].as_str()).await?;
            sqlx::raw_sql("INSERT INTO derivation.shell_progress(one) VALUES(true); GRANT SELECT ON derivation.shell_progress TO sinex_reader,sinex_writer,sinex_read_model; GRANT UPDATE ON derivation.shell_dirty,derivation.shell_progress TO sinex_writer; INSERT INTO derivation.shell_dirty(parent_id,dirty_seq) SELECT e.id,c.seq FROM core.live_events e CROSS JOIN core.commit_counter c WHERE c.one AND e.source='terminal.atuin-history' AND e.event_type='command.executed' AND e.record_state='present' ON CONFLICT(parent_id) DO UPDATE SET dirty_seq=greatest(shell_dirty.dirty_seq,EXCLUDED.dirty_seq)").execute(&mut *conn).await?;
        }
        3 => {
            let exists: bool = sqlx::query_scalar("SELECT to_regclass('core.events') IS NOT NULL")
                .fetch_one(&mut *conn)
                .await?;
            if !exists {
                return Ok(SchemaChangeCheck {
                    number: entry.number,
                    outcome: "old_objects_absent",
                });
            }
            let ddl = declarations_at(3)?;
            for (i, d) in installed_tables(3).iter().enumerate() {
                if d.name.starts_with("session_") {
                    conn.execute(ddl[i].as_str()).await?;
                }
            }
            sqlx::raw_sql("INSERT INTO derivation.session_progress(one) VALUES(true); INSERT INTO core.event_types(id,name) VALUES(5,'activity.session.boundary'); GRANT SELECT ON derivation.session_dirty,derivation.session_progress TO sinex_reader,sinex_writer,sinex_read_model; GRANT INSERT,UPDATE,DELETE ON derivation.session_dirty TO sinex_writer; GRANT UPDATE ON derivation.session_progress TO sinex_writer").execute(&mut *conn).await?;
            sqlx::query("INSERT INTO sinex_schemas.payload_schemas(id,event_type,version,schema) VALUES(6,'activity.session.boundary','shell-session-v1',$1)").bind(crate::session::payload_schema()).execute(&mut *conn).await?;
            sqlx::query(crate::session::DIRTY_SPANS)
                .bind(Option::<uuid::Uuid>::None)
                .execute(&mut *conn)
                .await?;
        }
        4 => {
            let exists: bool = sqlx::query_scalar("SELECT to_regclass('core.events') IS NOT NULL")
                .fetch_one(&mut *conn)
                .await?;
            if !exists {
                return Ok(SchemaChangeCheck {
                    number: entry.number,
                    outcome: "old_objects_absent",
                });
            }
            for table in ["core.events", "reflection.events"] {
                let body = EVENT_CONSTRAINTS
                    .iter()
                    .find(|(n, _)| *n == "registered_browser_payload")
                    .expect("declared browser constraint")
                    .1;
                conn.execute(
                    format!("ALTER TABLE {table} ADD CONSTRAINT registered_browser_payload {body}")
                        .as_str(),
                )
                .await?;
            }
            sqlx::raw_sql("INSERT INTO core.event_types(id,name) VALUES(6,'page.visited'),(7,'download.observed')").execute(&mut *conn).await?;
            for (id, kind, download) in [(7, "page.visited", false), (8, "download.observed", true)]
            {
                sqlx::query("INSERT INTO sinex_schemas.payload_schemas(id,event_type,version,schema) VALUES($1,$2,'chromium-history-v1',$3)").bind(id).bind(kind).bind(sinex_model::browser::payload_schema(download)).execute(&mut *conn).await?;
            }
        }
        5 => {
            let exists: bool =
                sqlx::query_scalar("SELECT to_regclass('core.search_text') IS NOT NULL")
                    .fetch_one(&mut *conn)
                    .await?;
            if !exists {
                return Ok(SchemaChangeCheck {
                    number: entry.number,
                    outcome: "old_objects_absent",
                });
            }
            conn.execute("ALTER TABLE core.search_text ALTER COLUMN tsv DROP NOT NULL")
                .await?;
            conn.execute(crate::event_storage::FULL_TEXT_UNAVAILABLE_INDEX)
                .await?;
        }
        6 => {
            let exists: bool = sqlx::query_scalar(
                "SELECT to_regclass('sinex_schemas.payload_schemas') IS NOT NULL",
            )
            .fetch_one(&mut *conn)
            .await?;
            if !exists {
                return Ok(SchemaChangeCheck {
                    number: entry.number,
                    outcome: "old_objects_absent",
                });
            }
            let changed = sqlx::query("UPDATE sinex_schemas.payload_schemas SET schema=$1 WHERE id=2 AND event_type='command.executed' AND version='atuin-v1'")
                .bind(atuin_payload_schema_at(6))
                .execute(&mut *conn)
                .await?;
            if changed.rows_affected() != 1 {
                return Err(Error::Schema(
                    "registered Atuin payload schema missing".into(),
                ));
            }
        }
        _ => return Err(Error::Schema("unknown schema change".into())),
    }
    Ok(SchemaChangeCheck {
        number: entry.number,
        outcome: "applied",
    })
}

/// Explicit operator upgrade. No DDL or data writes happen before a complete dump.
/// Each committed entry is a guarded catalog state that the next invocation checks.
pub async fn apply_changes(admin_url: &str, dump_file: &std::path::Path) -> Result<SchemaStatus> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut conn = PgConnection::connect(admin_url).await?;
    require_pg18(&mut conn).await?;
    let locked: bool = sqlx::query_scalar(
        "SELECT pg_try_advisory_lock(73018924011) AND pg_try_advisory_lock(73018924012)",
    )
    .fetch_one(&mut conn)
    .await?;
    if !locked {
        return Err(Error::Schema("schema or writer is already owned".into()));
    }
    let status = inspect(&mut conn).await?;
    if status.pending_changes.is_empty() {
        return Ok(status);
    }
    if !dump_file.is_absolute() {
        return Err(Error::Invalid("absolute dump path required"));
    }
    let dump = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(dump_file)?;
    let mut diagnostic_path = dump_file.as_os_str().to_os_string();
    diagnostic_path.push(".stderr");
    let diagnostic = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(std::path::Path::new(&diagnostic_path))?;
    let parent = dump_file
        .parent()
        .expect("absolute dump path has parent")
        .to_owned();
    let dump_environment = dump_environment(admin_url)?;
    let (complete, exit_code) =
        tokio::task::spawn_blocking(move || -> std::io::Result<(bool, Option<i32>)> {
            let output = dump.try_clone()?;
            let result = dump_command(dump_environment)
                .stdout(std::process::Stdio::from(output))
                .stderr(std::process::Stdio::from(diagnostic.try_clone()?))
                .status()?;
            dump.sync_all()?;
            diagnostic.sync_all()?;
            std::fs::File::open(parent)?.sync_all()?;
            Ok((
                result.success() && dump.metadata()?.len() > 0,
                result.code(),
            ))
        })
        .await
        .map_err(|_| Error::Schema("dump worker failed; no schema change applied".into()))??;
    if !complete {
        return Err(Error::Schema(format!(
            "pg_dump failed (exit {exit_code:?}); no schema change applied (partial dump and raw diagnostics retained)"
        )));
    }
    let mut checked_changes = Vec::new();
    for entry in status.pending_changes {
        // inspect validates the exact live state and prefix before every entry.
        let current = inspect(&mut conn).await?;
        if current.pending_changes.first().map(|c| c.number) != Some(entry.number) {
            return Err(Error::Schema("schema changes are out of order".into()));
        }
        let mut tx = conn.begin().await?;
        let checked = apply_change_entry(&mut tx, &entry).await?;
        sqlx::query("INSERT INTO sinex_schemas.applied_changes(number,description) VALUES($1,$2)")
            .bind(entry.number)
            .bind(entry.description)
            .execute(&mut *tx)
            .await?;
        let catalog = serde_json::to_value(capture_catalog(&mut tx).await?)
            .map_err(|_| Error::Schema("cannot encode upgraded catalog".into()))?;
        sqlx::query(
            "UPDATE sinex_schemas.definition SET fingerprint=$1,catalog=$2 WHERE version=1",
        )
        .bind(fingerprint_at(entry.number as usize)?)
        .bind(catalog)
        .execute(&mut *tx)
        .await?;
        inspect(&mut tx).await?;
        tx.commit().await?;
        checked_changes.push(checked);
    }
    let mut status = inspect(&mut conn).await?;
    status.checked_changes = checked_changes;
    Ok(status)
}

#[cfg(test)]
mod upgrade_tests {
    #[test]
    fn dump_uses_decoded_connection_fields_without_a_secret_argument() {
        let fields:std::collections::BTreeMap<_,_>=super::dump_environment("postgresql://synthetic:p%40ss%2Bword@localhost/synthetic%20db?host=%2Fsynthetic%2Fsocket&port=55431&ssl-mode=verify-full&ssl-ca=%2Fsynthetic%2Fca").unwrap().into_iter().collect();
        assert_eq!(fields["PGDATABASE"], "synthetic db");
        assert_eq!(fields["PGHOST"], "/synthetic/socket");
        assert_eq!(fields["PGPORT"], "55431");
        assert_eq!(fields["PGPASSWORD"], "p@ss+word");
        assert_eq!(fields["PGSSLMODE"], "verify-full");
        assert_eq!(fields["PGSSLROOTCERT"], "/synthetic/ca");
    }
    #[test]
    fn signed_exit_registration_preserves_stage_five_binding() {
        assert_eq!(
            super::fingerprint_at(5).unwrap(),
            "6ccf124f6bc31bb4c86522d1585e98a173b3b35c2db2205ff786dbf2909e17d7"
        );
        assert_eq!(
            super::fingerprint_at(6).unwrap(),
            "2f7b2a8789b6b7aadb0f2d6b06b1272d614d37e23cb480ce1455c1beeadee773"
        );
        assert_eq!(
            super::atuin_payload_schema_at(5)["properties"]["exit_code"]["minimum"],
            0
        );
        assert!(
            super::atuin_payload_schema_at(6)["properties"]["exit_code"]
                .get("minimum")
                .is_none()
        );
        assert_eq!(
            super::predecessor_shell_payload()["properties"]["exit_code"]["minimum"],
            0
        );
    }
    #[test]
    fn published_stage_four_declarations_keep_their_binding() {
        assert_eq!(
            super::fingerprint_at(4).unwrap(),
            "5e611ef17edcc8ced01df352bf993ad3c79f2fb0c3560f2a0bfe5a1a6e277c45"
        );
    }
    #[test]
    fn published_predecessor_declarations_retain_the_released_binding() {
        // Observed from the independently built and published 4b5bf0619 package.
        assert_eq!(
            super::fingerprint_at(0).unwrap(),
            "2e82b6514b2a7104dbed73a52dfac34e89fbec9e99e9adb0af10e05604c41a9b"
        );
    }
}
