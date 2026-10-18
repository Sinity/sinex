//! Table declarations for the current schema. Changing any of them changes the
//! schema fingerprint; past versions are derived in `declarations.rs`.
pub(super) struct Column {
    pub(super) name: &'static str,
    pub(super) ty: &'static str,
    pub(super) nullable: bool,
    pub(super) default: Option<&'static str>,
    pub(super) generated: Option<&'static str>,
}
pub(super) struct Definition {
    pub(super) schema: &'static str,
    pub(super) name: &'static str,
    pub(super) columns: &'static [Column],
    pub(super) constraints: &'static [(&'static str, &'static str)],
}

pub(super) const EVENT_COLUMNS: &[Column] = &[
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
pub(super) const EVENT_CONSTRAINTS: &[(&str, &str)] = &[
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

pub(super) const TABLES: &[Definition] = &[
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
