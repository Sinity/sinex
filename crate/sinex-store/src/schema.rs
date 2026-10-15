//! The deliberately small foundation schema. Future declarations extend this owner.
use crate::{Error, Result};
use sea_query::{Alias, ColumnDef, Expr, PostgresQueryBuilder, Table};
use sqlx::{Connection, Executor, PgConnection, Row};
use std::collections::BTreeMap;

pub const WRITER_ROLE: &str = "sinex_writer";
pub const READER_ROLE: &str = "sinex_reader";

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
        "event_material",
        "FOREIGN KEY (source_material_id) REFERENCES raw.source_material_registry(id)",
    ),
    ("event_run", "FOREIGN KEY (run_id) REFERENCES core.runs(id)"),
];

const TABLES: &[Definition] = &[
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
            col!("blob_id", "uuid"),
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
                "foundation_closed_material",
                "CHECK (status = 'completed'::text AND material_kind = 'file'::text AND ts_quality = 'unknown'::text)",
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

fn declarations() -> Result<Vec<String>> {
    TABLES
        .iter()
        .map(|d| {
            let mut table = Table::create();
            table.table((Alias::new(d.schema), Alias::new(d.name)));
            for c in d.columns {
                let mut column = ColumnDef::new(Alias::new(c.name));
                column.custom(Alias::new(c.ty));
                if !c.nullable {
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
                sql.push_str(", CONSTRAINT \"");
                sql.push_str(name);
                sql.push_str("\" ");
                let generated = match *name {
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
    Ok(blake3::hash(
        [
            declarations()?,
            crate::event_storage::declarations(),
            vec![TIMESCALEDB_VERSION.to_owned()],
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
        "SELECT count(*) FROM pg_namespace WHERE nspname IN ('core','raw','sinex_schemas','reflection','audit')",
    )
    .fetch_one(&mut *tx)
    .await?;
    if count == 0 {
        let extension_sql = [
            "CREATE EXTENSION IF NOT EXISTS timescaledb VERSION '",
            TIMESCALEDB_VERSION,
            "'",
        ]
        .concat();
        tx.execute(extension_sql.as_str()).await?;
        let installed: String =
            sqlx::query_scalar("SELECT extversion FROM pg_extension WHERE extname='timescaledb'")
                .fetch_one(&mut *tx)
                .await?;
        if installed != TIMESCALEDB_VERSION {
            return Err(Error::Schema("TimescaleDB 2.27.2 is required".into()));
        }
        sqlx::raw_sql("CREATE SCHEMA core; CREATE SCHEMA raw; CREATE SCHEMA sinex_schemas; CREATE SCHEMA reflection; CREATE SCHEMA audit; REVOKE ALL ON SCHEMA core, raw, sinex_schemas,reflection,audit FROM PUBLIC").execute(&mut *tx).await?;
        for ddl in declarations()? {
            tx.execute(ddl.as_str()).await?;
        }
        configure_events(&mut tx).await?;
        sqlx::query("INSERT INTO core.event_types (id,name) VALUES (1,'synthetic.observed')")
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO core.commit_counter (one,seq) VALUES (true,0)")
            .execute(&mut *tx)
            .await?;
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
        sqlx::raw_sql("GRANT USAGE ON SCHEMA core,raw,sinex_schemas TO sinex_writer,sinex_reader; GRANT USAGE ON SCHEMA reflection,audit TO sinex_writer,sinex_reader; GRANT SELECT ON ALL TABLES IN SCHEMA core,raw,sinex_schemas,reflection,audit TO sinex_writer,sinex_reader; GRANT INSERT ON core.events,reflection.events,core.live_coordinates,core.live_equivalence,audit.archived,core.commit_log,raw.source_material_registry,core.manifests TO sinex_writer; GRANT INSERT,UPDATE ON raw.source_cursors,core.blobs,core.runs TO sinex_writer; GRANT UPDATE ON core.commit_counter TO sinex_writer; GRANT UPDATE(parsed_event_count) ON raw.source_material_registry TO sinex_writer").execute(&mut *tx).await?;
    }
    let status = inspect(&mut tx).await?;
    tx.commit().await?;
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
    let rows = sqlx::query("SELECT n.nspname || '.' || c.relname || '.' || con.conname AS key,pg_get_constraintdef(con.oid) AS definition FROM pg_constraint con JOIN pg_class c ON c.oid=con.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('core','raw','sinex_schemas','reflection','audit') AND con.contype <> 'n'").fetch_all(&mut *conn).await?;
    let mut constraints = BTreeMap::new();
    for r in rows {
        constraints.insert(r.try_get("key")?, r.try_get("definition")?);
    }
    let rows = sqlx::query("SELECT n.nspname || '.' || c.relname || '.' || a.attname AS key,pg_get_expr(ad.adbin,ad.adrelid) AS expression FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_attrdef ad ON ad.adrelid=c.oid AND ad.adnum=a.attnum WHERE n.nspname IN ('core','raw','sinex_schemas','reflection','audit') AND a.attnum>0 AND NOT a.attisdropped").fetch_all(&mut *conn).await?;
    let mut expressions = BTreeMap::new();
    for r in rows {
        expressions.insert(r.try_get("key")?, r.try_get("expression")?);
    }
    let mut objects = BTreeMap::new();
    for (kind, sql) in [
        (
            "collation",
            "SELECT n.nspname || '.' || c.relname || '.' || a.attname AS key,jsonb_build_object('schema',cn.nspname,'name',co.collname) AS value FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_collation co ON co.oid=a.attcollation LEFT JOIN pg_namespace cn ON cn.oid=co.collnamespace WHERE n.nspname IN ('core','raw','sinex_schemas','reflection','audit') AND c.relkind IN ('r','v') AND a.attnum>0 AND NOT a.attisdropped",
        ),
        (
            "index",
            "SELECT n.nspname || '.' || c.relname AS key,jsonb_build_object('definition',pg_get_indexdef(c.oid),'valid',i.indisvalid,'ready',i.indisready,'live',i.indislive,'options',c.reloptions,'tablespace',c.reltablespace) AS value FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('core','raw','sinex_schemas','reflection','audit')",
        ),
        (
            "function",
            "SELECT p.oid::regprocedure::text AS key,jsonb_build_object('definition',pg_get_functiondef(p.oid),'owner',pg_get_userbyid(p.proowner),'acl',p.proacl) AS value FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname IN ('core','raw','sinex_schemas','reflection','audit')",
        ),
        (
            "trigger",
            "SELECT n.nspname || '.' || c.relname || '.' || t.tgname AS key,jsonb_build_object('definition',pg_get_triggerdef(t.oid),'enabled',t.tgenabled::text) AS value FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('core','raw','sinex_schemas','reflection','audit') AND NOT t.tgisinternal",
        ),
        (
            "view",
            "SELECT n.nspname || '.' || c.relname AS key,jsonb_build_object('definition',pg_get_viewdef(c.oid),'options',c.reloptions) AS value FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('core','raw','sinex_schemas','reflection','audit') AND c.relkind='v'",
        ),
        (
            "policy",
            "SELECT n.nspname || '.' || c.relname || '.' || p.polname AS key,jsonb_build_object('roles',p.polroles,'command',p.polcmd::text,'permissive',p.polpermissive,'using',pg_get_expr(p.polqual,p.polrelid),'check',pg_get_expr(p.polwithcheck,p.polrelid)) AS value FROM pg_policy p JOIN pg_class c ON c.oid=p.polrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('core','raw','sinex_schemas','reflection','audit')",
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
    let tables: Vec<String> = sqlx::query_scalar("SELECT n.nspname || '.' || c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('core','raw','sinex_schemas','reflection','audit') AND c.relkind IN ('r','p','v','m','f','S') ORDER BY 1").fetch_all(&mut *conn).await?;
    let mut expected: Vec<String> = TABLES
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
    if stored != fingerprint()? {
        return Err(Error::Schema("schema version fingerprint differs".into()));
    }
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
    for d in TABLES {
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
                || actual.try_get::<bool, _>("attnotnull")? == c.nullable
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
        if constraints.len() != d.constraints.len() {
            return Err(Error::Schema(format!(
                "{}.{} constraints differ",
                d.schema, d.name
            )));
        }
        for (name, _body) in d.constraints {
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
        let index_or_owner_drift: bool = sqlx::query_scalar("SELECT c.relrowsecurity OR c.relforcerowsecurity OR owner.rolname IN ('sinex_reader','sinex_writer') OR EXISTS(SELECT 1 FROM pg_index i WHERE i.indrelid=c.oid AND (NOT i.indisvalid OR NOT i.indisready OR NOT i.indislive)) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_roles owner ON owner.oid=c.relowner WHERE n.nspname=$1 AND c.relname=$2").bind(d.schema).bind(d.name).fetch_one(&mut *conn).await?;
        if index_or_owner_drift {
            return Err(Error::Schema(format!(
                "{}.{} index, owner or row-security declaration differs",
                d.schema, d.name
            )));
        }
    }
    let metadata_mutation_grant: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN LATERAL aclexplode(c.relacl) acl WHERE n.nspname='sinex_schemas' AND c.relname='definition' AND acl.grantee<>c.relowner AND acl.privilege_type<>'SELECT') OR EXISTS(SELECT 1 FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN LATERAL aclexplode(a.attacl) acl WHERE n.nspname='sinex_schemas' AND c.relname='definition' AND acl.grantee<>c.relowner AND acl.privilege_type<>'SELECT')").fetch_one(&mut *conn).await?;
    if metadata_mutation_grant {
        return Err(Error::Schema(
            "canonical catalog receipt has a non-owner mutation grant".into(),
        ));
    }
    let counters: i64 = sqlx::query_scalar("SELECT count(*) FROM core.commit_counter WHERE one")
        .fetch_one(&mut *conn)
        .await?;
    if counters != 1 {
        return Err(Error::Schema("commit counter singleton is absent".into()));
    }
    let dangerous: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname IN ('sinex_reader','sinex_writer') AND (rolsuper OR rolcreaterole OR rolcreatedb OR rolbypassrls OR oid=(SELECT datdba FROM pg_database WHERE datname=current_database()))) OR EXISTS(SELECT 1 FROM pg_auth_members m JOIN pg_roles r ON r.oid=m.member WHERE r.rolname IN ('sinex_reader','sinex_writer')) OR NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='sinex_reader') OR NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='sinex_writer')").fetch_one(&mut *conn).await?;
    if dangerous {
        return Err(Error::Schema(
            "dedicated unprivileged reader/writer roles required".into(),
        ));
    }
    for table in expected {
        let bad: bool = sqlx::query_scalar("SELECT NOT has_table_privilege('sinex_reader',$1,'SELECT') OR has_table_privilege('sinex_reader',$1,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') OR has_any_column_privilege('sinex_reader',$1,'INSERT,UPDATE,REFERENCES')").bind(&table).fetch_one(&mut *conn).await?;
        if bad {
            return Err(Error::Schema(format!("reader grants unsafe for {table}")));
        }
        let writer_insert = matches!(
            table.as_str(),
            "core.events"
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
        );
        let writer_update = matches!(
            table.as_str(),
            "core.commit_counter" | "raw.source_cursors" | "core.blobs" | "core.runs"
        );
        let good: bool = sqlx::query_scalar("SELECT has_table_privilege('sinex_writer',$1,'SELECT') AND has_table_privilege('sinex_writer',$1,'INSERT')=$2 AND has_table_privilege('sinex_writer',$1,'UPDATE')=$3 AND has_any_column_privilege('sinex_writer',$1,'INSERT')=$2 AND has_any_column_privilege('sinex_writer',$1,'UPDATE')=($3 OR $1='raw.source_material_registry') AND NOT has_any_column_privilege('sinex_writer',$1,'REFERENCES') AND NOT has_table_privilege('sinex_writer',$1,'DELETE,TRUNCATE,REFERENCES,TRIGGER')").bind(&table).bind(writer_insert).bind(writer_update).fetch_one(&mut *conn).await?;
        if !good {
            return Err(Error::Schema(format!("writer grants differ for {table}")));
        }
    }
    let material_column_grants_bad:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_attribute a WHERE a.attrelid='raw.source_material_registry'::regclass AND a.attnum>0 AND NOT a.attisdropped AND has_column_privilege('sinex_writer',a.attrelid,a.attname,'UPDATE')<>(a.attname='parsed_event_count'))").fetch_one(&mut *conn).await?;
    if material_column_grants_bad {
        return Err(Error::Schema(
            "writer material UPDATE grant must cover only parsed_event_count".into(),
        ));
    }
    let schema_grants_bad: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_namespace n JOIN pg_roles r ON r.oid=n.nspowner WHERE n.nspname IN ('core','raw','sinex_schemas','reflection','audit') AND (r.rolname IN ('sinex_reader','sinex_writer') OR has_schema_privilege('sinex_reader',n.oid,'CREATE') OR has_schema_privilege('sinex_writer',n.oid,'CREATE') OR NOT has_schema_privilege('sinex_reader',n.oid,'USAGE') OR NOT has_schema_privilege('sinex_writer',n.oid,'USAGE')))").fetch_one(&mut *conn).await?;
    if schema_grants_bad {
        return Err(Error::Schema("schema owner or grants differ".into()));
    }
    let internal_trigger_disabled: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('core','raw','sinex_schemas','reflection','audit') AND t.tgisinternal AND t.tgenabled <> 'O')").fetch_one(&mut *conn).await?;
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
        table_count: TABLES.len(),
    })
}
