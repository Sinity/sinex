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
        ],
        constraints: &[
            ("source_material_registry_pkey", "PRIMARY KEY (id)"),
            ("material_v7", "CHECK (uuid_extract_version(id) = 7)"),
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
        columns: &[
            col!("id", "uuid"),
            col!("source", "text"),
            col!("event_type", "text"),
            col!("payload", "jsonb"),
            col!("ts_orig", "timestamp with time zone", null),
            col!("ts_quality", "text"),
            col!("ts_coided", "timestamp with time zone", generated "uuid_extract_timestamp(id)"),
            col!("ts_persisted", "timestamp with time zone", default "now()"),
            col!("commit_seq", "bigint"),
            col!("source_material_id", "uuid"),
            col!("anchor_byte", "bigint"),
            col!("ordinal", "smallint"),
            col!("record_hash", "bytea"),
            col!("equivalence_key", "text", null),
            col!("run_id", "uuid"),
        ],
        constraints: &[
            ("events_pkey", "PRIMARY KEY (id)"),
            ("event_v7", "CHECK (uuid_extract_version(id) = 7)"),
            (
                "foundation_unknown_time",
                "CHECK (ts_orig IS NULL AND ts_quality = 'unknown'::text)",
            ),
            ("event_anchor", "CHECK (anchor_byte >= 0 AND ordinal >= 0)"),
            (
                "payload_object",
                "CHECK (jsonb_typeof(payload) = 'object'::text)",
            ),
            (
                "event_material",
                "FOREIGN KEY (source_material_id) REFERENCES raw.source_material_registry(id)",
            ),
            ("event_run", "FOREIGN KEY (run_id) REFERENCES core.runs(id)"),
            (
                "event_occurrence",
                "UNIQUE (source_material_id, anchor_byte, event_type, ordinal)",
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
            ("foundation_admit", "CHECK (change = 0)"),
            ("foundation_core_store", "CHECK (store = 0)"),
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

fn declarations() -> Vec<String> {
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
                sql.push_str(body);
            }
            sql.push(')');
            sql
        })
        .collect()
}

fn fingerprint() -> String {
    blake3::hash(declarations().join("\n").as_bytes())
        .to_hex()
        .to_string()
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
        "SELECT count(*) FROM pg_namespace WHERE nspname IN ('core','raw','sinex_schemas')",
    )
    .fetch_one(&mut *tx)
    .await?;
    if count == 0 {
        sqlx::raw_sql("CREATE SCHEMA core; CREATE SCHEMA raw; CREATE SCHEMA sinex_schemas; REVOKE ALL ON SCHEMA core, raw, sinex_schemas FROM PUBLIC").execute(&mut *tx).await?;
        for ddl in declarations() {
            tx.execute(ddl.as_str()).await?;
        }
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
        .bind(fingerprint())
        .bind(catalog)
        .execute(&mut *tx)
        .await?;
        sqlx::raw_sql("GRANT USAGE ON SCHEMA core,raw,sinex_schemas TO sinex_writer,sinex_reader; GRANT SELECT ON ALL TABLES IN SCHEMA core,raw,sinex_schemas TO sinex_writer,sinex_reader; GRANT INSERT ON core.events,core.commit_log,raw.source_material_registry,core.manifests TO sinex_writer; GRANT INSERT,UPDATE ON raw.source_cursors,core.blobs,core.runs TO sinex_writer; GRANT UPDATE ON core.commit_counter TO sinex_writer").execute(&mut *tx).await?;
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
}

async fn capture_catalog(conn: &mut PgConnection) -> Result<Catalog> {
    let rows = sqlx::query("SELECT n.nspname || '.' || c.relname || '.' || con.conname AS key,pg_get_constraintdef(con.oid) AS definition FROM pg_constraint con JOIN pg_class c ON c.oid=con.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('core','raw','sinex_schemas') AND con.contype <> 'n'").fetch_all(&mut *conn).await?;
    let mut constraints = BTreeMap::new();
    for r in rows {
        constraints.insert(r.try_get("key")?, r.try_get("definition")?);
    }
    let rows = sqlx::query("SELECT n.nspname || '.' || c.relname || '.' || a.attname AS key,pg_get_expr(ad.adbin,ad.adrelid) AS expression FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_attrdef ad ON ad.adrelid=c.oid AND ad.adnum=a.attnum WHERE n.nspname IN ('core','raw','sinex_schemas') AND a.attnum>0 AND NOT a.attisdropped").fetch_all(&mut *conn).await?;
    let mut expressions = BTreeMap::new();
    for r in rows {
        expressions.insert(r.try_get("key")?, r.try_get("expression")?);
    }
    Ok(Catalog {
        constraints,
        expressions,
    })
}

pub(crate) async fn inspect(conn: &mut PgConnection) -> Result<SchemaStatus> {
    let tables: Vec<String> = sqlx::query_scalar("SELECT n.nspname || '.' || c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('core','raw','sinex_schemas') AND c.relkind IN ('r','p','v','m','f','S') ORDER BY 1").fetch_all(&mut *conn).await?;
    let mut expected: Vec<String> = TABLES
        .iter()
        .map(|t| [t.schema, ".", t.name].concat())
        .collect();
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
    if stored != fingerprint() {
        return Err(Error::Schema("schema version fingerprint differs".into()));
    }
    let catalog: Catalog = serde_json::from_value(receipt.try_get("catalog")?)
        .map_err(|_| Error::Schema("canonical catalog receipt is invalid".into()))?;
    let actual_catalog = capture_catalog(conn).await?;
    if catalog.constraints != actual_catalog.constraints
        || catalog.expressions != actual_catalog.expressions
    {
        return Err(Error::Schema(
            "constraint or column expression differs from canonical bootstrap declaration".into(),
        ));
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
        let index_or_owner_drift: bool = sqlx::query_scalar("SELECT c.relrowsecurity OR c.relforcerowsecurity OR owner.rolname IN ('sinex_reader','sinex_writer') OR EXISTS(SELECT 1 FROM pg_index i WHERE i.indrelid=c.oid AND (NOT i.indisvalid OR NOT i.indisready OR NOT i.indislive OR NOT EXISTS(SELECT 1 FROM pg_constraint p WHERE p.conindid=i.indexrelid))) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_roles owner ON owner.oid=c.relowner WHERE n.nspname=$1 AND c.relname=$2").bind(d.schema).bind(d.name).fetch_one(&mut *conn).await?;
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
        );
        let writer_update = matches!(
            table.as_str(),
            "core.commit_counter" | "raw.source_cursors" | "core.blobs" | "core.runs"
        );
        let good: bool = sqlx::query_scalar("SELECT has_table_privilege('sinex_writer',$1,'SELECT') AND has_table_privilege('sinex_writer',$1,'INSERT')=$2 AND has_table_privilege('sinex_writer',$1,'UPDATE')=$3 AND has_any_column_privilege('sinex_writer',$1,'INSERT')=$2 AND has_any_column_privilege('sinex_writer',$1,'UPDATE')=$3 AND NOT has_any_column_privilege('sinex_writer',$1,'REFERENCES') AND NOT has_table_privilege('sinex_writer',$1,'DELETE,TRUNCATE,REFERENCES,TRIGGER')").bind(&table).bind(writer_insert).bind(writer_update).fetch_one(&mut *conn).await?;
        if !good {
            return Err(Error::Schema(format!("writer grants differ for {table}")));
        }
    }
    let schema_grants_bad: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_namespace n JOIN pg_roles r ON r.oid=n.nspowner WHERE n.nspname IN ('core','raw','sinex_schemas') AND (r.rolname IN ('sinex_reader','sinex_writer') OR has_schema_privilege('sinex_reader',n.oid,'CREATE') OR has_schema_privilege('sinex_writer',n.oid,'CREATE') OR NOT has_schema_privilege('sinex_reader',n.oid,'USAGE') OR NOT has_schema_privilege('sinex_writer',n.oid,'USAGE')))").fetch_one(&mut *conn).await?;
    if schema_grants_bad {
        return Err(Error::Schema("schema owner or grants differ".into()));
    }
    let extra: i64 = sqlx::query_scalar("SELECT (SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('core','raw','sinex_schemas') AND NOT t.tgisinternal) + (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname IN ('core','raw','sinex_schemas')) + (SELECT count(*) FROM pg_policy p JOIN pg_class c ON c.oid=p.polrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('core','raw','sinex_schemas'))").fetch_one(&mut *conn).await?;
    if extra != 0 {
        return Err(Error::Schema(
            "undeclared triggers/functions/policies present".into(),
        ));
    }
    Ok(SchemaStatus {
        version: 1,
        fingerprint: stored,
        table_count: TABLES.len(),
    })
}
