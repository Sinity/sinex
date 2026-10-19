//! The schema, declared once in Rust (architecture §3.7). The never-deployed
//! history is squashed into version 0: [`declarations`] creates the current
//! schema in an empty database. Later changes a declaration cannot express
//! (a rename, a type change) go in [`CHANGES`], in order.
//!
//! Enum CHECK lists come from the Rust enums (`DbCheck`), and the event-type
//! registry rows and typed views come from `sinex_model::kinds::ALL`, so the
//! database cannot disagree with the code.
//!
//! This crate uses no compile-time-checked queries: it builds before any
//! database exists and provisions the one the store's queries are checked
//! against.
use sinex_model::{Class, DbCheck, KeyKind, RecordState, ReplaceRule, TimeQuality, kinds};
use sqlx::{Connection, Executor, PgConnection};

mod tables;

/// The schema version a fresh database is created at.
pub const VERSION: i32 = (CHANGES.len()) as i32;

/// A change applied to an existing database to bring it to the next version.
pub struct Change {
    pub version: i32,
    pub statements: &'static [&'static str],
}

/// Ordered changes after version 0. Empty until the first deployment.
pub const CHANGES: &[Change] = &[];

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(
        "database is at schema version {found}, this build declares {declared}; upgrading an existing database is not built yet (architecture §3.7)"
    )]
    Upgrade { found: i32, declared: i32 },
    #[error("database has Sinex objects but no schema version; refusing to guess")]
    Unversioned,
}

/// What `apply` did.
#[derive(Debug, PartialEq, Eq)]
pub enum Applied {
    Created { version: i32 },
    Current { version: i32 },
}

impl std::fmt::Display for Applied {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Applied::Created { version } => write!(f, "created schema version {version}"),
            Applied::Current { version } => write!(f, "schema version {version} is current"),
        }
    }
}

/// Every statement that creates the schema in an empty database, in order.
pub fn declarations() -> Vec<String> {
    let mut statements: Vec<String> = [
        "CREATE EXTENSION IF NOT EXISTS timescaledb",
        "CREATE EXTENSION IF NOT EXISTS pg_trgm",
        "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='sinex_writer') THEN CREATE ROLE sinex_writer; END IF; IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='sinex_reader') THEN CREATE ROLE sinex_reader; END IF; END $$",
    ]
    .map(String::from)
    .to_vec();
    for schema in tables::SCHEMAS {
        statements.push(format!("CREATE SCHEMA {schema}"));
    }
    statements.extend(tables::tables(&Checks::from_model()));
    statements.extend(tables::after_tables());
    statements.extend(visibility());
    statements.extend(kind_views());
    statements.extend(grants());
    statements
}

/// BLAKE3 over the declarations: the name development tooling gives the
/// database it provisions, so a schema change selects a new database and
/// recompiles exactly the crates whose queries read it.
pub fn declaration_hash() -> String {
    let mut hasher = blake3::Hasher::new();
    for statement in declarations() {
        hasher.update(statement.as_bytes());
        hasher.update(&[0]);
    }
    hasher.finalize().to_hex()[..16].to_string()
}

/// CHECK lists generated from the model's enums.
pub(crate) struct Checks {
    pub time_quality: String,
    pub record_state: String,
    pub class: String,
    pub replace_rule: String,
}

impl Checks {
    fn from_model() -> Self {
        Self {
            time_quality: TimeQuality::check("ts_quality"),
            record_state: RecordState::check("record_state"),
            class: Class::check("class"),
            replace_rule: ReplaceRule::check("replace_rule"),
        }
    }
}

/// What a caller may read (architecture §3.5; spec `privacy.md` §4). The
/// API sets `sinex.grants` on its read connection to the classes the caller's
/// token holds (`guarded`, and `sealed` while unlocked); an `open` event is
/// visible to every caller. Every read path goes through these views, so a
/// guarded value never leaves Sinex for a caller without the grant: the event,
/// its type and time stay visible, its payload and equivalence key do not.
fn visibility() -> Vec<String> {
    vec![
        "CREATE FUNCTION core.class_visible(class text) RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT class = 'open'
      OR class = ANY (string_to_array(coalesce(current_setting('sinex.grants', true), ''), ','))
$$"
        .to_string(),
        "CREATE VIEW core.visible_events AS SELECT
  e.id, e.event_type, e.source, e.run_id, e.material_id, e.anchor, e.ordinal, e.parents,
  e.ts_orig, e.ts_quality, e.ts_coided, e.ts_persisted, e.commit_seq,
  CASE WHEN core.class_visible(e.class) THEN e.payload
       ELSE jsonb_build_object('withheld', e.class) END AS payload,
  CASE WHEN core.class_visible(e.class) THEN e.equivalence_key END AS equivalence_key,
  e.reading_order, e.record_hash, e.record_state, e.class, e.support
FROM core.events e"
            .to_string(),
        "CREATE VIEW core.visible_search_text AS SELECT s.event_id, s.text, s.tsv
FROM core.search_text s JOIN core.events e ON e.id = s.event_id
WHERE core.class_visible(e.class)"
            .to_string(),
    ]
}

/// One view per event type with the payload's top-level fields as columns, for
/// read-only SQL (architecture §3.6, `kinds.<kind>`).
fn kind_views() -> Vec<String> {
    let mut out = Vec::new();
    for kind in kinds::ALL {
        let schema = (kind.json_schema)();
        let mut columns = vec![
            "e.id".to_string(),
            "e.source".to_string(),
            "e.ts_orig".to_string(),
            "e.ts_quality".to_string(),
            "e.ts_coided".to_string(),
            "e.record_state".to_string(),
        ];
        if let Some(props) = schema.get("properties").and_then(|p| p.as_object()) {
            for field in props.keys() {
                columns.push(format!("e.payload->'{field}' AS {}", quote_ident(field)));
            }
        }
        out.push(format!(
            "CREATE VIEW kinds.{} AS SELECT {} FROM core.visible_events e WHERE e.event_type='{}' AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)",
            quote_ident(&kind.event_type.replace('.', "_")),
            columns.join(", "),
            kind.event_type
        ));
    }
    out
}

/// Registry rows for event types, written from the Rust catalog.
pub fn registry_rows() -> Vec<(
    String,
    serde_json::Value,
    &'static str,
    Vec<String>,
    &'static str,
)> {
    kinds::ALL
        .iter()
        .map(|k| {
            let (key_kind, fields) = match k.key {
                KeyKind::Location => ("location", vec![]),
                KeyKind::Fields(f) => ("fields", f.iter().map(|s| s.to_string()).collect()),
            };
            (
                k.event_type.to_string(),
                (k.json_schema)(),
                key_kind,
                fields,
                k.replace.as_db(),
            )
        })
        .collect()
}

fn grants() -> Vec<String> {
    let mut out = Vec::new();
    for schema in tables::SCHEMAS {
        out.push(format!(
            "GRANT USAGE ON SCHEMA {schema} TO sinex_writer, sinex_reader"
        ));
        out.push(format!(
            "GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO sinex_reader, sinex_writer"
        ));
        out.push(format!(
            "GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {schema} TO sinex_writer"
        ));
        out.push(format!(
            "GRANT USAGE ON ALL SEQUENCES IN SCHEMA {schema} TO sinex_writer"
        ));
    }
    // Read-only SQL sees events only through the visibility views.
    out.push(
        "REVOKE SELECT ON core.events, reflection.events, core.search_text FROM sinex_reader"
            .to_string(),
    );
    out
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Connect and [`apply`].
pub async fn apply_url(url: &str) -> Result<Applied, Error> {
    let mut connection = PgConnection::connect(url).await?;
    apply(&mut connection).await
}

/// Create the schema in an empty database, or confirm the database is at this
/// build's version. Runs in one transaction.
pub async fn apply(connection: &mut PgConnection) -> Result<Applied, Error> {
    let has_version_table: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='sinex_schema' AND tablename='applied')",
    )
    .fetch_one(&mut *connection)
    .await?;
    if has_version_table {
        let found: Option<i32> =
            sqlx::query_scalar("SELECT max(version) FROM sinex_schema.applied")
                .fetch_one(&mut *connection)
                .await?;
        return match found {
            Some(found) if found == VERSION => Ok(Applied::Current { version: found }),
            Some(found) => Err(Error::Upgrade {
                found,
                declared: VERSION,
            }),
            None => Err(Error::Unversioned),
        };
    }
    let has_core: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname='core')")
            .fetch_one(&mut *connection)
            .await?;
    if has_core {
        return Err(Error::Unversioned);
    }
    let mut tx = connection.begin().await?;
    for statement in declarations() {
        tx.execute(statement.as_str())
            .await
            .map_err(|e| sqlx::Error::Protocol(format!("{e}\nin statement: {statement}")))?;
    }
    tx.execute("CREATE TABLE sinex_schema.applied (version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())")
        .await?;
    for version in 0..=VERSION {
        sqlx::query("INSERT INTO sinex_schema.applied (version) VALUES ($1)")
            .bind(version)
            .execute(&mut *tx)
            .await?;
    }
    for (name, schema, key_kind, fields, replace) in registry_rows() {
        sqlx::query(
            "INSERT INTO registry.event_types (name, payload_schema, key_kind, key_fields, replace_rule) VALUES ($1,$2,$3,$4,$5)",
        )
        .bind(name)
        .bind(schema)
        .bind(key_kind)
        .bind(fields)
        .bind(replace)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(Applied::Created { version: VERSION })
}

/// A comparable description of the database's tables, columns, constraints and
/// indexes in Sinex's schemas: what the fresh-versus-changes test compares.
pub async fn catalog(connection: &mut PgConnection) -> Result<Vec<String>, Error> {
    let schemas: Vec<String> = tables::SCHEMAS.iter().map(|s| s.to_string()).collect();
    let mut rows: Vec<String> = sqlx::query_scalar(
        "SELECT table_schema||'.'||table_name||'.'||column_name||' '||data_type||' '||is_nullable||' '||coalesce(column_default,'') FROM information_schema.columns WHERE table_schema = ANY($1)",
    )
    .bind(&schemas)
    .fetch_all(&mut *connection)
    .await?;
    rows.extend(
        sqlx::query_scalar::<_, String>(
            "SELECT n.nspname||'.'||c.conname||' '||pg_get_constraintdef(c.oid) FROM pg_constraint c JOIN pg_namespace n ON n.oid=c.connamespace WHERE n.nspname = ANY($1)",
        )
        .bind(&schemas)
        .fetch_all(&mut *connection)
        .await?,
    );
    rows.extend(
        sqlx::query_scalar::<_, String>(
            "SELECT schemaname||'.'||indexname||' '||indexdef FROM pg_indexes WHERE schemaname = ANY($1)",
        )
        .bind(&schemas)
        .fetch_all(&mut *connection)
        .await?,
    );
    rows.sort();
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A payload field named like one of the view's event columns makes the
    /// kind's view, and so the whole schema, fail to apply.
    #[test]
    fn no_payload_field_shadows_an_event_column() {
        let reserved = ["id", "source", "ts_orig", "ts_quality", "ts_coided", "record_state"];
        for kind in kinds::ALL {
            let schema = (kind.json_schema)();
            let props = schema.get("properties").and_then(|p| p.as_object());
            for field in props.into_iter().flat_map(|p| p.keys()) {
                assert!(
                    !reserved.contains(&field.as_str()),
                    "{} has a payload field {field:?} that shadows an event column",
                    kind.event_type
                );
            }
        }
    }

    #[test]
    fn declarations_are_deterministic_and_named() {
        assert_eq!(declarations(), declarations());
        assert_eq!(declaration_hash().len(), 16);
        let all = declarations().join("\n");
        for table in [
            "core.events",
            "core.live_coordinates",
            "core.live_equivalence",
            "core.hidden_occurrences",
            "core.commit_log",
            "audit.archived",
            "raw.materials",
            "raw.positions",
            "derivation.dirty",
            "core.admission_stage",
        ] {
            assert!(all.contains(&format!("TABLE {table} (")), "{table}");
        }
        assert!(all.contains("'excise'"));
        assert!(all.contains("CREATE VIEW kinds.\"command_executed\""));
    }
}
