//! The declarations create an empty database at the current schema version,
//! [`VERSION`]. A database at an older version is upgraded by the forward
//! upgrades in `upgrades.rs`, in one transaction; a newer one is refused.
//!
//! Enum CHECK lists come from the Rust enums (`DbCheck`), and the event-type
//! bundled registry rows and views come from `sinex_model::event_types::ALL`.
//! Outside declarations are immutable data, registered by their producer.
//!
//! This crate uses no compile-time-checked queries: it builds before any
//! database exists and provisions the one the store's queries are checked
//! against.
use sinex_model::{ArchiveReason, Class, DbCheck, RecordState, TimeQuality, event_types};
use sqlx::{Connection, Executor, PgConnection};

mod description;
pub mod kinds;
mod tables;
mod upgrades;

/// The session advisory lock held by the database's one writer.
pub const WRITER_LOCK: i64 = 0x7369_6e65_7857_5254;

/// The schema version these declarations create: one more than the last
/// upgrade starts from.
pub const VERSION: i32 = upgrades::LANDED_VERSION + upgrades::NAMED.len() as i32;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error("database is at schema version {found}, this build declares version {VERSION}")]
    Version { found: i32 },
    #[error("database has Sinex objects but no schema version; refusing to guess")]
    Unversioned,
    #[error("a writer holds the database; stop sinexd before upgrading its schema")]
    WriterRunning,
    #[error("{0}")]
    Declaration(String),
}

/// What `apply` did.
#[derive(Debug, PartialEq, Eq)]
pub enum Applied {
    Created { version: i32 },
    Current { version: i32 },
    Upgraded { from: i32, to: i32 },
}

impl std::fmt::Display for Applied {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Applied::Created { version } => write!(f, "created schema version {version}"),
            Applied::Current { version } => write!(f, "schema version {version} is current"),
            Applied::Upgraded { from, to } => {
                write!(f, "upgraded schema version {from} to {to}")
            }
        }
    }
}

/// What only a superuser may create: the extensions and the two roles. Each
/// is created only when missing, so the owner can apply the schema once a
/// superuser has run these ([`prepare`]).
fn privileged() -> Vec<String> {
    [
        "CREATE EXTENSION IF NOT EXISTS timescaledb",
        "CREATE EXTENSION IF NOT EXISTS pg_trgm",
        "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='sinex_writer') THEN CREATE ROLE sinex_writer; END IF; IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='sinex_reader') THEN CREATE ROLE sinex_reader; END IF; END $$",
    ]
    .map(String::from)
    .to_vec()
}

/// Run as the database superuser before the owner applies the schema: create
/// the extensions and roles, and give `owner` both roles.
pub async fn prepare(connection: &mut PgConnection, owner: &str) -> Result<(), Error> {
    for statement in privileged() {
        connection.execute(statement.as_str()).await?;
    }
    connection
        .execute("GRANT EXECUTE ON FUNCTION pg_control_system() TO sinex_writer")
        .await?;
    connection
        .execute(format!("GRANT sinex_writer, sinex_reader TO {}", quote_ident(owner)).as_str())
        .await?;
    Ok(())
}

/// Every statement that creates the schema in an empty database, in order.
pub fn declarations() -> Vec<String> {
    let mut statements = privileged();
    for schema in tables::SCHEMAS {
        statements.push(format!("CREATE SCHEMA {schema}"));
    }
    statements.push("CREATE TABLE sinex_schema.applied (version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())".into());
    statements.extend(description::Description::current().statements());
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
    for kind in description::Description::current().kinds {
        hasher.update(kind.definition_hash().as_bytes());
        hasher.update(&[0]);
    }
    hasher.finalize().to_hex()[..16].to_string()
}

/// CHECK lists generated from the model's enums.
pub(crate) struct Checks {
    pub time_quality: String,
    pub record_state: String,
    pub class: String,
    pub archive_reason: String,
    /// The archive reasons, and `unhide` for a reading put back.
    pub archive_change_reason: String,
}

impl Checks {
    fn from_model() -> Self {
        Self {
            time_quality: TimeQuality::check("ts_quality"),
            record_state: RecordState::check("record_state"),
            class: Class::check("class"),
            archive_reason: ArchiveReason::check("reason"),
            archive_change_reason: {
                let mut reasons: Vec<String> = ArchiveReason::VALUES
                    .iter()
                    .map(|v| format!("'{v}'"))
                    .collect();
                reasons.push("'unhide'".into());
                format!("CHECK (reason IN ({}))", reasons.join(", "))
            },
        }
    }
}

/// What a caller may read. The API sets `sinex.grants` on its read connection
/// to the classes the caller's token holds (`guarded`, and `sealed` while
/// unlocked); an `open` event is visible to every caller. Every read path goes
/// through these views, so a guarded value never leaves Sinex for a caller
/// without the grant: the event, its type and time stay visible, its payload
/// and equivalence key do not.
fn visibility() -> Vec<String> {
    vec![
        "CREATE OR REPLACE FUNCTION core.payload_time(value text) RETURNS timestamptz LANGUAGE plpgsql IMMUTABLE STRICT AS $$ BEGIN
 IF value !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt][0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]+)?([Zz]|[+-][0-9]{2}:[0-9]{2})$' THEN RETURN NULL; END IF;
 RETURN value::timestamptz;
 EXCEPTION WHEN invalid_datetime_format OR datetime_field_overflow THEN RETURN NULL; END $$".to_string(),
        "CREATE OR REPLACE FUNCTION core.class_visible(class text) RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT class = 'open'
      OR class = ANY (string_to_array(coalesce(current_setting('sinex.grants', true), ''), ','))
$$"
        .to_string(),
        "CREATE VIEW core.visible_events AS SELECT
  e.id, e.event_type, e.source, e.run_id, e.material_id, e.anchor, e.output_index, e.parents,
  e.ts_orig, e.ts_quality, e.ts_coided, e.ts_persisted, e.commit_seq,
  CASE WHEN core.class_visible(e.class) THEN e.payload
       ELSE jsonb_build_object('withheld', e.class) END AS payload,
  CASE WHEN core.class_visible(e.class) THEN e.equivalence_key END AS equivalence_key,
  e.reading_order, e.record_hash, e.record_state, e.class, e.kind_version, e.validation
FROM core.events e"
            .to_string(),
        "CREATE VIEW core.visible_search_text AS SELECT s.event_id, s.text, s.tsv, s.trigram_indexed
FROM core.search_text s JOIN core.events e ON e.id = s.event_id
WHERE core.class_visible(e.class)"
            .to_string(),
    ]
}

/// One view per event type with the payload's top-level fields as columns, for
/// read-only SQL (`event_types.<type>`).
fn kind_view(kind: &event_types::RegisteredEventType) -> description::View {
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
    columns.push("e.kind_version".into());
    columns.push("e.validation".into());
    let name = kind.event_type.replace('.', "_");
    let mut names = vec![
        "id",
        "source",
        "ts_orig",
        "ts_quality",
        "ts_coided",
        "record_state",
    ]
    .into_iter()
    .map(String::from)
    .collect::<Vec<_>>();
    if let Some(props) = schema.get("properties").and_then(|p| p.as_object()) {
        names.extend(props.keys().cloned());
    }
    names.extend(["kind_version".into(), "validation".into()]);
    description::View {
        name: name.clone(),
        columns: names,
        statement: format!(
            "CREATE VIEW event_types.{} AS SELECT {} FROM core.visible_events e WHERE e.event_type='{}' AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)",
            quote_ident(&name),
            columns.join(", "),
            kind.event_type
        ),
    }
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
    out.push("REVOKE SELECT ON core.events, core.search_text FROM sinex_reader".to_string());
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

/// Refuse a database that is not at this build's schema version.
pub async fn require_current(connection: &mut PgConnection) -> Result<(), Error> {
    let found: Option<i32> = sqlx::query_scalar("SELECT max(version) FROM sinex_schema.applied")
        .fetch_one(connection)
        .await?;
    match found {
        Some(VERSION) => Ok(()),
        Some(found) => Err(Error::Version { found }),
        None => Err(Error::Unversioned),
    }
}

/// Create the schema in an empty database, upgrade one at an older version,
/// or confirm that an existing one is at this version. Creation and an
/// upgrade are each one transaction.
pub async fn apply(connection: &mut PgConnection) -> Result<Applied, Error> {
    apply_with_owners(connection, &kinds::Owners::default()).await
}

pub async fn apply_with_owners(
    connection: &mut PgConnection,
    owners: &kinds::Owners,
) -> Result<Applied, Error> {
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
            Some(VERSION) => apply_current(connection, owners).await,
            Some(found) if (0..VERSION).contains(&found) => {
                upgrade(connection, found, owners).await
            }
            Some(found) => Err(Error::Version { found }),
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
    let free: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
        .bind(WRITER_LOCK)
        .fetch_one(&mut *tx)
        .await?;
    if !free {
        tx.rollback().await?;
        return Err(Error::WriterRunning);
    }
    match create_schema(&mut tx, owners).await {
        Ok(()) => {
            tx.commit().await?;
            Ok(Applied::Created { version: VERSION })
        }
        Err(error) => {
            // Release the failed DDL's locks before returning the borrowed
            // connection, even if its caller performs no more IO.
            tx.rollback().await?;
            Err(error)
        }
    }
}

/// Run the upgrades from `from` to [`VERSION`] in one transaction, while no
/// writer holds the database: a writer of an older build would not keep the
/// new tables current.
async fn upgrade(
    connection: &mut PgConnection,
    from: i32,
    owners: &kinds::Owners,
) -> Result<Applied, Error> {
    let mut tx = connection.begin().await?;
    match run_upgrades(&mut tx, from, owners).await {
        Ok(()) => {
            tx.commit().await?;
            Ok(Applied::Upgraded { from, to: VERSION })
        }
        Err(error) => {
            tx.rollback().await?;
            Err(error)
        }
    }
}

async fn run_upgrades(
    connection: &mut PgConnection,
    from: i32,
    owners: &kinds::Owners,
) -> Result<(), Error> {
    let free: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
        .bind(WRITER_LOCK)
        .fetch_one(&mut *connection)
        .await?;
    if !free {
        return Err(Error::WriterRunning);
    }
    let all = upgrades::upgrades();
    let start = usize::try_from(from).expect("an upgrade starts at a version from 0");
    for (version, statements) in (from..).zip(&all[start..]) {
        for statement in statements {
            connection
                .execute(statement.as_str())
                .await
                .map_err(|e| sqlx::Error::Protocol(format!("{e}\nin statement: {statement}")))?;
        }
        sqlx::query("INSERT INTO sinex_schema.applied (version) VALUES ($1)")
            .bind(version + 1)
            .execute(&mut *connection)
            .await?;
    }
    kinds::upgrade_historical(connection, owners).await?;
    description::Description::current()
        .apply(connection, owners)
        .await?;
    Ok(())
}

async fn apply_current(
    connection: &mut PgConnection,
    owners: &kinds::Owners,
) -> Result<Applied, Error> {
    let mut tx = connection.begin().await?;
    let free: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
        .bind(WRITER_LOCK)
        .fetch_one(&mut *tx)
        .await?;
    if !free {
        tx.rollback().await?;
        return Err(Error::WriterRunning);
    }
    let result = description::Description::current()
        .apply(&mut tx, owners)
        .await;
    match result {
        Ok(()) => {
            tx.commit().await?;
            Ok(Applied::Current { version: VERSION })
        }
        Err(error) => {
            tx.rollback().await?;
            Err(error)
        }
    }
}

async fn apply_views(
    connection: &mut PgConnection,
    description: &description::Description,
) -> Result<(), Error> {
    for statement in &description.visibility {
        connection
            .execute(
                statement
                    .replacen("CREATE VIEW", "CREATE OR REPLACE VIEW", 1)
                    .as_str(),
            )
            .await?;
    }
    for view in &description.views {
        let name = &view.name;
        let existing: Vec<String> = sqlx::query_scalar("SELECT column_name FROM information_schema.columns WHERE table_schema='event_types' AND table_name=$1 ORDER BY ordinal_position")
            .bind(&name).fetch_all(&mut *connection).await?;
        let expected = &view.columns;
        let mut grants: Vec<(String, String, String)> = vec![];
        let mut owner: Option<String> = None;
        if !expected.starts_with(&existing) {
            owner = sqlx::query_scalar(
                "SELECT viewowner FROM pg_views WHERE schemaname='event_types' AND viewname=$1",
            )
            .bind(&name)
            .fetch_optional(&mut *connection)
            .await?;
            grants = sqlx::query_as("SELECT grantee, privilege_type, is_grantable FROM information_schema.role_table_grants WHERE table_schema='event_types' AND table_name=$1")
                .bind(&name).fetch_all(&mut *connection).await?;
            // DROP refuses an outside dependency and names it; no CASCADE.
            connection
                .execute(format!("DROP VIEW event_types.{}", quote_ident(&name)).as_str())
                .await?;
        }
        connection
            .execute(
                view.statement
                    .replacen("CREATE VIEW", "CREATE OR REPLACE VIEW", 1)
                    .as_str(),
            )
            .await?;
        if let Some(owner) = owner {
            connection
                .execute(
                    format!(
                        "ALTER VIEW event_types.{} OWNER TO {}",
                        quote_ident(&name),
                        quote_ident(&owner)
                    )
                    .as_str(),
                )
                .await?;
        }
        for (role, privilege, grantable) in grants {
            let role = if role == "PUBLIC" {
                role
            } else {
                quote_ident(&role)
            };
            connection
                .execute(
                    format!(
                        "GRANT {privilege} ON event_types.{} TO {role}{}",
                        quote_ident(&name),
                        if grantable == "YES" {
                            " WITH GRANT OPTION"
                        } else {
                            ""
                        }
                    )
                    .as_str(),
                )
                .await?;
        }
    }
    for statement in grants() {
        connection.execute(statement.as_str()).await?;
    }
    Ok(())
}

async fn create_schema(connection: &mut PgConnection, owners: &kinds::Owners) -> Result<(), Error> {
    let description = description::Description::current();
    let mut statements = privileged();
    statements.extend(
        tables::SCHEMAS
            .iter()
            .map(|schema| format!("CREATE SCHEMA {schema}")),
    );
    statements.push("CREATE TABLE sinex_schema.applied (version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())".into());
    statements.extend(description.tables.clone());
    statements.extend(description.after_tables.clone());
    for statement in statements {
        connection
            .execute(statement.as_str())
            .await
            .map_err(|e| sqlx::Error::Protocol(format!("{e}\nin statement: {statement}")))?;
    }
    sqlx::query("INSERT INTO sinex_schema.applied (version) VALUES ($1)")
        .bind(VERSION)
        .execute(&mut *connection)
        .await?;
    description.apply(connection, owners).await?;
    Ok(())
}

/// The PostgreSQL schemas Sinex owns, in creation order.
pub fn schemas() -> &'static [&'static str] {
    tables::SCHEMAS
}

/// A comparable description of the database's tables, columns, constraints and
/// indexes in Sinex's schemas.
pub async fn catalog(connection: &mut PgConnection) -> Result<Vec<String>, Error> {
    let schemas: Vec<String> = tables::SCHEMAS.iter().map(|s| s.to_string()).collect();
    let mut rows: Vec<String> = sqlx::query_scalar(
        "SELECT table_schema||'.'||table_name||'.'||column_name||' '||ordinal_position||' '||data_type||' '||is_nullable||' '||coalesce(column_default,'')||' generated='||coalesce(generation_expression,'')||' identity='||is_identity||':'||coalesce(identity_generation,'') FROM information_schema.columns WHERE table_schema = ANY($1)",
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
    for query in [
        "SELECT schemaname||'.'||viewname||' '||definition FROM pg_views WHERE schemaname=ANY($1)",
        "SELECT n.nspname||'.'||p.proname||' '||pg_get_functiondef(p.oid) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=ANY($1) AND p.prokind='f'",
        "SELECT n.nspname||'.'||c.relname||'.'||t.tgname||' '||t.tgenabled::text||' '||pg_get_triggerdef(t.oid) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=ANY($1) AND NOT t.tgisinternal",
        "SELECT 'dimension '||to_jsonb(d)::text FROM timescaledb_information.dimensions d WHERE hypertable_schema=ANY($1)",
    ] {
        rows.extend(
            sqlx::query_scalar::<_, String>(query)
                .bind(&schemas)
                .fetch_all(&mut *connection)
                .await?,
        );
    }
    rows.sort();
    Ok(rows)
}

#[cfg(test)]
mod tests;
