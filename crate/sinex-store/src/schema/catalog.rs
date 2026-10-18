//! The catalog recorded at bootstrap and the check of a live database against it.
use super::declarations::{
    atuin_payload_schema_at, fingerprint_at, first_release_shell_payload, installed_tables,
    nullable_at,
};
use super::tables::Definition;
use super::{SchemaStatus, TIMESCALEDB_VERSION, schema_changes};
use crate::{Error, Result};
use sqlx::{PgConnection, Row};
use std::collections::BTreeMap;

// PostgreSQL rewrites expressions (IN becomes = ANY), so checks compare against the
// text PostgreSQL produced when the declarations were applied, stored in
// `sinex_schemas.definition`. Only the admin role can change that row, and its
// fingerprint must match before the stored catalog is used.
#[derive(serde::Serialize, serde::Deserialize)]
pub(super) struct Catalog {
    constraints: BTreeMap<String, String>,
    expressions: BTreeMap<String, Option<String>>,
    objects: BTreeMap<String, serde_json::Value>,
}

pub(super) async fn capture_catalog(conn: &mut PgConnection) -> Result<Catalog> {
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
    for candidate in 0..=7 {
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
    // Call this SECURITY DEFINER function only after its body, owner and ACL
    // matched the stored catalog above.
    let preloaded: bool = sqlx::query_scalar("SELECT sinex_schemas.timescale_preloaded()")
        .fetch_one(&mut *conn)
        .await?;
    if !preloaded {
        return Err(Error::Schema("TimescaleDB must be preloaded".into()));
    }
    for d in &definitions {
        check_table(conn, stage, d, &catalog).await?;
    }
    let metadata_mutation_grant: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN LATERAL aclexplode(c.relacl) acl WHERE n.nspname='sinex_schemas' AND c.relname IN ('definition','applied_changes') AND acl.grantee<>c.relowner AND acl.privilege_type<>'SELECT') OR EXISTS(SELECT 1 FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN LATERAL aclexplode(a.attacl) acl WHERE n.nspname='sinex_schemas' AND c.relname IN ('definition','applied_changes') AND acl.grantee<>c.relowner AND acl.privilege_type<>'SELECT')").fetch_one(&mut *conn).await?;
    if metadata_mutation_grant {
        return Err(Error::Schema(
            "canonical catalog receipt has a non-owner mutation grant".into(),
        ));
    }
    check_registrations(conn, stage).await?;
    check_roles_and_grants(conn, stage, &expected).await?;
    Ok(SchemaStatus {
        version: 1,
        fingerprint: stored,
        table_count: definitions.len(),
        pending_changes: schema_changes().into_iter().skip(stage).collect(),
        checked_changes: Vec::new(),
    })
}

/// Compares one table's columns, constraints, indexes, owner and row security
/// with its declaration and the stored catalog.
async fn check_table(
    conn: &mut PgConnection,
    stage: usize,
    d: &Definition,
    catalog: &Catalog,
) -> Result<()> {
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
    Ok(())
}

/// Checks registered payload schemas, event types and singleton rows for `stage`.
async fn check_registrations(conn: &mut PgConnection, stage: usize) -> Result<()> {
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
        if canonical.as_ref() != Some(&first_release_shell_payload()) {
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
    Ok(())
}

/// Checks that the Sinex roles are unprivileged and hold exactly their declared grants.
async fn check_roles_and_grants(
    conn: &mut PgConnection,
    stage: usize,
    tables: &[String],
) -> Result<()> {
    let dangerous: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname IN ('sinex_reader','sinex_writer','sinex_read_model') AND (rolsuper OR rolcreaterole OR rolcreatedb OR rolbypassrls OR oid=(SELECT datdba FROM pg_database WHERE datname=current_database()))) OR EXISTS(SELECT 1 FROM pg_auth_members m JOIN pg_roles r ON r.oid=m.member WHERE r.rolname IN ('sinex_reader','sinex_writer','sinex_read_model')) OR NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='sinex_reader') OR NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='sinex_writer')").fetch_one(&mut *conn).await?;
    if dangerous {
        return Err(Error::Schema(
            "dedicated unprivileged reader/writer roles required".into(),
        ));
    }
    for table in tables {
        let bad: bool = sqlx::query_scalar("SELECT NOT has_table_privilege('sinex_reader',$1,'SELECT') OR has_table_privilege('sinex_reader',$1,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') OR has_any_column_privilege('sinex_reader',$1,'INSERT,UPDATE,REFERENCES')").bind(table).fetch_one(&mut *conn).await?;
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
        let good: bool = sqlx::query_scalar("SELECT has_table_privilege('sinex_writer',$1,'SELECT') AND has_table_privilege('sinex_writer',$1,'INSERT')=$2 AND has_table_privilege('sinex_writer',$1,'UPDATE')=$3 AND has_any_column_privilege('sinex_writer',$1,'INSERT')=$2 AND has_any_column_privilege('sinex_writer',$1,'UPDATE')=($3 OR $1='raw.source_material_registry') AND NOT has_any_column_privilege('sinex_writer',$1,'REFERENCES') AND has_table_privilege('sinex_writer',$1,'DELETE')=($1 IN ('core.deleted_occurrences','core.live_equivalence','core.live_coordinates','derivation.shell_dirty','derivation.session_dirty')) AND NOT has_table_privilege('sinex_writer',$1,'TRUNCATE,REFERENCES,TRIGGER')").bind(table).bind(writer_insert).bind(writer_update).fetch_one(&mut *conn).await?;
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
    for table in tables {
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
    Ok(())
}
