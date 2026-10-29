//! Registration is serialized with the writer and preserves every definition.
use crate::Error;
use serde_json::{Value, json};
use sinex_model::kinds::{KeyRule, Normalization, RemovedRule, TimeRule};
use sinex_model::{DbCheck, KindDeclaration, KindRef};
use sqlx::PgConnection;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, OnceLock};

#[derive(Clone, Debug)]
pub struct Owners {
    pub prefixes: BTreeMap<String, String>,
}

impl Default for Owners {
    fn default() -> Self {
        Self {
            prefixes: BTreeMap::from([("polylogue".into(), "polylogue".into())]),
        }
    }
}

fn invalid(reason: impl Into<String>) -> Error {
    Error::Declaration(reason.into())
}

pub async fn load(
    conn: &mut PgConnection,
    refs: &[KindRef],
) -> Result<BTreeMap<KindRef, KindDeclaration>, Error> {
    if refs.is_empty() {
        return Ok(BTreeMap::new());
    }
    // A caller may name the same kind once per event. SQL joins and immutable
    // definition validation operate on exact kind identities, once each.
    let refs = refs.iter().collect::<BTreeSet<_>>();
    let names: Vec<_> = refs.iter().map(|r| r.name.clone()).collect();
    let versions: Vec<_> = refs
        .iter()
        .map(|reference| reference.database_version())
        .collect::<Result<_, _>>()
        .map_err(invalid)?;
    let rows: Vec<Value> = sqlx::query_scalar(
        "SELECT jsonb_build_object('kind', jsonb_build_object('name', e.name, 'version', e.version),
          'declaring_producer', e.declaring_producer, 'payload_schema', e.payload_schema,
          'normalization', e.normalization, 'key_rule', e.key_rule, 'replace_rule', e.replace_rule,
          'time_rule', e.time_rule, 'field_hints', e.field_hints, 'removed_rule', e.removed_rule,
          'keep_known', e.keep_known, 'owned_blobs', e.owned_blobs, 'search', e.search, '_definition_hash', e.definition_hash)
         FROM registry.event_types e JOIN unnest($1::text[], $2::bigint[]) r(name, version)
           ON e.name=r.name AND e.version=r.version")
        .bind(&names).bind(&versions).fetch_all(conn).await?;
    rows.into_iter()
        .map(|mut v| {
            let saved_hash = v
                .as_object_mut()
                .and_then(|o| o.remove("_definition_hash"))
                .and_then(|v| v.as_str().map(String::from))
                .ok_or_else(|| invalid("stored declaration has no hash"))?;
            let d: KindDeclaration = serde_json::from_value(v)
                .map_err(|e| invalid(format!("stored declaration: {e}")))?;
            if saved_hash != d.definition_hash() {
                return Err(invalid(format!(
                    "definition_conflict: stored hash of {} version {} differs",
                    d.kind.name, d.kind.version
                )));
            }
            Ok((d.kind.clone(), d))
        })
        .collect()
}

pub async fn all(conn: &mut PgConnection) -> Result<BTreeMap<KindRef, KindDeclaration>, Error> {
    let rows: Vec<(String, i64)> =
        sqlx::query_as("SELECT name, version FROM registry.event_types ORDER BY name, version")
            .fetch_all(&mut *conn)
            .await?;
    let refs: Vec<_> = rows
        .into_iter()
        .map(|(name, version)| KindRef {
            name,
            version: version as u64,
        })
        .collect();
    load(conn, &refs).await
}

async fn prefix(conn: &mut PgConnection, name: &str, owner: &str) -> Result<(), Error> {
    let found: Option<String> =
        sqlx::query_scalar("SELECT declaring_producer FROM registry.kind_prefixes WHERE prefix=$1")
            .bind(name)
            .fetch_optional(&mut *conn)
            .await?;
    match found {
        Some(found) if found != owner => {
            return Err(invalid(format!(
                "definition_conflict: prefix {name} belongs to {found}, requested {owner}"
            )));
        }
        Some(_) => {}
        None => {
            sqlx::query(
                "INSERT INTO registry.kind_prefixes (prefix, declaring_producer) VALUES ($1,$2)",
            )
            .bind(name)
            .bind(owner)
            .execute(conn)
            .await?;
        }
    }
    Ok(())
}

fn prefix_from_name(name: &str) -> Result<&str, String> {
    let (prefix, _) = name
        .split_once('.')
        .ok_or("kind name needs a producer prefix")?;
    if name.split('.').any(str::is_empty)
        || name.chars().any(|c| {
            !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '_' || c == '-')
        })
    {
        return Err("invalid kind name".into());
    }
    Ok(prefix)
}

pub async fn check_namespace(
    conn: &mut PgConnection,
    name: &str,
    producer: &str,
) -> Result<(), Error> {
    let prefix_name = prefix_from_name(name).map_err(invalid)?;
    let owner: Option<String> =
        sqlx::query_scalar("SELECT declaring_producer FROM registry.kind_prefixes WHERE prefix=$1")
            .bind(prefix_name)
            .fetch_optional(&mut *conn)
            .await?;
    if (owner.as_deref() == Some("sinex") && prefix_name != producer && prefix_name != "polylogue")
        || owner.as_deref().is_some_and(|owner| owner != producer)
        || (owner.is_none() && prefix_name != producer)
    {
        return Err(invalid(format!(
            "namespace_refused: {producer} cannot declare {name}"
        )));
    }
    Ok(())
}

/// Called within the writer's transaction. The entire frame is checked before
/// inserting definitions or claiming an unclaimed prefix.
pub async fn register(
    conn: &mut PgConnection,
    producer: &str,
    declarations: &[KindDeclaration],
) -> Result<Vec<(KindRef, String)>, Error> {
    for d in declarations {
        check_namespace(conn, &d.kind.name, producer).await?;
        if d.normalization != Normalization::Json {
            return Err(invalid("only bundled declarations use Rust validators"));
        }
        if d.key_rule.is_none() {
            return Err(invalid(
                "historical declarations cannot be registered by producers",
            ));
        }
    }
    register_checked(conn, producer, declarations).await
}

async fn register_checked(
    conn: &mut PgConnection,
    producer: &str,
    declarations: &[KindDeclaration],
) -> Result<Vec<(KindRef, String)>, Error> {
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(crate::WRITER_LOCK)
        .execute(&mut *conn)
        .await?;
    let mut checked: BTreeMap<KindRef, &KindDeclaration> = BTreeMap::new();
    for d in declarations {
        d.kind.database_version().map_err(invalid)?;
        if d.declaring_producer != producer {
            return Err(invalid("declaring producer differs from ingress producer"));
        }

        check(d).map_err(invalid)?;
        if let Some(prior) = checked.insert(d.kind.clone(), d)
            && prior != d
        {
            return Err(invalid(format!(
                "definition_conflict: {} version {}",
                d.kind.name, d.kind.version
            )));
        }
    }
    let refs: Vec<_> = checked.keys().cloned().collect();
    let saved = load(conn, &refs).await?;
    for (reference, d) in &checked {
        if let Some(prior) = saved.get(reference)
            && prior != *d
        {
            return Err(invalid(format!(
                "definition_conflict: {} version {}",
                reference.name, reference.version
            )));
        }
        let owner: Option<String> = sqlx::query_scalar(
            "SELECT declaring_producer FROM registry.event_types WHERE name=$1 LIMIT 1",
        )
        .bind(&reference.name)
        .fetch_optional(&mut *conn)
        .await?;
        if owner.as_deref().is_some_and(|owner| owner != producer) {
            return Err(invalid(format!(
                "definition_conflict: {} declaring producer",
                reference.name
            )));
        }
    }
    for d in checked.values() {
        if producer != "sinex" {
            prefix(
                conn,
                d.kind.name.split_once('.').expect("checked prefix").0,
                producer,
            )
            .await?;
        }
        if saved.contains_key(&d.kind) {
            continue;
        }
        sqlx::query("INSERT INTO registry.event_types (name, version, declaring_producer, payload_schema, normalization,
          key_rule, replace_rule, time_rule, field_hints, removed_rule, keep_known, owned_blobs, search, definition_hash)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)")
            .bind(&d.kind.name).bind(d.kind.database_version().map_err(invalid)?)
            .bind(&d.declaring_producer).bind(&d.payload_schema)
            .bind(d.normalization.as_db())
            .bind(json!(d.key_rule)).bind(d.replace_rule.map(|r| r.as_db())).bind(json!(d.time_rule))
            .bind(json!(d.field_hints)).bind(json!(d.removed_rule)).bind(&d.keep_known)
            .bind(json!(d.owned_blobs)).bind(d.search.as_db()).bind(d.definition_hash()).execute(&mut *conn).await?;
    }
    Ok(declarations
        .iter()
        .map(|d| (d.kind.clone(), d.definition_hash()))
        .collect())
}

pub(crate) async fn install(
    conn: &mut PgConnection,
    owners: &Owners,
    declarations: &[KindDeclaration],
) -> Result<(), Error> {
    for declaration in declarations {
        let p = declaration
            .kind
            .name
            .split_once('.')
            .expect("bundled name")
            .0;
        prefix(conn, p, "sinex").await?;
    }
    for (p, owner) in &owners.prefixes {
        prefix(conn, p, owner).await?;
    }
    register_checked(conn, "sinex", declarations).await?;
    Ok(())
}

pub async fn upgrade_historical(conn: &mut PgConnection, owners: &Owners) -> Result<(), Error> {
    let exists: bool =
        sqlx::query_scalar("SELECT to_regclass('registry.unversioned_event_types') IS NOT NULL")
            .fetch_one(&mut *conn)
            .await?;
    if !exists {
        return Ok(());
    }
    let names: Vec<String> = sqlx::query_scalar("SELECT name FROM registry.unversioned_event_types UNION SELECT event_type FROM core.events").fetch_all(&mut *conn).await?;
    let mut grouped: BTreeMap<String, Vec<KindDeclaration>> = BTreeMap::new();
    for name in names {
        let mut d = KindDeclaration::historical(name);
        if d.kind.name.starts_with("polylogue.") {
            d.declaring_producer = owners
                .prefixes
                .get("polylogue")
                .cloned()
                .unwrap_or_else(|| "polylogue".into());
        }
        grouped
            .entry(d.declaring_producer.clone())
            .or_default()
            .push(d);
    }
    for (owner, declarations) in grouped {
        register_checked(conn, &owner, &declarations).await?;
    }
    sqlx::query("DROP TABLE registry.unversioned_event_types")
        .execute(conn)
        .await?;
    Ok(())
}

fn key_fields<'a>(rule: &'a KeyRule, fields: &mut Vec<&'a str>) -> Result<(), String> {
    match rule {
        KeyRule::Location => {}
        KeyRule::Fields { fields: keys } => {
            if keys.is_empty() {
                return Err("field key is empty".into());
            }
            fields.extend(keys.iter().map(String::as_str));
        }
        KeyRule::WhenPresent {
            field,
            fields: keys,
        } => {
            if keys.is_empty() {
                return Err("field key is empty".into());
            }
            fields.push(field);
            fields.extend(keys.iter().map(String::as_str));
        }
        KeyRule::WhenNonempty {
            field,
            then,
            otherwise,
            ..
        }
        | KeyRule::WhenEqual {
            field,
            then,
            otherwise,
            ..
        } => {
            fields.push(field);
            key_fields(then, fields)?;
            key_fields(otherwise, fields)?;
        }
    }
    Ok(())
}

pub fn check(d: &KindDeclaration) -> Result<(), String> {
    let mut hints = std::collections::BTreeSet::new();
    for hint in &d.field_hints {
        if !hint.field.starts_with('/') || !hints.insert(&hint.field) {
            return Err("field hints need distinct JSON Pointers".into());
        }
        let mut bytes = hint.field.bytes();
        while let Some(byte) = bytes.next() {
            if byte == b'~' && !matches!(bytes.next(), Some(b'0' | b'1')) {
                return Err("invalid JSON Pointer in field hint".into());
            }
        }
    }
    if d.key_rule.is_none()
        || d.replace_rule.is_none()
        || d.time_rule.is_none()
        || d.removed_rule.is_none()
    {
        let mut historical = KindDeclaration::historical(d.kind.name.clone());
        historical.declaring_producer = d.declaring_producer.clone();
        if *d == historical {
            return Ok(());
        }
        return Err("declaration is missing rules".into());
    }
    prefix_from_name(&d.kind.name)?;
    let mut fields: Vec<&str> = Vec::new();
    key_fields(d.key_rule.as_ref().expect("checked"), &mut fields)?;
    if let Some(TimeRule::FirstPresent {
        fields: times,
        quality,
    }) = &d.time_rule
    {
        if times.is_empty() || *quality == sinex_model::TimeQuality::Unknown {
            return Err("time rule needs fields and quality".into());
        }
        fields.extend(times.iter().map(String::as_str));
    }
    if let Some(TimeRule::InstantEvidence {
        field,
        evidence_field,
    }) = &d.time_rule
    {
        fields.push(field);
        fields.push(evidence_field);
    }
    if let Some(RemovedRule::WhenPresent { field }) = &d.removed_rule {
        fields.push(field);
    }
    fields.extend(d.keep_known.iter().map(String::as_str));
    for b in &d.owned_blobs {
        fields.push(&b.field);
        fields.push(&b.size);
        if let Some(w) = &b.when {
            fields.push(w);
        }
    }
    if fields.iter().any(|field| field.is_empty()) {
        return Err("empty rule field".into());
    }
    if let Some(schema) = &d.payload_schema {
        let properties = declared_fields(schema, schema, &mut std::collections::BTreeSet::new());
        for hint in &d.field_hints {
            let root_field = hint.field[1..]
                .split('/')
                .next()
                .expect("pointer has a field")
                .replace("~1", "/")
                .replace("~0", "~");
            if !properties.contains(&root_field) {
                return Err(format!(
                    "hint field {} is absent from the schema",
                    hint.field
                ));
            }
        }
        if let Some(field) = fields.iter().find(|field| !properties.contains(**field)) {
            return Err(format!("rule field {field} is absent from the schema"));
        }
        if d.normalization == Normalization::Json {
            validator(d)?;
        }
    }
    Ok(())
}

fn declared_fields(
    schema: &Value,
    root: &Value,
    active: &mut std::collections::BTreeSet<String>,
) -> std::collections::BTreeSet<String> {
    let mut fields: std::collections::BTreeSet<String> = schema
        .get("properties")
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|p| p.keys().cloned())
        .collect();
    if let Some(reference) = schema.get("$ref").and_then(Value::as_str)
        && active.insert(reference.into())
    {
        if let Some(target) = reference.strip_prefix('#').and_then(|r| root.pointer(r)) {
            fields.extend(declared_fields(target, root, active));
        }
        active.remove(reference);
    }
    for key in ["anyOf", "oneOf", "allOf"] {
        if let Some(branches) = schema.get(key).and_then(Value::as_array) {
            for branch in branches {
                fields.extend(declared_fields(branch, root, active));
            }
        }
    }
    fields
}

pub fn validator(d: &KindDeclaration) -> Result<Option<Arc<jsonschema::Validator>>, String> {
    let Some(schema) = &d.payload_schema else {
        return Ok(None);
    };
    if d.normalization == Normalization::Rust {
        return Ok(None);
    }
    fn local_refs(v: &Value) -> bool {
        match v {
            Value::Object(o) => o.iter().all(|(k, v)| {
                if k == "$ref" || k == "$dynamicRef" {
                    v.as_str().is_some_and(|s| s.starts_with('#'))
                } else if k == "$id" {
                    false
                } else {
                    local_refs(v)
                }
            }),
            Value::Array(a) => a.iter().all(local_refs),
            _ => true,
        }
    }
    if !local_refs(schema) {
        return Err("schemas may use local references only".into());
    }
    static CACHE: OnceLock<Mutex<HashMap<String, Arc<jsonschema::Validator>>>> = OnceLock::new();
    let mut cache = CACHE
        .get_or_init(Default::default)
        .lock()
        .map_err(|_| "validator cache unavailable")?;
    let hash = d.definition_hash();
    if let Some(v) = cache.get(&hash) {
        return Ok(Some(v.clone()));
    }
    let v = Arc::new(
        jsonschema::options()
            .with_draft(jsonschema::Draft::Draft202012)
            .should_validate_formats(true)
            .build(schema)
            .map_err(|e| format!("invalid schema at {}", e.instance_path()))?,
    );
    cache.insert(hash, v.clone());
    Ok(Some(v))
}

/// Saved outside owners used when a writer checks its bundled declarations.
pub async fn owners(conn: &mut PgConnection) -> Result<Owners, Error> {
    let prefixes:Vec<(String,String)>=sqlx::query_as("SELECT prefix,declaring_producer FROM registry.kind_prefixes WHERE declaring_producer<>'sinex'").fetch_all(conn).await?;
    Ok(Owners {
        prefixes: prefixes.into_iter().collect(),
    })
}

/// Writer startup reads the installed description; only schema apply writes it.
pub async fn require_bundled(conn: &mut PgConnection) -> Result<(), Error> {
    let expected = crate::description::Description::current().kinds;
    let references: Vec<_> = expected
        .iter()
        .map(|declaration| declaration.kind.clone())
        .collect();
    let saved = load(conn, &references).await?;
    for declaration in expected {
        if saved.get(&declaration.kind) != Some(&declaration) {
            return Err(invalid(format!(
                "definition_conflict: {} version {}; apply this build's schema before starting the writer",
                declaration.kind.name, declaration.kind.version
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod version_tests;
