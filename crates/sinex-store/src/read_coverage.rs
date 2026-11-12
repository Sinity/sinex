//! Owner-signed admitted-input coverage; no server-side replay ledger.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use sinex_model::KindRef;
use sqlx::PgConnection;
mod parents;

pub(crate) async fn validate_parents(
    conn: &mut PgConnection,
    frame: &sinex_model::IngressFrame,
    events: &[sinex_model::Event],
) -> Result<()> {
    parents::validate(conn, frame, events).await
}

#[derive(Debug, thiserror::Error)]
pub enum AuthorityError {
    #[error("{0}")]
    Invalidated(String),
    #[error("{0}")]
    Unverified(#[source] Error),
}
impl From<Error> for AuthorityError {
    fn from(error: Error) -> Self {
        Self::Unverified(error)
    }
}
impl From<sqlx::Error> for AuthorityError {
    fn from(error: sqlx::Error) -> Self {
        Self::Unverified(Error::from(error))
    }
}
impl From<sinex_schema::Error> for AuthorityError {
    fn from(error: sinex_schema::Error) -> Self {
        Self::Unverified(Error::from(error))
    }
}
impl From<AuthorityError> for Error {
    fn from(error: AuthorityError) -> Self {
        match error {
            AuthorityError::Invalidated(reason) => Self::Invalid(reason),
            AuthorityError::Unverified(error) => error,
        }
    }
}

pub const DOMAIN: &str = "sinex.owner_protocol.admitted_input_coverage.v1";
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Claims {
    pub purpose: String,
    pub producer: String,
    pub family: String,
    pub cache: String,
    pub scope: String,
    pub position: i64,
    pub instance: String,
    pub catalog: String,
    pub host: String,
    pub settings_identity: String,
    pub owner_revision: u64,
    pub kinds: Vec<(KindRef, String)>,
    pub indexed_kinds: Vec<(KindRef, String)>,
    pub classes: Vec<String>,
    pub replay_root: Option<String>,
}
pub async fn sign(conn: &mut PgConnection, claims: &Claims) -> Result<String> {
    crate::owner_protocol::sign(&crate::owner_protocol::key(conn).await?, DOMAIN, claims)
}
pub async fn open(conn: &mut PgConnection, stamp: &str) -> Result<Claims> {
    crate::owner_protocol::open(&crate::owner_protocol::key(conn).await?, DOMAIN, stamp)
}
/// Ordinary admission and lifecycle history remain readable as of the pinned
/// position. Irrecoverable removal of a pinned input invalidates its replay.
pub async fn membership_changed(conn: &mut PgConnection, claims: &Claims) -> Result<bool> {
    let kinds = claims
        .indexed_kinds
        .iter()
        .map(|(kind, _)| kind.name.clone())
        .collect::<Vec<_>>();
    Ok(sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM core.event_tombstones WHERE commit_seq>$1 AND event_type=ANY($2) AND (original_commit_seq<=$1 OR original_commit_seq IS NULL))")
        .bind(claims.position).bind(kinds).fetch_one(conn).await?)
}
pub async fn current(
    conn: &mut PgConnection,
    claims: &Claims,
) -> std::result::Result<(), AuthorityError> {
    let head = crate::query::current_seq(conn).await?;
    if claims.position < 0 || claims.position > head {
        return Err(AuthorityError::Invalidated(
            "coverage_position_outside_owner_history".into(),
        ));
    }
    let saved: Option<(i64, serde_json::Value)> =
        sqlx::query_as("SELECT revision,state FROM core.signal_checkpoints WHERE host=$1")
            .bind(&claims.host)
            .fetch_optional(&mut *conn)
            .await?;
    let Some((revision, state)) = saved else {
        return Err(AuthorityError::Invalidated(
            "coverage_owner_policy_unavailable".into(),
        ));
    };
    if u64::try_from(revision)
        .ok()
        .is_none_or(|revision| revision < claims.owner_revision)
        || state
            .get("settings_identity")
            .and_then(|value| value.as_str())
            != Some(claims.settings_identity.as_str())
    {
        return Err(AuthorityError::Invalidated(
            "coverage_owner_settings_changed".into(),
        ));
    }
    if membership_changed(conn, claims).await? {
        return Err(AuthorityError::Invalidated(
            "coverage_input_membership_changed".into(),
        ));
    }
    Ok(())
}
/// Called only for a new frame, after retry lookup and under the admission lock.
pub async fn validate(conn: &mut PgConnection, frame: &sinex_model::IngressFrame) -> Result<bool> {
    let Some(expected) = &frame.coverage else {
        return Ok(false);
    };
    let key = crate::owner_protocol::key(conn).await?;
    let claims: Claims = crate::owner_protocol::open(&key, DOMAIN, &expected.stamp)?;
    if claims.replay_root.is_none()
        || claims.purpose != "write"
        || claims.producer != frame.producer
        || frame.family.as_deref() != Some(claims.family.as_str())
        || claims.position != expected.position
        || claims.scope != expected.scope
        || claims.catalog != expected.catalog
        || claims.instance != expected.instance
        || claims.host != expected.host
        || claims.settings_identity != expected.settings_identity
    {
        return Err(Error::Invalid("coverage_frame_authority_changed".into()));
    }
    sqlx::query("SELECT host FROM core.signal_checkpoints WHERE host=$1 FOR SHARE")
        .bind(&claims.host)
        .execute(&mut *conn)
        .await?;
    current(conn, &claims).await?;
    Ok(true)
}

#[derive(sqlx::FromRow)]
pub struct InputRow {
    pub id: uuid::Uuid,
    pub commit_seq: i64,
    pub source: String,
    pub event_type: String,
    pub class: String,
    pub kind_version: i64,
    pub source_version: Option<i64>,
    pub source_fingerprint: Option<String>,
    pub producer_family: Option<String>,
    pub payload: serde_json::Value,
    pub archived: bool,
    pub lifecycle_position: Option<i64>,
    pub history_unknown: bool,
}
pub async fn inputs(
    conn: &mut PgConnection,
    claims: &Claims,
    after: Option<(i64, uuid::Uuid)>,
    limit: i64,
) -> Result<Vec<InputRow>> {
    let names = claims
        .indexed_kinds
        .iter()
        .map(|(kind, _)| kind.name.clone())
        .collect::<Vec<_>>();
    Ok(sqlx::query_as("SELECT e.id,e.commit_seq,e.source,e.event_type,e.class,e.kind_version,e.source_version,e.source_fingerprint,e.producer_family,e.payload,coalesce(h.archived,false) AS archived,h.commit_seq AS lifecycle_position,(h.commit_seq IS NULL AND (EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id AND a.archived_seq<=$1) OR coalesce((SELECT NOT c.archived FROM audit.archive_changes c WHERE c.event_id=e.id AND c.commit_seq>$1 ORDER BY c.commit_seq LIMIT 1),false))) AS history_unknown FROM core.events e LEFT JOIN LATERAL (SELECT c.archived,c.commit_seq FROM audit.archive_changes c WHERE c.event_id=e.id AND c.commit_seq<=$1 ORDER BY c.commit_seq DESC LIMIT 1) h ON true WHERE e.event_type=ANY($2) AND e.commit_seq<=$1 AND ($3::bigint IS NULL OR (e.commit_seq,e.id)>($3,$4::uuid)) AND e.class=ANY($6) ORDER BY e.commit_seq,e.id LIMIT $5")
        .bind(claims.position).bind(names).bind(after.map(|after|after.0)).bind(after.map(|after|after.1)).bind(limit).bind(&claims.classes).fetch_all(conn).await?)
}

/// Check the immutable catalog binding. Each bounded projection page checks
/// every selected row's version and availability before signing completion;
/// a completed certificate need not enumerate the same immutable history again.
pub async fn projection_catalog(
    conn: &mut PgConnection,
    claims: &Claims,
) -> std::result::Result<(), AuthorityError> {
    let refs = claims
        .indexed_kinds
        .iter()
        .map(|(kind, _)| kind.clone())
        .collect::<Vec<_>>();
    let registered = sinex_schema::kinds::load(conn, &refs).await?;
    if claims.indexed_kinds.iter().any(|(kind, hash)| {
        registered
            .get(kind)
            .is_none_or(|definition| definition.definition_hash() != *hash)
    }) {
        return Err(AuthorityError::Invalidated(
            "projection_kind_catalog_unknown_or_changed".into(),
        ));
    }
    Ok(())
}
/// Validate the whole retained contract, including archived readings. Missing
/// authority is evidence of incomplete coverage, never a silently skipped row.
pub async fn input_contract(
    conn: &mut PgConnection,
    claims: &Claims,
) -> std::result::Result<(), AuthorityError> {
    let refs = claims
        .kinds
        .iter()
        .map(|(kind, _)| kind.clone())
        .collect::<Vec<_>>();
    let known = sinex_schema::kinds::load(conn, &refs).await?;
    if claims.kinds.iter().any(|(kind, hash)| {
        known
            .get(kind)
            .is_none_or(|known| known.definition_hash() != *hash)
    }) {
        return Err(AuthorityError::Invalidated(
            "coverage_kind_catalog_changed".into(),
        ));
    }
    let names = refs
        .iter()
        .map(|kind| kind.name.clone())
        .collect::<Vec<_>>();
    let rows:Vec<RequiredInput>=sqlx::query_as("SELECT DISTINCT e.event_type,e.kind_version,e.source,e.source_version,e.source_fingerprint,e.producer_family,e.class FROM core.events e WHERE e.event_type=ANY($1) AND e.commit_seq<=$2")
        .bind(names).bind(claims.position).fetch_all(&mut *conn).await?;
    for (kind, version, source, source_version, fingerprint, family, class) in rows {
        if family.as_deref() != Some(claims.family.as_str()) {
            return Err(AuthorityError::Invalidated(
                "coverage_required_input_family_changed".into(),
            ));
        }
        if class == "sealed" {
            return Err(AuthorityError::Invalidated(
                "coverage_required_projection_locked".into(),
            ));
        }
        if !refs
            .iter()
            .any(|known| known.name == kind && i64::try_from(known.version).ok() == Some(version))
        {
            return Err(AuthorityError::Invalidated(
                "coverage_required_kind_version_unknown".into(),
            ));
        }
        let source = sinex_model::SourceRef {
            name: source,
            version: u32::try_from(source_version.ok_or_else(|| {
                AuthorityError::Invalidated("coverage_native_source_unknown".into())
            })?)
            .map_err(|_| AuthorityError::Invalidated("coverage_native_source_unknown".into()))?,
        };
        let declaration = crate::source_declarations::load(conn, &source)
            .await?
            .ok_or_else(|| AuthorityError::Invalidated("coverage_native_source_unknown".into()))?;
        let reference = KindRef {
            name: kind,
            version: u64::try_from(version).map_err(|_| {
                AuthorityError::Invalidated("coverage_required_kind_version_unknown".into())
            })?,
        };
        let hash = claims
            .kinds
            .iter()
            .find(|(known, _)| known == &reference)
            .map(|(_, hash)| hash.as_str())
            .ok_or_else(|| {
                AuthorityError::Invalidated("coverage_required_kind_version_unknown".into())
            })?;
        if fingerprint.as_deref() != Some(declaration.fingerprint().as_str())
            || declaration.family != claims.family
            || !declaration.allows(&reference, hash)
        {
            return Err(AuthorityError::Invalidated(
                "coverage_native_source_schema_changed".into(),
            ));
        }
    }
    Ok(())
}
pub async fn required_classes(
    conn: &mut PgConnection,
    claims: &Claims,
) -> Result<Vec<(String, String)>> {
    let names = claims
        .kinds
        .iter()
        .map(|(kind, _)| kind.name.clone())
        .collect::<Vec<_>>();
    Ok(sqlx::query_as("SELECT DISTINCT e.event_type,e.class FROM core.events e WHERE e.event_type=ANY($1) AND e.commit_seq<=$2")
        .bind(names).bind(claims.position).fetch_all(conn).await?)
}

/// One distinct required input: kind, kind version, source, source version,
/// source fingerprint, producer family and class.
type RequiredInput = (
    String,
    i64,
    String,
    Option<i64>,
    Option<String>,
    Option<String>,
    String,
);

#[cfg(test)]
mod tests;
