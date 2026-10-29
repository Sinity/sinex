//! Fixed WorkSnapshot admitted-input contract and stateless replay certificates.
use super::*;
use serde::{Deserialize, Serialize};
use sinex_protocol::{
    CoverageProof, CoverageRequest, CoverageStatus, InputReference, InputReplayReply,
    ReplayBasisStatus, ReplayContinuation,
};
use sinex_store::read_coverage::{self, Claims};
mod delivery;

#[derive(Debug, thiserror::Error)]
enum Failure {
    #[error("{0}")]
    Invalidated(String),
    #[error("{0}")]
    Unverified(String),
}
impl From<String> for Failure {
    fn from(reason: String) -> Self {
        Self::Unverified(reason)
    }
}
impl From<&str> for Failure {
    fn from(reason: &str) -> Self {
        Self::Unverified(reason.into())
    }
}
impl From<sinex_store::read_coverage::AuthorityError> for Failure {
    fn from(error: sinex_store::read_coverage::AuthorityError) -> Self {
        match error {
            sinex_store::read_coverage::AuthorityError::Invalidated(reason) => {
                Self::Invalidated(reason)
            }
            sinex_store::read_coverage::AuthorityError::Unverified(error) => {
                Self::Unverified(error.to_string())
            }
        }
    }
}
impl From<Failure> for String {
    fn from(error: Failure) -> Self {
        error.to_string()
    }
}

const RAW: &[&str] = &[
    "input_observation",
    "blob",
    "session",
    "message",
    "document",
    "event",
    "link",
    "background",
    "repository",
    "parse_progress",
];
pub(super) fn required_kind(name: &str) -> bool {
    name.strip_prefix("polylogue.")
        .is_some_and(|name| RAW.contains(&name))
}
const CURSOR_DOMAIN: &str = "sinex.owner_protocol.admitted_input_replay.v1";
const PAGE: i64 = 256;

struct Contract {
    family: String,
    scope: String,
    catalog: String,
    raw: Vec<(sinex_model::KindRef, String)>,
    indexed: Vec<(sinex_model::KindRef, String)>,
    classes: Vec<String>,
}
fn contract(
    grants: &fetch::ProducerGrants,
    producer: &str,
    subjects: &Subjects,
) -> Result<Contract, String> {
    let grant = grants.get(producer).ok_or("coverage_read_grants_missing")?;
    let artifact = grant
        .indexed_catalog
        .as_ref()
        .ok_or("projection_catalog_not_installed")?;
    let (_, scope) = grant.consumer_scope(producer, subjects);
    let write = grant.write.as_ref();
    let mut raw = Vec::new();
    if let Some(write) = write.filter(|write| write.family == "polylogue") {
        for name in RAW {
            let name = format!("polylogue.{name}");
            raw.extend(
                write
                    .source_declarations
                    .iter()
                    .flat_map(|source| &source.kinds)
                    .filter(|kind| kind.kind.name == name)
                    .map(|kind| (kind.kind.clone(), kind.definition_hash())),
            );
        }
    }
    raw.sort();
    raw.dedup();
    let mut indexed = artifact
        .kinds
        .iter()
        .filter(|kind| authority::matches(&grant.kind_prefixes, &kind.kind.name))
        .map(|kind| (kind.kind.clone(), kind.definition_hash()))
        .collect::<Vec<_>>();
    indexed.sort();
    indexed.dedup();
    let mut sources = write
        .into_iter()
        .flat_map(|write| &write.source_declarations)
        .map(|source| (source.source.clone(), source.fingerprint()))
        .collect::<Vec<_>>();
    sources.sort();
    sources.dedup();
    let catalog = sinex_model::json_identity::hash(
        &serde_json::json!({"contract":"sinex.indexed_projection.v1","indexed":artifact.fingerprint(),"raw":raw,"sources":sources}),
    );
    let mut classes = grant
        .classes
        .iter()
        .map(|class| {
            serde_json::to_value(class)
                .expect("classes serialize")
                .as_str()
                .expect("class string")
                .to_owned()
        })
        .collect::<Vec<_>>();
    classes.sort();
    classes.dedup();
    Ok(Contract {
        family: write.map(|write| write.family.clone()).unwrap_or_default(),
        scope,
        catalog,
        raw,
        indexed,
        classes,
    })
}
async fn authorize_raw(
    conn: &mut PgConnection,
    claims: &Claims,
    grant: &fetch::Grants,
) -> Result<(), Failure> {
    if RAW.iter().any(|name| {
        !claims
            .kinds
            .iter()
            .any(|(kind, _)| kind.name == format!("polylogue.{name}"))
    }) {
        return Err(Failure::Invalidated(
            "coverage_required_raw_catalog_unknown".into(),
        ));
    }
    if claims
        .kinds
        .iter()
        .any(|kind| !claims.indexed_kinds.contains(kind))
    {
        return Err(Failure::Invalidated(
            "coverage_raw_inputs_not_in_completed_projection".into(),
        ));
    }
    read_coverage::current(conn, claims)
        .await
        .map_err(Failure::from)?;
    read_coverage::input_contract(conn, claims)
        .await
        .map_err(Failure::from)?;
    for (kind, class) in read_coverage::required_classes(conn, claims)
        .await
        .map_err(|error| error.to_string())?
    {
        let class: sinex_model::Class =
            serde_json::from_value(Value::String(class)).map_err(|error| error.to_string())?;
        if !grant.allows(&kind, class) || class == sinex_model::Class::Sealed {
            return Err(Failure::Invalidated(
                "coverage_required_class_unavailable".into(),
            ));
        }
    }
    Ok(())
}
async fn instance(client: &async_nats::Client, subjects: &Subjects) -> Result<String, String> {
    let js = crate::publisher::context(client.clone());
    crate::publisher::ensure_stream(&js, subjects).await?;
    let stream = js
        .get_stream(subjects.confirmed_stream())
        .await
        .map_err(|error| error.to_string())?;
    Ok(crate::publisher::stream_instance(&stream))
}
fn same(
    claims: &Claims,
    producer: &str,
    cache: &str,
    family: &str,
    scope: &str,
    catalog: &str,
    instance: &str,
    settings: &str,
    host: &str,
) -> bool {
    claims.producer == producer
        && claims.cache == cache
        && claims.family == family
        && claims.scope == scope
        && claims.catalog == catalog
        && claims.instance == instance
        && claims.settings_identity == settings
        && claims.host == host
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    claims: Claims,
    after: Option<(i64, uuid::Uuid)>,
    chain: String,
}

impl Requests {
    pub(super) async fn replay_inputs(
        &self,
        producer: &str,
        cache: &str,
        cursor: Option<&str>,
    ) -> InputReplayReply {
        let mut current_epoch = None;
        let current_scope = self
            .read_grants
            .get(producer)
            .map(|grant| grant.consumer_scope(producer, &self.subjects).1);
        let result: Result<_, Failure> = async {
            if cache.is_empty() {
                return Err(Failure::Invalidated(
                    "coverage_cache_identity_missing".into(),
                ));
            }
            let Contract {
                family,
                scope,
                catalog,
                raw: kinds,
                indexed: indexed_kinds,
                classes,
            } = contract(&self.read_grants, producer, &self.subjects)
                .map_err(Failure::Invalidated)?;
            let grant = self
                .read_grants
                .get(producer)
                .ok_or_else(|| Failure::Invalidated("coverage_read_grants_missing".into()))?;
            if !grant.public_context {
                return Err(Failure::Invalidated("public_context_not_authorized".into()));
            }
            let native = instance(&self.client, &self.subjects).await?;
            let mut conn = self
                .reads
                .acquire()
                .await
                .map_err(|error| error.to_string())?;
            use sqlx::Connection;
            let mut tx = conn.begin().await.map_err(|error| error.to_string())?;
            sinex_store::query::read_one_snapshot(&mut tx)
                .await
                .map_err(|error| error.to_string())?;
            let policy = sinex_store::privacy::projection::epoch(&mut tx)
                .await
                .map_err(|error| error.to_string())?;
            current_epoch = policy.ready().then_some(policy.active);
            let context = self
                .public_context
                .snapshot(
                    &mut tx,
                    producer,
                    &sinex_protocol::PublicContextRequest::default(),
                    Timestamp::now(),
                )
                .await?;
            let sinex_protocol::PublicContext::Available {
                settings_identity,
                owner_revision,
                ..
            } = context
            else {
                return Err("coverage_owner_policy_unavailable".into());
            };
            let key = sinex_store::owner_protocol::key(&mut tx)
                .await
                .map_err(|error| error.to_string())?;
            let mut cursor = if let Some(cursor) = cursor {
                let cursor: Cursor = sinex_store::owner_protocol::open(&key, CURSOR_DOMAIN, cursor)
                    .map_err(|_| Failure::Invalidated("coverage_replay_cursor_invalid".into()))?;
                if !same(
                    &cursor.claims,
                    producer,
                    cache,
                    &family,
                    &scope,
                    &catalog,
                    &native,
                    &settings_identity,
                    &self.public_context.host,
                ) || cursor.claims.purpose != "projection"
                    || owner_revision < cursor.claims.owner_revision
                {
                    return Err(Failure::Invalidated(
                        "coverage_replay_context_changed".into(),
                    ));
                }
                cursor
            } else {
                if !policy.ready() {
                    return Err(Failure::Invalidated(
                        "coverage_policy_projection_pending".into(),
                    ));
                }
                Cursor {
                    claims: Claims {
                        purpose: "projection".into(),
                        producer: producer.into(),
                        family,
                        cache: cache.into(),
                        scope,
                        catalog,
                        instance: native,
                        host: self.public_context.host.clone(),
                        settings_identity,
                        owner_revision,
                        position: sinex_store::query::current_seq(&mut tx)
                            .await
                            .map_err(|error| error.to_string())?,
                        policy_epoch: policy.active,
                        kinds,
                        indexed_kinds,
                        classes,
                        replay_root: None,
                    },
                    after: None,
                    chain: String::new(),
                }
            };
            read_coverage::current(&mut tx, &cursor.claims).await?;
            read_coverage::projection_contract(&mut tx, &cursor.claims).await?;
            let mut rows = read_coverage::inputs(&mut tx, &cursor.claims, cursor.after, PAGE + 1)
                .await
                .map_err(|error| error.to_string())?;
            let more = rows.len() > PAGE as usize;
            if more {
                rows.pop();
            }
            let mut inputs = Vec::with_capacity(rows.len());
            for row in rows {
                if row.history_unknown || row.archived && row.lifecycle_position.is_none() {
                    return Err(Failure::Invalidated(
                        "coverage_archived_position_unknown".into(),
                    ));
                }
                cursor.after = Some((row.commit_seq, row.id));
                let kind = sinex_model::KindRef {
                    name: row.event_type,
                    version: u64::try_from(row.kind_version).map_err(|error| error.to_string())?,
                };
                let kind_definition_hash = cursor
                    .claims
                    .indexed_kinds
                    .iter()
                    .find(|(known, _)| known == &kind)
                    .map(|(_, hash)| hash.clone())
                    .ok_or_else(|| {
                        Failure::Invalidated("projection_indexed_kind_version_unknown".into())
                    })?;
                let source = row
                    .source_version
                    .map(|version| {
                        u32::try_from(version).map(|version| sinex_model::SourceRef {
                            name: row.source.clone(),
                            version,
                        })
                    })
                    .transpose()
                    .map_err(|_| {
                        Failure::Invalidated("projection_native_source_version_unknown".into())
                    })?;
                if required_kind(&kind.name) {
                    let source = source.as_ref().ok_or_else(|| {
                        Failure::Invalidated("coverage_native_source_unknown".into())
                    })?;
                    let declaration = sinex_store::source_declarations::load(&mut tx, source)
                        .await
                        .map_err(|error| error.to_string())?
                        .ok_or_else(|| {
                            Failure::Invalidated("coverage_native_source_unknown".into())
                        })?;
                    if row.source_fingerprint.as_deref() != Some(declaration.fingerprint().as_str())
                        || row.producer_family.as_deref() != Some(declaration.family.as_str())
                        || !declaration.allows(&kind, &kind_definition_hash)
                    {
                        return Err(Failure::Invalidated(
                            "coverage_native_source_schema_changed".into(),
                        ));
                    }
                    if row.changed {
                        return Err(Failure::Invalidated(
                            "coverage_required_projection_affected".into(),
                        ));
                    }
                }
                inputs.push(InputReference {
                    event: EventId(row.id),
                    commit_seq: row.commit_seq,
                    archived: row.archived,
                    lifecycle_position: row.lifecycle_position,
                    projection: sinex_protocol::ProjectionIdentity::for_payload(
                        cursor.claims.policy_epoch,
                        &row.payload,
                    ),
                    kind,
                    kind_definition_hash,
                    source,
                    source_fingerprint: row.source_fingerprint,
                });
            }
            cursor.chain =
                sinex_model::json_identity::hash(&serde_json::json!([cursor.chain, &inputs]));
            if !more {
                cursor.claims.replay_root = Some(cursor.chain.clone());
            }
            let continuation = if more {
                ReplayContinuation::Next {
                    cursor: sinex_store::owner_protocol::sign(&key, CURSOR_DOMAIN, &cursor)
                        .map_err(|error| error.to_string())?,
                }
            } else {
                let projection = sinex_protocol::FullProjectionComplete {
                    stamp: read_coverage::sign(&mut tx, &cursor.claims)
                        .await
                        .map_err(|error| error.to_string())?,
                    position: cursor.claims.position,
                    scope: cursor.claims.scope.clone(),
                    catalog: cursor.claims.catalog.clone(),
                };
                let raw_inputs = match authorize_raw(&mut tx, &cursor.claims, grant).await {
                    Ok(()) => {
                        let mut raw = cursor.claims.clone();
                        raw.purpose = "basis".into();
                        CoverageStatus::Complete {
                            proof: CoverageProof {
                                stamp: read_coverage::sign(&mut tx, &raw)
                                    .await
                                    .map_err(|error| error.to_string())?,
                                position: raw.position,
                                scope: raw.scope,
                            },
                        }
                    }
                    Err(reason) => CoverageStatus::Incomplete {
                        reason: reason.to_string(),
                    },
                };
                ReplayContinuation::Complete {
                    projection,
                    raw_inputs,
                }
            };
            Ok(InputReplayReply::Page {
                position: cursor.claims.position,
                policy_epoch: cursor.claims.policy_epoch,
                scope: cursor.claims.scope,
                inputs,
                continuation,
            })
        }
        .await;
        result.unwrap_or_else(|failure| {
            let reason = failure.to_string();
            let basis = match failure {
                Failure::Invalidated(_) => ReplayBasisStatus::Invalidated {
                    policy_epoch: current_epoch,
                    scope: current_scope,
                },
                Failure::Unverified(_) => ReplayBasisStatus::Unverified,
            };
            InputReplayReply::Incomplete { reason, basis }
        })
    }

    pub(super) async fn coverage(
        &self,
        conn: &mut PgConnection,
        producer: &str,
        request: &CoverageRequest,
        context: &sinex_protocol::PublicContext,
    ) -> CoverageStatus {
        let result: Result<_, String> = async {
            let sinex_protocol::PublicContext::Available {
                settings_identity,
                owner_revision,
                ..
            } = context
            else {
                return Err("coverage_owner_policy_unavailable".into());
            };
            let basis = request
                .basis
                .as_ref()
                .ok_or("coverage_completed_replay_basis_required")?;
            let mut claims = read_coverage::open(conn, &basis.stamp)
                .await
                .map_err(|error| error.to_string())?;
            let Contract {
                family,
                scope,
                catalog,
                ..
            } = contract(&self.read_grants, producer, &self.subjects)?;
            let native = instance(&self.client, &self.subjects).await?;
            if claims.replay_root.is_none()
                || claims.purpose != "basis"
                || basis.position != claims.position
                || basis.scope != claims.scope
                || request.completed_position < claims.position
                || !same(
                    &claims,
                    producer,
                    &request.cache,
                    &family,
                    &scope,
                    &catalog,
                    &native,
                    settings_identity,
                    &self.public_context.host,
                )
                || *owner_revision < claims.owner_revision
            {
                return Err("coverage_replay_basis_changed".into());
            }
            if request.completed_position > claims.position {
                let completion = request
                    .completion
                    .as_ref()
                    .ok_or("coverage_native_completion_receipt_required")?;
                delivery::verify(
                    &self.client,
                    &self.subjects,
                    self.read_grants
                        .get(producer)
                        .ok_or("coverage_read_grants_missing")?,
                    &claims,
                    completion,
                    request.completed_position,
                )
                .await?;
            }
            claims.position = request.completed_position;
            claims.purpose = "write".into();
            claims.owner_revision = *owner_revision;
            authorize_raw(
                conn,
                &claims,
                self.read_grants
                    .get(producer)
                    .ok_or("coverage_read_grants_missing")?,
            )
            .await?;
            let stamp = read_coverage::sign(conn, &claims)
                .await
                .map_err(|error| error.to_string())?;
            Ok(CoverageProof {
                stamp,
                position: claims.position,
                scope: claims.scope,
            })
        }
        .await;
        match result {
            Ok(proof) => CoverageStatus::Complete { proof },
            Err(reason) => CoverageStatus::Incomplete { reason },
        }
    }
}

impl Ingest {
    pub(super) async fn coverage_claim(
        &self,
        producer: &str,
        frame: Option<&Ingress>,
    ) -> Result<Option<sinex_model::OwnerCoverage>, Check> {
        let optional = matches!(frame,Some(Ingress::Signals{signals,emergency,..}) if signals.is_empty()&&!emergency.is_empty());
        let (coverage, required) = match frame {
            Some(Ingress::Events {
                events,
                coverage,
                source,
                ..
            }) => {
                let required = events
                    .iter()
                    .any(|event| event.kind.name == "polylogue.work_snapshot");
                if let Some(proof) = coverage
                    && required
                    && (source.is_some()
                        || events
                            .iter()
                            .filter(|event| event.kind.name == "polylogue.work_snapshot")
                            .any(|event| event.reading_order != Some(proof.position)))
                {
                    return Err(Check::Configuration("canonical snapshot order must equal its owner-certified completed position".into()));
                }
                (coverage, required)
            }
            Some(Ingress::Signals {
                coverage, signals, ..
            }) => (coverage, !signals.is_empty()),
            _ => return Ok(None),
        };
        let Some(proof) = coverage else {
            return if required {
                Err(Check::Configuration(
                    "canonical complete-set write requires owner read coverage".into(),
                ))
            } else {
                Ok(None)
            };
        };
        let claim: Result<_, Check> = async {
            let provider = self
                .public_context
                .as_ref()
                .ok_or_else(|| Check::Configuration("owner public policy is unavailable".into()))?;
            let Contract { scope, catalog, .. } =
                contract(&self.grants, producer, &self.subjects).map_err(Check::Configuration)?;
            if proof.scope != scope {
                return Err(Check::Configuration(
                    "coverage scope differs from current credential grants".into(),
                ));
            }
            let native = instance(
                self.transport_client
                    .as_ref()
                    .ok_or_else(|| Check::Unavailable("native transport is unavailable".into()))?,
                &self.subjects,
            )
            .await
            .map_err(Check::Unavailable)?;
            Ok(sinex_model::OwnerCoverage {
                required: !optional,
                stamp: proof.stamp.clone(),
                position: proof.position,
                scope,
                catalog,
                instance: native,
                host: provider.host.clone(),
                settings_identity: provider
                    .settings_identity
                    .clone()
                    .ok_or_else(|| Check::Configuration("owner settings are unavailable".into()))?,
            })
        }
        .await;
        match claim {
            Ok(claim) => Ok(Some(claim)),
            Err(_) if optional => Ok(None),
            Err(error) => Err(error),
        }
    }
}

impl Requests {
    pub(super) async fn verify_replay_basis(
        &self,
        producer: &str,
        cache: &str,
        after: i64,
        proof: &sinex_protocol::FullProjectionComplete,
    ) -> Result<(), String> {
        let mut conn = self
            .reads
            .acquire()
            .await
            .map_err(|error| error.to_string())?;
        let claims = read_coverage::open(&mut conn, &proof.stamp)
            .await
            .map_err(|error| error.to_string())?;
        let Contract {
            family,
            scope,
            catalog,
            ..
        } = contract(&self.read_grants, producer, &self.subjects)?;
        let native = instance(&self.client, &self.subjects).await?;
        let settings = self
            .public_context
            .settings_identity
            .as_ref()
            .ok_or("coverage_owner_settings_unavailable")?;
        if claims.purpose != "projection"
            || claims.replay_root.is_none()
            || proof.position != claims.position
            || proof.scope != claims.scope
            || proof.catalog != claims.catalog
            || after < claims.position
            || !same(
                &claims,
                producer,
                cache,
                &family,
                &scope,
                &catalog,
                &native,
                settings,
                &self.public_context.host,
            )
        {
            return Err("coverage_replay_basis_changed".into());
        }
        // A native resume after the certificate may include later ordinary
        // lifecycle changes. Its retained replay basis is still valid.
        let mut current = claims;
        current.position = after;
        read_coverage::current(&mut conn, &current)
            .await
            .map_err(|error| error.to_string())?;
        read_coverage::projection_contract(&mut conn, &current)
            .await
            .map_err(|error| error.to_string())
    }
}

impl Requests {
    pub(super) async fn public_context_snapshot(
        &self,
        producer: &str,
        request: &sinex_protocol::PublicContextRequest,
    ) -> sinex_protocol::PublicContext {
        let result: Result<_, String> = async {
            use sqlx::Connection;
            let mut conn = self
                .reads
                .acquire()
                .await
                .map_err(|error| error.to_string())?;
            let mut tx = conn.begin().await.map_err(|error| error.to_string())?;
            sinex_store::query::read_one_snapshot(&mut tx)
                .await
                .map_err(|error| error.to_string())?;
            let mut context = self
                .public_context
                .snapshot(&mut tx, producer, request, Timestamp::now())
                .await?;
            if let Some(request) = &request.coverage {
                let status = self.coverage(&mut tx, producer, request, &context).await;
                if let sinex_protocol::PublicContext::Available { coverage, .. } = &mut context {
                    *coverage = status;
                }
            }
            self.public_context.grant_offline_emergency(
                &mut context,
                producer,
                self.read_grants
                    .get(producer)
                    .and_then(|grant| grant.write.as_ref()),
                Timestamp::now(),
            )?;
            self.public_context
                .sign_snapshot(&mut tx, producer, &mut context)
                .await?;
            tx.commit().await.map_err(|error| error.to_string())?;
            Ok(context)
        }
        .await;
        result.unwrap_or_else(|reason| sinex_protocol::PublicContext::Unavailable { reason })
    }
}
