//! Fixed WorkSnapshot admitted-input contract and stateless replay certificates.
use super::*;
use serde::{Deserialize, Serialize};
use sinex_protocol::{
    CoverageProof, CoverageRequest, CoverageStatus, InputReference, InputReplayReply,
    ReplayContinuation,
};
use sinex_store::read_coverage::{self, Claims};
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

fn contract(
    grants: &fetch::ProducerGrants,
    producer: &str,
    subjects: &Subjects,
) -> Result<(String, String, String, Vec<(sinex_model::KindRef, String)>), String> {
    let write = authority::write(grants, producer)?;
    if write.family != "polylogue" {
        return Err("coverage_contract_not_installed".into());
    }
    let grant = grants.get(producer).ok_or("coverage_read_grants_missing")?;
    let (_, scope) = grant.consumer_scope(producer, subjects);
    let mut kinds = Vec::new();
    for name in RAW {
        let name = format!("polylogue.{name}");
        let mut found = write
            .source_declarations
            .iter()
            .flat_map(|source| &source.kinds)
            .filter(|kind| kind.kind.name == name)
            .map(|kind| (kind.kind.clone(), kind.definition_hash()))
            .collect::<Vec<_>>();
        found.sort();
        found.dedup();
        if found.is_empty()
            || !grant
                .kind_prefixes
                .iter()
                .any(|prefix| authority::matches(std::slice::from_ref(prefix), &name))
        {
            return Err("coverage_required_kind_or_scope_unknown".into());
        }
        kinds.extend(found);
    }
    kinds.sort();
    let mut sources = write
        .source_declarations
        .iter()
        .map(|source| (source.source.clone(), source.fingerprint()))
        .collect::<Vec<_>>();
    sources.sort();
    sources.dedup();
    let mut canonical = write
        .canonical_kinds
        .iter()
        .map(|kind| (kind.kind.clone(), kind.definition_hash()))
        .collect::<Vec<_>>();
    canonical.sort();
    canonical.dedup();
    let catalog = sinex_model::json_identity::hash(
        &serde_json::json!({"contract":"polylogue.work_snapshot.inputs.v1","kinds":kinds,"sources":sources,"canonical":canonical}),
    );
    Ok((write.family.clone(), scope, catalog, kinds))
}
async fn authorize(
    conn: &mut PgConnection,
    claims: &Claims,
    grant: &fetch::Grants,
) -> Result<(), String> {
    read_coverage::current(conn, claims)
        .await
        .map_err(|error| error.to_string())?;
    read_coverage::input_contract(conn, claims)
        .await
        .map_err(|error| error.to_string())?;
    for (kind, class) in read_coverage::required_classes(conn, claims)
        .await
        .map_err(|error| error.to_string())?
    {
        let class: sinex_model::Class =
            serde_json::from_value(Value::String(class)).map_err(|error| error.to_string())?;
        if !grant.allows(&kind, class) || class == sinex_model::Class::Sealed {
            return Err("coverage_required_class_unavailable".into());
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
        let result: Result<_, String> = async {
            if cache.is_empty() {
                return Err("coverage_cache_identity_missing".into());
            }
            let (family, scope, catalog, kinds) =
                contract(&self.read_grants, producer, &self.subjects)?;
            let grant = self
                .read_grants
                .get(producer)
                .ok_or("coverage_read_grants_missing")?;
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
                    .map_err(|error| error.to_string())?;
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
                ) || cursor.claims.purpose != "basis"
                    || owner_revision < cursor.claims.owner_revision
                {
                    return Err("coverage_replay_context_changed".into());
                }
                cursor
            } else {
                let policy = sinex_store::privacy::projection::epoch(&mut tx)
                    .await
                    .map_err(|error| error.to_string())?;
                if !policy.ready() {
                    return Err("coverage_policy_projection_pending".into());
                }
                Cursor {
                    claims: Claims {
                        purpose: "basis".into(),
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
                        replay_root: None,
                    },
                    after: None,
                    chain: String::new(),
                }
            };
            authorize(&mut tx, &cursor.claims, grant).await?;
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
                    return Err("coverage_archived_position_unknown".into());
                }
                cursor.after = Some((row.commit_seq, row.id));
                inputs.push(InputReference {
                    event: EventId(row.id),
                    commit_seq: row.commit_seq,
                    archived: row.archived,
                    lifecycle_position: row.lifecycle_position,
                    projection: sinex_protocol::ProjectionIdentity::for_payload(
                        cursor.claims.policy_epoch,
                        &row.payload,
                    ),
                    source: sinex_model::SourceRef {
                        name: row.source,
                        version: u32::try_from(
                            row.source_version.ok_or("coverage_native_source_unknown")?,
                        )
                        .map_err(|error| error.to_string())?,
                    },
                    source_fingerprint: row
                        .source_fingerprint
                        .ok_or("coverage_native_source_unknown")?,
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
                ReplayContinuation::Complete {
                    basis: CoverageProof {
                        stamp: read_coverage::sign(&mut tx, &cursor.claims)
                            .await
                            .map_err(|error| error.to_string())?,
                        position: cursor.claims.position,
                        scope: cursor.claims.scope.clone(),
                    },
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
        result.unwrap_or_else(|reason| InputReplayReply::Incomplete { reason })
    }

    pub(super) async fn coverage(
        &self,
        conn: &mut PgConnection,
        producer: &str,
        request: &CoverageRequest,
        context: &sinex_protocol::PublicContext,
    ) -> CoverageStatus {
        let result:Result<_,String>=async{
            let sinex_protocol::PublicContext::Available{settings_identity,owner_revision,..}=context else{return Err("coverage_owner_policy_unavailable".into())};
            let basis=request.basis.as_ref().ok_or("coverage_completed_replay_basis_required")?;
            let mut claims=read_coverage::open(conn,&basis.stamp).await.map_err(|error|error.to_string())?;
            let (family,scope,catalog,_)=contract(&self.read_grants,producer,&self.subjects)?;
            let native=instance(&self.client,&self.subjects).await?;
            if claims.replay_root.is_none()||claims.purpose!="basis"||basis.position!=claims.position||basis.scope!=claims.scope||request.completed_position<claims.position
                ||!same(&claims,producer,&request.cache,&family,&scope,&catalog,&native,settings_identity,&self.public_context.host)||*owner_revision<claims.owner_revision{return Err("coverage_replay_basis_changed".into());}
            if request.completed_position>claims.position {
                let js=crate::publisher::context(self.client.clone());let stream=js.get_stream(self.subjects.confirmed_stream()).await.map_err(|error|error.to_string())?;
                let consumer=stream.get_consumer::<consumer::pull::Config>(&self.subjects.consumer_name(&scope)).await.map_err(|error|error.to_string())?;
                let info=consumer.cached_info();
                let cache=blake3::hash(request.cache.as_bytes()).to_hex().to_string();
                if info.config.metadata.get("sinex_scope")!=Some(&scope)||info.config.metadata.get("sinex_cache")!=Some(&cache){return Err("coverage_native_cache_or_scope_changed".into());}
                let message=stream.get_raw_message(info.ack_floor.stream_sequence).await.map_err(|error|error.to_string())?;
                if !matches!(serde_json::from_slice::<sinex_protocol::Confirmed>(&message.payload),Ok(sinex_protocol::Confirmed::PositionComplete{commit_seq}) if commit_seq>=request.completed_position){return Err("coverage_native_completed_position_unproven".into());}
            }
            claims.position=request.completed_position;claims.purpose="write".into();claims.owner_revision=*owner_revision;
            authorize(conn,&claims,self.read_grants.get(producer).ok_or("coverage_read_grants_missing")?).await?;
            let stamp=read_coverage::sign(conn,&claims).await.map_err(|error|error.to_string())?;
            Ok(CoverageProof{stamp,position:claims.position,scope:claims.scope})
        }.await;
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
        let provider = self
            .public_context
            .as_ref()
            .ok_or_else(|| Check::Configuration("owner public policy is unavailable".into()))?;
        let (_, scope, catalog, _) =
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
        Ok(Some(sinex_model::OwnerCoverage {
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
        }))
    }
}

impl Requests {
    pub(super) async fn verify_replay_basis(
        &self,
        producer: &str,
        cache: &str,
        after: i64,
        proof: &CoverageProof,
    ) -> Result<(), String> {
        let mut conn = self
            .reads
            .acquire()
            .await
            .map_err(|error| error.to_string())?;
        let claims = read_coverage::open(&mut conn, &proof.stamp)
            .await
            .map_err(|error| error.to_string())?;
        let (family, scope, catalog, _) = contract(&self.read_grants, producer, &self.subjects)?;
        let native = instance(&self.client, &self.subjects).await?;
        let settings = self
            .public_context
            .settings_identity
            .as_ref()
            .ok_or("coverage_owner_settings_unavailable")?;
        if claims.purpose != "basis"
            || claims.replay_root.is_none()
            || proof.position != claims.position
            || proof.scope != claims.scope
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
        authorize(
            &mut conn,
            &current,
            self.read_grants
                .get(producer)
                .ok_or("coverage_read_grants_missing")?,
        )
        .await
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
