//! Producer reads use operator grants attached to the authenticated NATS identity.
use super::*;
use serde::{Deserialize, de::DeserializeOwned};
use sinex_model::Class;
use sinex_protocol::{
    ConfirmedEvent, FetchEvent, FetchEventReply, FetchMaterial, FetchMaterialReply,
    FetchRefusalKind,
};
use sqlx::Connection;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Grants {
    #[serde(default)]
    pub indexed_catalog: Option<sinex_model::ProjectionCatalog>,
    #[serde(default)]
    pub public_context: bool,
    #[serde(default)]
    pub write: Option<super::authority::WriteGrant>,
    pub kind_prefixes: Vec<String>,
    pub classes: Vec<Class>,
}
pub(crate) type ProducerGrants = BTreeMap<String, Grants>;
impl Grants {
    pub(crate) fn allows(&self, kind: &str, class: Class) -> bool {
        self.kind(kind) && self.class(class)
    }

    pub(crate) fn consumer_scope(
        &self,
        producer: &str,
        subjects: &Subjects,
    ) -> (Vec<String>, String) {
        let mut prefixes = self
            .kind_prefixes
            .iter()
            .map(|prefix| {
                prefix
                    .trim_end_matches('*')
                    .trim_end_matches('.')
                    .to_owned()
            })
            .collect::<Vec<_>>();
        prefixes.sort();
        prefixes.dedup();
        let all = prefixes.clone();
        prefixes.retain(|prefix| {
            !all.iter()
                .any(|other| other != prefix && prefix.starts_with(&format!("{other}.")))
        });
        let mut filters = vec![subjects.confirmed_control()];
        for class in &self.classes {
            let class = serde_json::to_value(class).expect("class serializes");
            let class = class.as_str().expect("class string");
            for prefix in &prefixes {
                for subject in [
                    subjects.confirmed_event(class, prefix),
                    subjects.confirmed_lifecycle(class, prefix),
                ] {
                    filters.push(format!("{subject}.>"));
                    filters.push(subject);
                }
            }
        }
        filters.sort();
        filters.dedup();
        let scope =
            blake3::hash(&serde_json::to_vec(&(producer, &filters)).expect("scope serializes"))
                .to_hex()
                .to_string();
        (filters, scope)
    }
    fn kind(&self, name: &str) -> bool {
        self.kind_prefixes.iter().any(|prefix| {
            let prefix = prefix.trim_end_matches('*').trim_end_matches('.');
            name == prefix
                || name
                    .strip_prefix(prefix)
                    .is_some_and(|tail| tail.starts_with('.'))
        })
    }
    fn class(&self, class: Class) -> bool {
        self.classes.contains(&class)
    }
    fn intersects(&self, other: &Self) -> bool {
        self.kind_prefixes
            .iter()
            .any(|left| other.kind(left.trim_end_matches('*').trim_end_matches('.')))
            || other
                .kind_prefixes
                .iter()
                .any(|right| self.kind(right.trim_end_matches('*').trim_end_matches('.')))
    }
}
pub(crate) fn load(path: Option<&std::path::Path>) -> Result<ProducerGrants, String> {
    let Some(path) = path else {
        return Ok(BTreeMap::new());
    };
    let bytes = std::fs::read(path).map_err(|error| format!("{}: {error}", path.display()))?;
    let grants: ProducerGrants =
        serde_json::from_slice(&bytes).map_err(|error| format!("{}: {error}", path.display()))?;
    for (producer, grant) in &grants {
        if !sinex_protocol::nats::valid_producer(producer) {
            return Err("invalid producer in read grants".into());
        }
        for prefix in &grant.kind_prefixes {
            let prefix = prefix.trim_end_matches('*').trim_end_matches('.');
            if prefix.is_empty()
                || prefix.split('.').any(|part| {
                    part.is_empty()
                        || !part.bytes().all(|byte| {
                            byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-'
                        })
                })
            {
                return Err(format!("invalid kind prefix in grants for {producer}"));
            }
        }
    }
    super::authority::validate(&grants)?;
    Ok(grants)
}
pub(super) struct Refusal {
    kind: FetchRefusalKind,
    reason: String,
}
type ReadResult<T> = Result<T, Refusal>;
fn denied() -> Refusal {
    Refusal {
        kind: FetchRefusalKind::Denied,
        reason: "producer grants do not allow this read".into(),
    }
}
fn missing() -> Refusal {
    Refusal {
        kind: FetchRefusalKind::Missing,
        reason: "requested retained data is unavailable".into(),
    }
}
fn unavailable(error: impl std::fmt::Display) -> Refusal {
    Refusal {
        kind: FetchRefusalKind::Unavailable,
        reason: error.to_string(),
    }
}
fn field<T: DeserializeOwned>(value: &Value, name: &str) -> ReadResult<T> {
    serde_json::from_value(value[name].clone()).map_err(unavailable)
}
impl Requests {
    pub(super) fn grants(&self, producer: &str) -> ReadResult<&Grants> {
        self.read_grants
            .get(producer)
            .filter(|grant| !grant.kind_prefixes.is_empty() && !grant.classes.is_empty())
            .ok_or_else(denied)
    }
    async fn event_for(&self, producer: &str, id: EventId) -> ReadResult<FetchEventReply> {
        let grant = self.grants(producer)?;
        let mut connection = self.reads.acquire().await.map_err(unavailable)?;
        let mut conn = connection.begin().await.map_err(unavailable)?;
        sinex_store::query::read_one_snapshot(&mut conn)
            .await
            .map_err(unavailable)?;
        let epoch = sinex_store::privacy::projection::epoch(&mut conn)
            .await
            .map_err(unavailable)?;
        if !epoch.ready() {
            return Err(unavailable("current privacy projection is not ready"));
        }
        let tombstone=sqlx::query!("SELECT event_type,class,commit_seq,original_commit_seq FROM core.event_tombstones WHERE event_id=$1",id.0).fetch_optional(&mut *conn).await.map_err(unavailable)?;
        if let Some(erased) = tombstone {
            if !grant.kind(&erased.event_type) {
                return Err(denied());
            }
            let class: Class =
                serde_json::from_value(Value::String(erased.class.ok_or_else(|| {
                    unavailable(
                        "erased event has unknown historical class; confirmed history has a gap",
                    )
                })?))
                .map_err(unavailable)?;
            if !grant.class(class) {
                return Err(denied());
            }
            let (Some(commit_seq), Some(original_commit_seq)) =
                (erased.commit_seq, erased.original_commit_seq)
            else {
                return Err(unavailable(
                    "erased event has unknown historical positions; confirmed history has a gap",
                ));
            };
            return Ok(FetchEventReply {
                erased: Some(sinex_protocol::ErasedEvent {
                    event: id,
                    commit_seq,
                    original_commit_seq,
                }),
                ..Default::default()
            });
        }
        let classes = grant
            .classes
            .iter()
            .map(|class| {
                serde_json::to_value(class)
                    .expect("classes serialize")
                    .as_str()
                    .expect("class string")
                    .to_owned()
            })
            .collect::<Vec<_>>()
            .join(",");
        sinex_store::query::set_grants(&mut conn, &classes)
            .await
            .map_err(unavailable)?;
        let output = sinex_store::producer_reads::event_output(&mut conn, id.0)
            .await
            .map_err(unavailable)?
            .ok_or_else(missing)?;
        if output.epoch != Some(epoch.active) || output.state.as_deref() != Some("complete") {
            return Err(unavailable("event privacy projection is incomplete"));
        }
        if super::coverage::required_kind(&output.kind) && output.changed != Some(false) {
            return Err(unavailable(
                "required canonical input is affected by current privacy rules",
            ));
        }
        let output_class: Class = serde_json::from_value(Value::String(
            output
                .class
                .ok_or_else(|| unavailable("event privacy class is unavailable"))?,
        ))
        .map_err(unavailable)?;
        if !grant.kind(&output.kind) || !grant.class(output_class) {
            return Err(denied());
        }
        if output_class == Class::Sealed {
            return Err(Refusal {
                kind: FetchRefusalKind::Locked,
                reason: "sealed producer reads require an operator unlock".into(),
            });
        }
        let view = sinex_store::query::show(&mut conn, &sinex_model::query::Reference::Event(id.0))
            .await
            .map_err(|error| match error {
                sinex_store::Error::Invalid(_) => missing(),
                other => unavailable(other),
            })?;
        let event_type: String = field(&view, "event_type")?;
        let class: Class = field(&view, "class")?;
        if !grant.kind(&event_type) || !grant.class(class) {
            return Err(denied());
        }
        if class == Class::Sealed {
            return Err(Refusal {
                kind: FetchRefusalKind::Locked,
                reason: "sealed producer reads require an operator unlock".into(),
            });
        }
        let location = if view["material"].is_null() {
            None
        } else {
            Some(sinex_model::RecordLocation {
                material: field(&view, "material")?,
                anchor: field(&view, "offset")?,
            })
        };
        let (source_name, source_version, source_fingerprint, producer_family, producer_identity) =
            sinex_store::producer_reads::event_source(&mut conn, id.0)
                .await
                .map_err(unavailable)?
                .ok_or_else(missing)?;
        let source_ref = source_version
            .map(|version| {
                u32::try_from(version).map(|version| sinex_model::SourceRef {
                    name: source_name,
                    version,
                })
            })
            .transpose()
            .map_err(unavailable)?;
        let kind_version: u64 = field(&view, "kind_version")?;
        let reference = sinex_model::KindRef {
            name: event_type.clone(),
            version: kind_version,
        };
        let declarations = sinex_schema::kinds::load(&mut conn, std::slice::from_ref(&reference))
            .await
            .map_err(unavailable)?;
        let kind_definition_hash = declarations
            .get(&reference)
            .ok_or_else(|| unavailable("event kind definition is unavailable"))?
            .definition_hash();
        Ok(FetchEventReply {
            event: Some(ConfirmedEvent {
                source_ref,
                source_fingerprint,
                producer_family,
                producer_identity,
                kind_version,
                kind_definition_hash,
                validation: field(&view, "validation")?,
                commit_seq: field(&view, "commit_seq")?,
                id: field(&view, "id")?,
                event_type,
                source: field(&view, "source")?,
                run: field(&view, "run")?,
                location,
                output_index: field(&view, "output")?,
                parents: field::<Option<Vec<EventId>>>(&view, "parents")?.unwrap_or_default(),
                ts_orig: field(&view, "when")?,
                ts_quality: field(&view, "time_quality")?,
                ts_coided: field(&view, "seen")?,
                ts_persisted: field(&view, "written")?,
                record_state: field(&view, "record_state")?,
                class,
                equivalence_key: field(&view, "key")?,
                reading_order: field(&view, "reading_order")?,
                record_hash: field(&view, "record_hash")?,
                projection: sinex_protocol::ProjectionIdentity::for_payload(
                    epoch.active,
                    &view["payload"],
                ),
                payload: view["payload"].clone(),
            }),
            ..Default::default()
        })
    }
    pub(super) async fn fetch_event(
        &self,
        producer: &str,
        request: &FetchEvent,
    ) -> FetchEventReply {
        match self.event_for(producer, request.event).await {
            Ok(reply) => reply,
            Err(error) => FetchEventReply {
                refusal: Some(error.reason),
                refusal_kind: Some(error.kind),
                ..Default::default()
            },
        }
    }
    pub(super) async fn fetch_material(
        &self,
        producer: &str,
        request: &FetchMaterial,
    ) -> FetchMaterialReply {
        let result: ReadResult<_> = async {
            let grant = self.grants(producer)?;
            let mut connection = self.reads.acquire().await.map_err(unavailable)?;
            let mut conn = connection.begin().await.map_err(unavailable)?;
            sinex_store::query::read_one_snapshot(&mut conn)
                .await
                .map_err(unavailable)?;
            let ownership =
                sinex_store::producer_reads::material_family(&mut conn, request.material.0)
                    .await
                    .map_err(unavailable)?
                    .ok_or_else(missing)?;
            let (Some(_family), Some(prefixes)) = ownership else {
                return Err(unavailable(
                    "historical material family ownership is unknown",
                ));
            };
            let owner_scope = Grants {
                indexed_catalog: None,
                public_context: false,
                write: None,
                kind_prefixes: prefixes,
                classes: vec![],
            };
            if !grant.intersects(&owner_scope) {
                return Err(denied());
            }
            let classes = grant
                .classes
                .iter()
                .map(|class| {
                    serde_json::to_value(class)
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .to_owned()
                })
                .collect::<Vec<_>>()
                .join(",");
            sinex_store::query::set_grants(&mut conn, &classes)
                .await
                .map_err(unavailable)?;
            let epoch = sinex_store::privacy::projection::epoch(&mut conn)
                .await
                .map_err(unavailable)?;
            if !epoch.ready() {
                return Err(unavailable(
                    "current privacy projection is not ready for raw material access",
                ));
            }
            let events =
                sinex_store::producer_reads::material_classes(&mut conn, request.material.0)
                    .await
                    .map_err(unavailable)?;
            if events.is_empty() && !(grant.class(Class::Open) && grant.class(Class::Guarded)) {
                return Err(denied());
            }
            for event in events {
                if event.epoch != Some(epoch.active) || event.state.as_deref() != Some("complete") {
                    return Err(unavailable(
                        "material privacy output is incomplete at the current policy epoch",
                    ));
                }
                if event.changed != Some(false) {
                    return Err(Refusal {
                        kind: FetchRefusalKind::Denied,
                        reason: "current privacy rules withhold raw material bytes".into(),
                    });
                }
                let class: Class = serde_json::from_value(Value::String(
                    event
                        .class
                        .ok_or_else(|| unavailable("material privacy class is unavailable"))?,
                ))
                .map_err(unavailable)?;
                if !grant.kind(&event.kind) || !grant.class(class) {
                    return Err(denied());
                }
                if class == Class::Sealed {
                    return Err(Refusal {
                        kind: FetchRefusalKind::Locked,
                        reason: "sealed material requires an operator unlock".into(),
                    });
                }
            }
            sinex_store::material_bytes::authorize_raw(&mut conn, request.material)
                .await
                .map_err(|error| Refusal {
                    kind: FetchRefusalKind::Denied,
                    reason: error.to_string(),
                })?;
            let material = sinex_store::material_bytes::reference(&mut conn, request.material)
                .await
                .map_err(unavailable)?
                .ok_or_else(missing)?;
            let sources =
                sinex_store::producer_reads::material_sources(&mut conn, &[request.material.0])
                    .await
                    .map_err(unavailable)?;
            let (_, name, version) = sources.into_iter().next().ok_or_else(|| {
                unavailable(
                    "material source authority is unknown; explicit historical mapping required",
                )
            })?;
            let source = sinex_model::SourceRef {
                name: name.ok_or_else(|| unavailable("material source authority is unknown"))?,
                version: u32::try_from(
                    version.ok_or_else(|| unavailable("material source version is unknown"))?,
                )
                .map_err(unavailable)?,
            };
            let input = sinex_store::producer_reads::material_input(&mut conn, request.material.0)
                .await
                .map_err(unavailable)?
                .ok_or_else(missing)?;
            let descriptor = sinex_store::producer_reads::extent(&mut conn, &material.blob)
                .await
                .map_err(unavailable)?
                .ok_or_else(missing)?;
            if self.blobs.is_sealed(&material.blob).map_err(unavailable)? {
                return Err(Refusal {
                    kind: FetchRefusalKind::Locked,
                    reason: "sealed material requires an operator unlock".into(),
                });
            }
            Ok(FetchMaterialReply {
                material: Some(MaterialFrame {
                    source,
                    id: material.id,
                    input: input,
                    blob: material.blob,
                    length: material.length,
                    parser: material.parser,
                    parser_version: material.parser_version,
                    part_index: material.part_index,
                    previous_part: material.previous_part,
                    manifest: material.manifest,
                }),
                extents: vec![descriptor],
                ..Default::default()
            })
        }
        .await;
        result.unwrap_or_else(|error| FetchMaterialReply {
            refusal: Some(error.reason),
            refusal_kind: Some(error.kind),
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod tests;
