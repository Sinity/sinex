//! Producer reads use operator grants attached to the authenticated NATS identity.
use super::*;
use crate::{Error, Result};
use serde::Deserialize;
use sinex_model::{Class, DbCheck};
use sinex_protocol::{
    FetchEvent, FetchEventReply, FetchMaterial, FetchMaterialReply, FetchRefusalKind,
};
use sqlx::Connection;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Grants {
    #[serde(default)]
    pub indexed_catalog: Option<sinex_model::SnapshotCatalog>,
    #[serde(default)]
    pub public_context: bool,
    #[serde(default)]
    pub write: Option<super::write_access::WriteGrant>,
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
pub(crate) fn load(path: Option<&std::path::Path>) -> Result<ProducerGrants> {
    let Some(path) = path else {
        return Ok(BTreeMap::new());
    };
    let bytes = std::fs::read(path).map_err(|error| Error::file(path, error))?;
    let grants: ProducerGrants = serde_json::from_slice(&bytes)
        .map_err(|error| Error::from(error).within(path.display()))?;
    for (producer, grant) in &grants {
        if !sinex_protocol::nats::valid_producer(producer) {
            return Err(Error::internal("invalid producer in read grants"));
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
                return Err(Error::internal(format!(
                    "invalid kind prefix in grants for {producer}"
                )));
            }
        }
    }
    super::write_access::validate(&grants)?;
    Ok(grants)
}
use sinex_protocol::FetchRefusal as Refusal;
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
        reason: "requested stored data is unavailable".into(),
    }
}
fn unavailable(error: impl std::fmt::Display) -> Refusal {
    Refusal {
        kind: FetchRefusalKind::Unavailable,
        reason: error.to_string(),
    }
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
        let tombstone = sinex_store::commit_log::erased_reading(&mut conn, id)
            .await
            .map_err(unavailable)?;
        if let Some(erased) = tombstone {
            if !grant.kind(&erased.event_type) {
                return Err(denied());
            }
            let class: Class = Class::from_db(&erased.class.ok_or_else(|| {
                unavailable(
                    "erased event has unknown historical class; confirmed history has a gap",
                )
            })?)
            .ok_or_else(|| unavailable("unknown erased event class"))?;
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
            return Ok(FetchEventReply::Erased(sinex_protocol::ErasedEvent {
                event: id,
                commit_seq,
                original_commit_seq,
            }));
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
        let output_class: Class = Class::from_db(&output.class)
            .ok_or_else(|| unavailable(format!("unknown event class {:?}", output.class)))?;
        if !grant.kind(&output.kind) || !grant.class(output_class) {
            return Err(denied());
        }
        if output_class == Class::Sealed {
            return Err(Refusal {
                kind: FetchRefusalKind::Locked,
                reason: "sealed producer reads require an operator unlock".into(),
            });
        }
        let row = sinex_store::confirmed::rows(&mut conn, &[id.0])
            .await
            .map_err(unavailable)?
            .pop()
            .ok_or_else(missing)?;
        let event = crate::confirmed_event::build(&mut conn, vec![row])
            .await
            .map_err(unavailable)?
            .pop()
            .ok_or_else(missing)?;
        Ok(FetchEventReply::Event(Box::new(event)))
    }
    pub(super) async fn fetch_event(
        &self,
        producer: &str,
        request: &FetchEvent,
    ) -> FetchEventReply {
        self.event_for(producer, request.event)
            .await
            .unwrap_or_else(FetchEventReply::Refused)
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
            let material_family =
                sinex_store::producer_reads::material_family(&mut conn, request.material.0)
                    .await
                    .map_err(unavailable)?
                    .ok_or_else(missing)?;
            let (Some(_family), Some(prefixes)) = material_family else {
                return Err(unavailable(
                    "the family that wrote the historical material is unknown",
                ));
            };
            let material_scope = Grants {
                indexed_catalog: None,
                public_context: false,
                write: None,
                kind_prefixes: prefixes,
                classes: vec![],
            };
            if !grant.intersects(&material_scope) {
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
            let events =
                sinex_store::producer_reads::material_classes(&mut conn, request.material.0)
                    .await
                    .map_err(unavailable)?;
            if events.is_empty() && !(grant.class(Class::Open) && grant.class(Class::Guarded)) {
                return Err(denied());
            }
            for event in events {
                let class: Class = Class::from_db(&event.class)
                    .ok_or_else(|| unavailable(format!("unknown event class {:?}", event.class)))?;
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
                unavailable("material source is unknown; explicit historical mapping required")
            })?;
            let source = sinex_model::SourceRef {
                name: name.ok_or_else(|| unavailable("material source is unknown"))?,
                version: u32::try_from(
                    version.ok_or_else(|| unavailable("material source version is unknown"))?,
                )
                .map_err(unavailable)?,
            };
            let input = sinex_store::producer_reads::material_input(&mut conn, request.material.0)
                .await
                .map_err(unavailable)?
                .ok_or_else(missing)?;
            let located = self
                .blobs
                .locate(&mut conn, &material.blob)
                .await
                .map_err(unavailable)?;
            if located.is_sealed() {
                return Err(Refusal {
                    kind: FetchRefusalKind::Locked,
                    reason: "sealed material requires an operator unlock".into(),
                });
            }
            let end = request.end.unwrap_or(material.length);
            if request.offset > end || end > material.length || request.byte_budget == 0 {
                return Err(Refusal {
                    kind: FetchRefusalKind::Invalid,
                    reason: "material range must be within its bytes and the page budget positive"
                        .into(),
                });
            }
            let length = (end - request.offset).min(request.byte_budget);
            let blobs = self.blobs.clone();
            let offset = request.offset;
            let bytes =
                tokio::task::spawn_blocking(move || blobs.read_range(&located, offset, length))
                    .await
                    .map_err(unavailable)?
                    .map_err(unavailable)?;
            let through = offset + bytes.len() as u64;
            if through != offset + length {
                return Err(unavailable("material range is incomplete"));
            }
            Ok(FetchMaterialReply::Material {
                material: Box::new(MaterialFrame {
                    source,
                    id: material.id,
                    input,
                    blob: material.blob,
                    length: material.length,
                    chunks: Vec::new(),
                    parser: material.parser,
                    parser_version: material.parser_version,
                    part_index: material.part_index,
                    previous_part: material.previous_part,
                    manifest: material.manifest,
                }),
                offset,
                bytes,
                next_offset: (through < end).then_some(through),
            })
        }
        .await;
        result.unwrap_or_else(FetchMaterialReply::Refused)
    }
}

#[cfg(test)]
mod tests;
