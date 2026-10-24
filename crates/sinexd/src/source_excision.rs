//! Source acquisition applies permanent exclusions before retaining readable bytes.
use crate::sealed_access::Authority;
use sinex_model::{Event, ExcisionBasis, MaterialId};
use sinex_store::{
    Blobs, excision,
    sealed::{MaterialSecrets, metadata},
};
use sqlx::PgConnection;
use std::collections::BTreeSet;
use uuid::Uuid;

struct Domain {
    names: MaterialSecrets,
    raw: BTreeSet<[u8; 32]>,
}

pub(crate) struct AcquisitionPolicy {
    pub basis: ExcisionBasis,
    domains: Vec<Domain>,
    keys: BTreeSet<(String, String)>,
    hashes: BTreeSet<(String, [u8; 32])>,
    locations: BTreeSet<(MaterialId, u64)>,
    authority: Option<Authority>,
}
impl AcquisitionPolicy {
    pub async fn load(
        conn: &mut PgConnection,
        blobs: &Blobs,
        source: &str,
        authority: Option<&Authority>,
    ) -> Result<Self, String> {
        let (basis, operations) = excision::operation_set(conn, source)
            .await
            .map_err(|e| e.to_string())?;
        let mut policy = Self {
            basis,
            domains: Vec::new(),
            keys: BTreeSet::new(),
            hashes: BTreeSet::new(),
            locations: BTreeSet::new(),
            authority: authority.cloned(),
        };
        if operations.is_empty() {
            return Ok(policy);
        }
        let image:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM raw.sqlite_images x JOIN raw.materials m ON m.id=x.material_id JOIN raw.inputs i ON i.id=m.input_id JOIN raw.bindings b ON b.binding=i.binding WHERE b.source=$1)")
            .bind(source).fetch_one(&mut *conn).await.map_err(|e|e.to_string())?;
        if image {
            return Err("source has permanent exclusions and physical images; image-safe acquisition is unavailable".into());
        }
        let authority = authority
            .ok_or("permanent sealed exclusions require explicit scoped unlock authority")?;
        authority.check()?;
        let scopes: Vec<serde_json::Value> =
            sqlx::query_scalar("SELECT scope FROM core.operations WHERE id=ANY($1) ORDER BY id")
                .bind(&operations)
                .fetch_all(&mut *conn)
                .await
                .map_err(|e| e.to_string())?;
        let mut materials = BTreeSet::new();
        for scope in scopes {
            if scope["record_domains_version"] != 1 {
                return Err("legacy exclusion lacks verified record naming domains".into());
            }
            let domains = scope["record_domains"]
                .as_array()
                .ok_or("exclusion lacks record naming domains")?;
            for domain in domains {
                if domain["source"] != source || domain["naming"] != "sealed" {
                    return Err(
                        "exclusion lacks a supported source-owned sealed naming domain".into(),
                    );
                }
                materials.insert(
                    serde_json::from_value::<MaterialId>(domain["material"].clone())
                        .map_err(|_| "invalid exclusion material identity")?,
                );
            }
        }
        for material in materials {
            authority.check()?;
            let (_, names) = authority.material_secrets(conn, material).await?;
            let (parser,version,length,blob):(String,i32,i64,Option<String>)=sqlx::query_as("SELECT parser,parser_version,durable_bytes,erased_replay_blob FROM raw.materials WHERE id=$1")
                .bind(material.0).fetch_one(&mut *conn).await.map_err(|e|e.to_string())?;
            let blob =
                blob.ok_or("exclusion lacks authenticated exact raw-byte matching evidence")?;
            let scope = metadata::Scope::Replay {
                source: source.into(),
                material,
                parser,
                parser_version: version,
                length: u64::try_from(length).map_err(|_| "negative excluded material length")?,
                whole: names.role() == sinex_store::sealed::SecretRole::RecordNamingOnly,
            };
            let check = authority.clone();
            let reader = blobs
                .read_plain(&blob, None, move || {
                    check
                        .check()
                        .map_err(std::io::Error::other)
                        .map_err(Into::into)
                })
                .map_err(|e| e.to_string())?;
            let records = authority.open_replay_metadata(reader, &scope)?;
            policy.domains.push(Domain {
                names,
                raw: records.into_iter().map(|r| r.raw_digest).collect(),
            });
        }
        let keys:Vec<(String,Option<String>,Option<Uuid>,Option<i64>)>=sqlx::query_as("SELECT event_type,equivalence_key,material_id,anchor FROM core.hidden_occurrences WHERE kind='excise' AND source=$1 AND operation_id=ANY($2)")
            .bind(source).bind(&operations).fetch_all(&mut *conn).await.map_err(|e|e.to_string())?;
        for (kind, key, material, anchor) in keys {
            if let Some(key) = key {
                policy.keys.insert((kind, key));
            } else if let (Some(material), Some(anchor)) = (material, anchor) {
                policy.locations.insert((
                    MaterialId(material),
                    u64::try_from(anchor).map_err(|_| "negative excluded record anchor")?,
                ));
            } else {
                return Err("excluded keyless record lacks exact location evidence".into());
            }
        }
        let hashes:Vec<(String,Vec<u8>)>=sqlx::query_as("SELECT event_type,record_hash FROM core.excised_digests WHERE source=$1 AND operation_id=ANY($2)")
            .bind(source).bind(&operations).fetch_all(&mut *conn).await.map_err(|e|e.to_string())?;
        for (kind, hash) in hashes {
            policy.hashes.insert((
                kind,
                hash.try_into()
                    .map_err(|_| "invalid excluded record digest")?,
            ));
        }
        policy.revalidate(conn).await?;
        Ok(policy)
    }
    pub async fn revalidate(&self, conn: &mut PgConnection) -> Result<(), String> {
        if let Some(authority) = &self.authority {
            authority.check()?;
        }
        let current = excision::operations_basis(conn, &self.basis.source)
            .await
            .map_err(|e| e.to_string())?;
        if current != self.basis {
            return Err("source excision operations changed; retry acquisition".into());
        }
        Ok(())
    }
    pub fn excludes(
        &self,
        material: MaterialId,
        anchor: u64,
        raw: &[u8],
        events: &[Event],
    ) -> Result<bool, String> {
        if let Some(authority) = &self.authority {
            authority.check()?;
        }
        if self.locations.contains(&(material, anchor)) {
            return Ok(true);
        }
        for domain in &self.domains {
            if domain.raw.contains(&domain.names.name_raw_record(raw)) {
                return Ok(true);
            }
            for event in events {
                if event.source != self.basis.source {
                    return Err("parser output changed source exclusion namespace".into());
                }
                if event.equivalence_key.as_ref().is_some_and(|key| {
                    self.keys.contains(&(
                        event.event_type.into(),
                        domain.names.name_equivalence(&key.0).0,
                    ))
                }) || self.hashes.contains(&(
                    event.event_type.into(),
                    domain.names.name_record_hash(&event.record_hash),
                )) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests;
