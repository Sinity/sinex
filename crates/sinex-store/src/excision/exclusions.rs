//! Exclusions: what earlier excisions of a source keep out of its later
//! capture. Re-reading a source can meet an excised record again (a shell
//! history still holds the command); acquisition blanks such records before
//! their bytes are stored, and admission checks that the excisions it applied
//! are still the current ones.
use crate::{Error, Result};
use futures::TryStreamExt;
use sinex_model::{Batch, BatchKind, Exclusions};
use sqlx::PgConnection;
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

/// What the excisions of `source` exclude, read in one pass.
#[derive(Debug)]
pub struct Excluded {
    pub exclusions: Exclusions,
    /// `(event type, equivalence key)` of excised keyed records.
    pub keys: BTreeSet<(String, String)>,
    /// Record hashes of excised records.
    pub hashes: BTreeSet<[u8; 32]>,
    /// `(material, anchor)` of excised keyless records.
    pub locations: BTreeSet<(Uuid, i64)>,
    /// Stored bytes the excised events cited, with their lengths.
    pub media: BTreeMap<String, u64>,
}

/// The current excisions of `source`.
pub async fn current(conn: &mut PgConnection, source: &str) -> Result<Exclusions> {
    Ok(version(conn, source).await?.0)
}

async fn version(conn: &mut PgConnection, source: &str) -> Result<(Exclusions, Vec<Uuid>)> {
    let mut rows = sqlx::query_scalar!(
        r#"SELECT id FROM core.operations WHERE kind = 'excise' AND scope->'sources' ? $1 ORDER BY id"#,
        source
    )
    .fetch(conn);
    let mut ids = Vec::new();
    let mut digest = blake3::Hasher::new();
    while let Some(id) = rows.try_next().await? {
        digest.update(id.as_bytes());
        ids.push(id);
    }
    let exclusions = Exclusions {
        source: source.into(),
        digest: *digest.finalize().as_bytes(),
        count: ids.len() as u64,
    };
    Ok((exclusions, ids))
}

/// Everything the excisions of `source` exclude.
pub async fn load(conn: &mut PgConnection, source: &str) -> Result<Excluded> {
    let (exclusions, operations) = version(conn, source).await?;
    let mut excluded = Excluded {
        exclusions,
        keys: BTreeSet::new(),
        hashes: BTreeSet::new(),
        locations: BTreeSet::new(),
        media: BTreeMap::new(),
    };
    if operations.is_empty() {
        return Ok(excluded);
    }
    let hidden = sqlx::query!(
        r#"SELECT event_type, equivalence_key, material_id, anchor FROM core.hidden_occurrences
           WHERE kind = 'excise' AND source = $1 AND operation_id = ANY($2)"#,
        source,
        &operations,
    )
    .fetch_all(&mut *conn)
    .await?;
    for row in hidden {
        match (row.equivalence_key, row.material_id, row.anchor) {
            (Some(key), _, _) => {
                excluded.keys.insert((row.event_type, key));
            }
            (None, Some(material), Some(anchor)) => {
                excluded.locations.insert((material, anchor));
            }
            _ => {
                return Err(Error::Invalid(
                    "excised record has no key or location".into(),
                ));
            }
        }
    }
    let digests = sqlx::query_scalar!(
        "SELECT record_hash FROM core.excised_digests WHERE source = $1 AND operation_id = ANY($2)",
        source,
        &operations,
    )
    .fetch_all(&mut *conn)
    .await?;
    for hash in digests {
        let hash = hash
            .try_into()
            .map_err(|_| Error::Invalid("excised record hash is not 32 bytes".into()))?;
        excluded.hashes.insert(hash);
    }
    let scopes = sqlx::query_scalar!(
        "SELECT scope FROM core.operations WHERE id = ANY($1)",
        &operations
    )
    .fetch_all(&mut *conn)
    .await?;
    for scope in scopes {
        let cited: Vec<sinex_model::Cited> = scope["cited_media"]
            .get(source)
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()
            .map_err(|_| Error::Invalid("invalid excised media list".into()))?
            .unwrap_or_default();
        for c in cited {
            if excluded
                .media
                .insert(c.blob, c.size)
                .is_some_and(|old| old != c.size)
            {
                return Err(Error::Invalid(
                    "excised media has contradictory lengths".into(),
                ));
            }
        }
    }
    Ok(excluded)
}

/// The stored bytes `events` cite, by source, for an excision's record.
pub(super) async fn cited_media_of(
    conn: &mut PgConnection,
    events: &[Uuid],
) -> Result<serde_json::Value> {
    let cited = sqlx::query!(
        r#"SELECT DISTINCT e.source, b.blob, c.size FROM core.events e
           JOIN core.event_blobs b ON b.event_id = e.id JOIN core.blobs c ON c.hash = b.blob
           WHERE e.id = ANY($1) ORDER BY e.source, b.blob"#,
        events
    )
    .fetch_all(&mut *conn)
    .await?;
    let mut by_source: BTreeMap<String, Vec<sinex_model::Cited>> = BTreeMap::new();
    for row in cited {
        by_source
            .entry(row.source)
            .or_default()
            .push(sinex_model::Cited {
                blob: row.blob,
                size: u64::try_from(row.size)
                    .map_err(|_| Error::Invalid("negative blob size".into()))?,
            });
    }
    serde_json::to_value(by_source).map_err(|e| Error::Invalid(e.to_string()))
}

/// Refuse a batch whose acquisition applied other excisions than the current
/// ones of its source. Run inside admission.
pub async fn check(conn: &mut PgConnection, batch: &Batch) -> Result<()> {
    let progress = match &batch.kind {
        BatchKind::Source(p) => Some(&**p),
        BatchKind::Ingress(f) => {
            if f.refusal.is_some() && f.material.is_none() && batch.records.is_empty() {
                return Ok(());
            }
            f.material.as_ref()
        }
        // Replay reads stored materials; derivations and API requests read
        // no source.
        BatchKind::Replay { .. } | BatchKind::Derived { .. } | BatchKind::Api { .. } => {
            return Ok(());
        }
    };
    let applied = progress.and_then(|p| p.exclusions.as_ref());
    let mut sources: BTreeSet<String> = batch.events().map(|event| event.source.clone()).collect();
    if let BatchKind::Ingress(frame) = &batch.kind {
        sources.insert(frame.producer.clone());
    }
    if let Some(p) = progress
        && let Some(source) = sqlx::query_scalar!(
            "SELECT source FROM raw.bindings WHERE binding=$1",
            p.binding
        )
        .fetch_optional(&mut *conn)
        .await?
    {
        sources.insert(source);
    }
    if let Some(applied) = applied {
        sources.insert(applied.source.clone());
    }
    for source in sources {
        let current = current(conn, &source).await?;
        match applied {
            Some(applied) if *applied != current => {
                return Err(Error::Invalid(
                    "the source was excised during this read; read it again".into(),
                ));
            }
            None if current.count != 0 => {
                return Err(Error::Invalid(
                    "the source has excisions, and this read did not apply them".into(),
                ));
            }
            _ => {}
        }
    }
    Ok(())
}
