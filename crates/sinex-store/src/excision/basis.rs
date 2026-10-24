//! Exclusion policy is checked before keys, bytes or source progress become owned.
use crate::{Error, Result};
use futures::TryStreamExt;
use sinex_model::{Batch, BatchKind, ExcisionBasis};
use sqlx::PgConnection;
use std::collections::BTreeSet;

pub async fn operations_basis(conn: &mut PgConnection, source: &str) -> Result<ExcisionBasis> {
    visit_operations(conn, source, |_| {}).await
}

/// The acquisition owner loads exclusions from exactly this source operation set.
pub async fn operation_set(
    conn: &mut PgConnection,
    source: &str,
) -> Result<(ExcisionBasis, Vec<uuid::Uuid>)> {
    let mut ids = Vec::new();
    let basis = visit_operations(conn, source, |id| ids.push(id)).await?;
    Ok((basis, ids))
}

async fn visit_operations(
    conn: &mut PgConnection,
    source: &str,
    mut visit: impl FnMut(uuid::Uuid),
) -> Result<ExcisionBasis> {
    let mut operations = sqlx::query_scalar!(
        r#"SELECT o.id FROM core.operations o WHERE o.kind='excise' AND (
             o.scope->'sources' ? $1
             OR EXISTS(SELECT 1 FROM core.event_tombstones t WHERE t.operation_id=o.id AND t.source=$1)
             OR EXISTS(SELECT 1 FROM core.excised_digests d WHERE d.operation_id=o.id AND d.source=$1)
             OR EXISTS(SELECT 1 FROM core.hidden_occurrences h WHERE h.operation_id=o.id AND h.source=$1 AND h.kind='excise')
             OR (o.scope->'sources' IS NULL AND o.scope->>'material' IS NOT NULL AND EXISTS(
                  SELECT 1 FROM raw.materials m LEFT JOIN raw.inputs i ON i.id=m.input_id
                  LEFT JOIN raw.bindings b ON b.binding=i.binding
                  WHERE m.id::text=o.scope->>'material' AND (b.source=$1 OR b.source IS NULL))))
           ORDER BY o.id"#,
        source
    ).fetch(conn);
    let mut hash = blake3::Hasher::new();
    hash.update(b"sinex source excision operations 1\0");
    let mut count = 0u64;
    while let Some(id) = operations.try_next().await? {
        hash.update(id.as_bytes());
        visit(id);
        count = count
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("too many excision operations".into()))?;
    }
    Ok(ExcisionBasis {
        source: source.into(),
        operations: *hash.finalize().as_bytes(),
        count,
    })
}

pub async fn validate_basis(conn: &mut PgConnection, batch: &Batch) -> Result<()> {
    let progress = match &batch.kind {
        BatchKind::Source(p) => Some(&**p),
        BatchKind::Ingress(f) => {
            if f.refusal.is_some() && f.material.is_none() && batch.records.is_empty() {
                return Ok(());
            }
            f.material.as_ref()
        }
        // Replay reads retained replacements; derivations do not acquire source bodies.
        BatchKind::Replay { .. } | BatchKind::Derived { .. } | BatchKind::Api { .. } => {
            return Ok(());
        }
    };
    let supplied = progress.and_then(|p| p.excision_basis.as_ref());
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
    if let Some(basis) = supplied {
        sources.insert(basis.source.clone());
    }
    if sources.is_empty() {
        // Old opaque producer frames cannot establish which exclusion domain they acquire.
        let exclusions = sqlx::query_scalar!(
            "SELECT EXISTS(SELECT 1 FROM core.operations WHERE kind='excise') AS \"present!\""
        )
        .fetch_one(&mut *conn)
        .await?;
        if exclusions {
            return Err(Error::Invalid(
                "source excision basis is unavailable; source acquisition is pending".into(),
            ));
        }
    }
    for source in sources {
        let current = operations_basis(conn, &source).await?;
        if let Some(basis) = supplied {
            if *basis != current {
                return Err(Error::Invalid(
                    "source excision operations changed; retry acquisition before retaining bytes"
                        .into(),
                ));
            }
        } else if current.count != 0 {
            return Err(Error::Invalid(
                "source has permanent exclusions; acquisition needs its current excision basis"
                    .into(),
            ));
        }
    }
    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CitedMedia {
    version: u32,
    by_source: std::collections::BTreeMap<String, Vec<sinex_model::Cited>>,
}

/// Freeze only source-owned public/plain naming facts before removing their
/// event references. A sealed source remains unknown to this plain matcher.
pub(super) async fn capture_cited_media(
    conn: &mut PgConnection,
    sources: &[String],
    events: &[uuid::Uuid],
    domains: &serde_json::Value,
) -> Result<serde_json::Value> {
    let mut sealed: BTreeSet<String> = sqlx::query_scalar(
        "SELECT DISTINCT source FROM core.events WHERE id=ANY($1) AND class='sealed'",
    )
    .bind(events)
    .fetch_all(&mut *conn)
    .await?
    .into_iter()
    .collect();
    let domains = domains
        .as_array()
        .ok_or_else(|| Error::Invalid("missing record naming domains".into()))?;
    let mut unknown = false;
    for domain in domains {
        if domain["naming"] == "sealed" {
            match domain["source"].as_str() {
                Some(source) => {
                    sealed.insert(source.into());
                }
                None => unknown = true,
            }
        }
    }
    let mut by_source = sources
        .iter()
        .filter(|source| !unknown && !sealed.contains(*source))
        .map(|source| (source.clone(), Vec::new()))
        .collect::<std::collections::BTreeMap<_, _>>();
    let cited: Vec<(String,String,i64)> = sqlx::query_as("SELECT DISTINCT e.source,b.blob,c.size FROM core.events e JOIN core.event_blobs b ON b.event_id=e.id JOIN core.blobs c ON c.hash=b.blob WHERE e.id=ANY($1) ORDER BY e.source,b.blob")
        .bind(events).fetch_all(&mut *conn).await?;
    for (source, blob, size) in cited {
        if let Some(facts) = by_source.get_mut(&source) {
            facts.push(sinex_model::Cited {
                blob,
                size: u64::try_from(size)
                    .map_err(|_| Error::Invalid("negative cited media length".into()))?,
            });
        }
    }
    for facts in by_source.values() {
        crate::native_materials::citation_set(facts)?;
    }
    serde_json::to_value(CitedMedia {
        version: 1,
        by_source,
    })
    .map_err(|_| Error::Invalid("cited media facts cannot encode".into()))
}

/// None is historical or class-protected absence, distinct from a complete
/// empty list. No caller reconstructs missing past facts from current bytes.
pub fn cited_media(
    scope: &serde_json::Value,
    source: &str,
) -> Result<Option<Vec<sinex_model::Cited>>> {
    let Some(value) = scope.get("cited_media") else {
        return Ok(None);
    };
    let mut facts: CitedMedia = serde_json::from_value(value.clone())
        .map_err(|_| Error::Invalid("invalid cited media exclusion facts".into()))?;
    if facts.version != 1 {
        return Err(Error::Invalid(
            "unsupported cited media exclusion version".into(),
        ));
    }
    let sources: BTreeSet<String> = serde_json::from_value(scope["sources"].clone())
        .map_err(|_| Error::Invalid("cited media facts lack their source scope".into()))?;
    for (name, cited) in &facts.by_source {
        if name.is_empty() || !sources.contains(name) {
            return Err(Error::Invalid(
                "cited media facts differ from operation source scope".into(),
            ));
        }
        crate::native_materials::citation_set(cited)?;
    }
    Ok(facts.by_source.remove(source))
}
