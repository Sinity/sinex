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
    if let Some(p) = progress {
        if let Some(source) = sqlx::query_scalar!(
            "SELECT source FROM raw.bindings WHERE binding=$1",
            p.binding
        )
        .fetch_optional(&mut *conn)
        .await?
        {
            sources.insert(source);
        }
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
