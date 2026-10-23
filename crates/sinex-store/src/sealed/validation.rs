//! A prepared source chunk cannot reintroduce a retired content capability.
use super::{SecretRole, key_ref, sealed_text};
use crate::{Error, Result};
use sinex_model::{Batch, BatchKind, MaterialId, RecordOutcome};
use sqlx::PgConnection;
use std::collections::BTreeMap;

pub(crate) async fn validate_batch(conn: &mut PgConnection, batch: &Batch) -> Result<()> {
    let mut references: BTreeMap<String, Vec<Option<MaterialId>>> = BTreeMap::new();
    for event in batch.events() {
        if event.class == sinex_model::Class::Sealed
            && let Some(reference) = key_ref(&event.payload)
        {
            references
                .entry(reference.into())
                .or_default()
                .push(event.location().map(|(location, _)| location.material));
        }
    }
    for record in &batch.records {
        if let RecordOutcome::Failure { location, reason } = record
            && let Some(value) = sealed_text(reason)
            && let Some(reference) = key_ref(&value)
        {
            references
                .entry(reference.into())
                .or_default()
                .push(Some(location.material));
        }
    }
    let progress = match &batch.kind {
        BatchKind::Source(p) => Some(&**p),
        BatchKind::Ingress(f) => f.material.as_ref(),
        _ => None,
    };
    let carry_ref = progress.and_then(|p| key_ref(&p.carry));
    if let Some(reference) = carry_ref {
        references.entry(reference.into()).or_default();
    }
    for (reference, locations) in references {
        let row = sqlx::query!(
            r#"SELECT k.material_id,k.role,k.class,i.binding AS "binding?" FROM privacy.material_keys k
             LEFT JOIN raw.materials m ON m.id=k.material_id
             LEFT JOIN raw.inputs i ON i.id=m.input_id WHERE k.key_ref=$1"#,
            reference,
        )
        .fetch_optional(&mut *conn)
        .await?
        .ok_or_else(|| {
            Error::Invalid("sealed content capability was retired; reload source state".into())
        })?;
        if row.class != "sealed"
            || row.role != SecretRole::DataAndRecordNaming.as_str()
            || locations
                .iter()
                .flatten()
                .any(|location| location.0 != row.material_id)
            || carry_ref == Some(reference.as_str())
                && row.binding.as_deref() != progress.map(|p| p.binding.as_str())
        {
            return Err(Error::Invalid(
                "sealed content capability does not own this batch's data and progress".into(),
            ));
        }
    }
    Ok(())
}
