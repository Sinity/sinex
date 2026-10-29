//! Resolve exact definitions once for the batch before privacy and staging.
use crate::{Error, Result};
use sinex_model::kinds::Normalization;
use sinex_model::{
    Batch, BatchKind, Event, EventId, KindDeclaration, KindRef, Origin, Validation, event_types,
};
use sqlx::PgConnection;
use std::collections::{BTreeMap, BTreeSet};

pub(super) async fn prepare(
    conn: &mut PgConnection,
    batch: &Batch,
    events: &mut [Event],
) -> Result<BTreeMap<KindRef, KindDeclaration>> {
    let refs: Vec<_> = events
        .iter()
        .map(Event::kind_ref)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let mut declarations = sinex_schema::kinds::load(conn, &refs).await?;
    for reference in &refs {
        if declarations.contains_key(reference) {
            continue;
        }
        let BatchKind::Ingress(frame) = &batch.kind else {
            return Err(Error::Invalid(format!(
                "unknown kind reference {reference:?}"
            )));
        };
        let family = frame.family.as_ref().ok_or_else(|| {
            Error::Invalid("producer declaration has no configured family".into())
        })?;
        sinex_schema::kinds::check_namespace(conn, &reference.name, family).await?;
        let exists: bool = sqlx::query_scalar!(
            r#"SELECT EXISTS(SELECT 1 FROM registry.event_types WHERE name=$1) AS "exists!""#,
            &reference.name
        )
        .fetch_one(&mut *conn)
        .await?;
        if exists {
            return Err(Error::Invalid(format!(
                "unknown kind version {reference:?}"
            )));
        }
        let d = KindDeclaration::schemaless(reference.clone(), family.clone());
        sinex_schema::kinds::register(conn, family, std::slice::from_ref(&d)).await?;
        declarations.insert(reference.clone(), d);
    }
    let mut rust_validators = BTreeMap::new();
    let mut json_validators = BTreeMap::new();
    for (reference, declaration) in &declarations {
        match declaration.normalization {
            Normalization::Rust => {
                let kind = event_types::ALL
                    .iter()
                    .zip(event_types::declarations())
                    .find(|(_, bundled)| *bundled == declaration)
                    .map(|(kind, _)| kind)
                    .ok_or_else(|| {
                        Error::Invalid(format!("validator_unavailable: {reference:?}"))
                    })?;
                rust_validators.insert(reference.clone(), kind);
            }
            Normalization::Json => {
                json_validators.insert(
                    reference.clone(),
                    sinex_schema::kinds::validator(declaration).map_err(Error::Invalid)?,
                );
            }
        }
    }
    for event in events {
        if declarations[&event.kind_ref()].key_rule.is_none() {
            return Err(Error::Invalid(
                "historical declarations cannot accept new readings".into(),
            ));
        }
        let d = &declarations[&event.kind_ref()];
        let producer_frame = matches!(batch.kind, BatchKind::Ingress(_));
        match d.normalization {
            Normalization::Rust => {
                let k = rust_validators[&d.kind];
                if producer_frame || event.validation != Validation::Validated {
                    let id: EventId = event.id;
                    *event = (k.build)(
                        &event.payload,
                        &event.source,
                        event.origin.clone(),
                        event.reading_order,
                        event.record_hash,
                        event.class,
                    )
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                    event.id = id;
                }
                event.validation = Validation::Validated;
            }
            Normalization::Json => {
                let validator = &json_validators[&d.kind];
                if let Some(validator) = &validator {
                    let failures: Vec<_> = validator
                        .iter_errors(&event.payload)
                        .map(|e| format!("{} at {}", e.schema_path(), e.instance_path()))
                        .collect();
                    if !failures.is_empty() {
                        return Err(Error::Invalid(failures.join("; ")));
                    }
                }
                let evaluated = d
                    .evaluate(&event.payload, event.class)
                    .map_err(Error::Invalid)?;
                event.equivalence_key = evaluated.equivalence_key;
                event.ts_orig = evaluated.ts_orig;
                event.record_state = evaluated.record_state;
                event.owned_blobs = evaluated.owned_blobs;
                event.class = evaluated.class;
                event.replace = d.replace_rule;
                event.validation = if validator.is_some() {
                    Validation::Validated
                } else {
                    Validation::Unvalidated
                };
            }
        }
        if let Origin::Derived { parents } = &event.origin
            && (parents.is_empty() || event.equivalence_key.is_none())
        {
            return Err(Error::Invalid(
                "derived events need parents and field identity".into(),
            ));
        }
    }
    Ok(declarations)
}
