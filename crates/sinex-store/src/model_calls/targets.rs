//! A request's claimed identities are checked against current stored evidence.
use crate::{Blobs, Error, Result, derivations};
use sinex_model::model_calls::{InputHash, InputIdentity, InputSelection, Request};
use sinex_model::{Event, EventId, Origin};
use sqlx::PgConnection;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;

pub(super) async fn verify(
    conn: &mut PgConnection,
    blobs: Option<&Blobs>,
    request: &Request,
) -> Result<()> {
    request.validate().map_err(Error::Invalid)?;
    let mut targets = BTreeMap::<EventId, Vec<&InputIdentity>>::new();
    for target in &request.inputs {
        targets
            .entry(target.event)
            .or_default()
            .push(&target.identity);
    }
    let mut found = BTreeSet::new();
    let ids = targets.keys().copied().collect();
    let mut read = derivations::ExactInputs::new(ids, 1000, crate::PAGE_BYTES)?;
    while let Some(page) = read.next(conn).await? {
        for committed in page {
            let event = committed.event;
            let requirements: Vec<_> = targets[&event.id]
                .iter()
                .map(|identity| {
                    let paths: Vec<String> = match &identity.selection {
                        InputSelection::Payload => vec![String::new()],
                        InputSelection::Text { pointer } => vec![pointer.clone()],
                        InputSelection::Bytes { field, .. } if field != "material" => {
                            vec![field.clone()]
                        }
                        InputSelection::Bytes { .. } => vec![],
                    };
                    sinex_model::InputRequirement::new(event.kind_ref(), paths)
                })
                .collect();
            if !crate::privacy::estimability::unavailable(
                conn,
                std::slice::from_ref(&event),
                Some(&requirements),
            )
            .await?
            .is_empty()
            {
                return Err(Error::Invalid(
                    "model selected content is unavailable under current policy".into(),
                ));
            }
            for identity in &targets[&event.id] {
                verify_one(conn, blobs, &event, identity).await?;
            }
            found.insert(event.id);
        }
    }
    if found.len() != targets.len() {
        return Err(Error::Invalid(
            "model request target is missing, archived or unavailable under current policy".into(),
        ));
    }
    Ok(())
}

async fn verify_one(
    conn: &mut PgConnection,
    blobs: Option<&Blobs>,
    event: &Event,
    expected: &InputIdentity,
) -> Result<()> {
    let actual = match &expected.selection {
        InputSelection::Payload | InputSelection::Text { .. } => {
            InputIdentity::for_event(event, expected.selection.clone(), None).map_err(|_| {
                Error::Invalid(
                    "model request selected content is unavailable under current policy".into(),
                )
            })?
        }
        InputSelection::Bytes {
            field,
            blob,
            offset,
            length,
        } => {
            let size = if field == "material" {
                let Origin::Record { location, .. } = &event.origin else {
                    return Err(Error::Invalid(
                        "model byte input is not a material reading".into(),
                    ));
                };
                // This selects retained source bytes, so use the same raw
                // material boundary as public fetch rather than bypassing a
                // field transform through its unchanged source material.
                crate::material_bytes::authorize_raw(conn, location.material).await?;
                let row = sqlx::query!("SELECT m.blob, m.length FROM raw.materials m JOIN core.blobs b ON b.hash=m.blob WHERE m.id=$1 AND b.status='present'", location.material.0)
                    .fetch_optional(&mut *conn).await?.ok_or_else(|| Error::Invalid("model input material is unavailable".into()))?;
                if row.blob != *blob {
                    return Err(Error::Invalid(
                        "model input material hash differs from the cited reading".into(),
                    ));
                }
                u64::try_from(row.length)
                    .map_err(|_| Error::Invalid("model input material length is invalid".into()))?
            } else {
                if event
                    .payload
                    .pointer(field)
                    .and_then(serde_json::Value::as_str)
                    != Some(blob.as_str())
                {
                    return Err(Error::Invalid("model input content pointer is unavailable or differs from the cited reading".into()));
                }
                if let Some(owned) = event.owned_blobs.iter().find(|owned| &owned.blob == blob) {
                    owned.size
                } else {
                    let row = sqlx::query!("SELECT b.size FROM core.event_blobs e JOIN core.blobs b ON b.hash=e.blob WHERE e.event_id=$1 AND e.blob=$2 AND b.status='present'", event.id.0, blob)
                        .fetch_optional(&mut *conn).await?.ok_or_else(|| Error::Invalid("model input content is not owned by the cited reading".into()))?;
                    u64::try_from(row.size).map_err(|_| {
                        Error::Invalid("model input content length is invalid".into())
                    })?
                }
            };
            if offset.checked_add(*length).is_none_or(|end| end > size) {
                return Err(Error::Invalid(
                    "model input range exceeds its retained content".into(),
                ));
            }
            let blobs = blobs
                .ok_or_else(|| Error::Invalid("model input content store is unavailable".into()))?
                .clone();
            let name = blob.clone();
            let offset = *offset;
            let length = *length;
            let hash = tokio::task::spawn_blocking(move || -> Result<InputHash> {
                let mut reader = blobs.read_plain(&name, Some(size), || Ok(()))?;
                let skipped =
                    std::io::copy(&mut reader.by_ref().take(offset), &mut std::io::sink())?;
                if skipped != offset {
                    return Err(Error::Invalid(
                        "model input bytes ended before their selected offset".into(),
                    ));
                }
                let mut reader = reader.take(length);
                let mut hasher = blake3::Hasher::new();
                let read = std::io::copy(&mut reader, &mut hasher)?;
                if read != length {
                    return Err(Error::Invalid(
                        "model input bytes ended before their selected range".into(),
                    ));
                }
                Ok(InputHash(hasher.finalize().to_hex().to_string()))
            })
            .await
            .map_err(|_| {
                Error::Invalid("model input content validation did not complete".into())
            })??;
            let mut actual = InputIdentity::for_event(event, InputSelection::Payload, None)
                .map_err(Error::Invalid)?;
            actual.selection = expected.selection.clone();
            actual.input_hash = hash;
            actual.selected_bytes = length;
            actual
        }
    };
    if &actual != expected {
        return Err(Error::Invalid("model request typed source, kind, record, reading hash or selected content differs from current evidence".into()));
    }
    Ok(())
}
