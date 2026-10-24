//! Polylogue's confirmed row descriptors authorize superseded row batches and source dependencies.
//! Selection is staged on disk; descriptors and commit-log entries stay intact.
use sinex_model::event_types::polylogue::{ArchiveUnit, PolylogueRows};
use sinex_protocol::{RetireBatches, Retirement};
use sinex_store::{Blobs, retirement as store};
use sqlx::{Connection, PgConnection};

fn identity(unit: &ArchiveUnit) -> String {
    serde_json::to_string(unit).expect("unit serializes")
}
fn hash_hex(hash: &str) -> Result<&str, String> {
    let hex = hash
        .strip_prefix("blake3:")
        .filter(|h| {
            h.len() == 64
                && h.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        })
        .ok_or("invalid content hash")?;
    Ok(hex)
}

fn frame(archive: &str, hash: &str) -> Result<String, String> {
    Ok(format!("batch:{archive}:{}", hash_hex(hash)?))
}
async fn sources(
    conn: &mut PgConnection,
    producer: &str,
    key: &str,
    hashes: &[String],
) -> Result<(), String> {
    for hash in hashes {
        store::candidate(
            conn,
            producer,
            key,
            &format!("blob:{}", hash_hex(hash)?),
            hash,
            None,
            false,
        )
        .await
        .map_err(|e| e.to_string())?;
    }
    Ok(())
}

async fn protect_archives(conn: &mut PgConnection) -> Result<(), String> {
    let mut after = 0;
    loop {
        let rows = store::all_descriptors(conn, "polylogue.rows", after)
            .await
            .map_err(|e| e.to_string())?;
        if rows.is_empty() {
            return Ok(());
        }
        for (seq, producer, payload) in rows {
            after = seq;
            let row: PolylogueRows = serde_json::from_value(payload).map_err(|e| e.to_string())?;
            let namespace =
                serde_json::to_string(&(producer, &row.archive)).expect("namespace serializes");
            let unit = ArchiveUnit {
                commit_seq: row.local_seq,
                kind: row.unit,
                key: row.key,
            };
            if !unit.has_known_kind() {
                return Err("unknown archive unit".into());
            }
            store::protect(
                conn,
                &namespace,
                &identity(&unit),
                0,
                "",
                Some(&row.batch),
                &row.blobs,
            )
            .await
            .map_err(|e| e.to_string())?;
            if let Some(publication) = row.excision {
                if unit.kind != "excision" {
                    return Err("replacement manifest has no excision owner".into());
                }
                for replacement in publication.replacements {
                    if replacement.unit >= unit {
                        return Err("replacement is not earlier than its owner".into());
                    }
                    store::protect(
                        conn,
                        &namespace,
                        &identity(&replacement.unit),
                        unit.commit_seq,
                        &unit.key,
                        replacement.batch.as_deref(),
                        &replacement.blobs,
                    )
                    .await
                    .map_err(|e| e.to_string())?;
                }
            }
        }
    }
}

/// A recorded frame is historical evidence, not a grant to reuse erased backing.
/// Return availability separately without changing its durable acknowledgment.
pub async fn availability(
    conn: &mut PgConnection,
    producer: &str,
    frames: &mut [sinex_store::commit_log::StoredFrame],
) -> Result<(), String> {
    let expected: Vec<_> = frames
        .iter()
        .filter(|f| f.refusal.is_none() && f.material_id.is_some())
        .filter_map(|f| {
            f.frame
                .strip_prefix("blob:")
                .map(|hex| (f.frame.clone(), format!("blake3:{hex}")))
        })
        .collect();
    let missing = store::unavailable(conn, producer, &expected)
        .await
        .map_err(|e| e.to_string())?;
    for frame in frames {
        if missing.contains(&frame.frame) {
            frame.refusal = Some("recorded source material backing is unavailable; historical acknowledgment is preserved".into());
        }
    }
    Ok(())
}

/// Run under the writer transaction so a queued descriptor cannot revive a
/// reference after retirement has changed its material backing.
pub async fn validate_sources(
    conn: &mut PgConnection,
    batch: &sinex_model::Batch,
) -> Result<(), String> {
    let sinex_model::BatchKind::Ingress(frame) = &batch.kind else {
        return Ok(());
    };
    let mut expected = Vec::new();
    for event in batch.events().filter(|e| e.event_type == "polylogue.rows") {
        let row: PolylogueRows =
            serde_json::from_value(event.payload.clone()).map_err(|e| e.to_string())?;
        for hash in row.blobs.iter().chain(
            row.excision
                .iter()
                .flat_map(|p| p.replacements.iter().flat_map(|r| r.blobs.iter())),
        ) {
            expected.push((format!("blob:{}", hash_hex(hash)?), hash.clone()));
        }
    }
    let missing = store::unavailable(conn, &frame.producer, &expected)
        .await
        .map_err(|e| e.to_string())?;
    if !missing.is_empty() {
        return Err("row descriptor cites unavailable producer source material".into());
    }
    Ok(())
}

pub async fn retire(
    conn: &mut PgConnection,
    blobs: &Blobs,
    producer: &str,
    request: &RetireBatches,
) -> Result<Retirement, String> {
    {
        let mut tx = conn.begin().await.map_err(|e| e.to_string())?;
        store::begin(&mut tx).await.map_err(|e| e.to_string())?;
        let mut after = 0;
        let mut proof = false;
        loop {
            let rows = store::descriptors(
                &mut tx,
                producer,
                "polylogue.rows",
                "archive",
                &request.archive,
                after,
            )
            .await
            .map_err(|e| e.to_string())?;
            if rows.is_empty() {
                break;
            }
            for (seq, event, material, payload) in rows {
                after = seq;
                let row: PolylogueRows =
                    serde_json::from_value(payload).map_err(|e| e.to_string())?;
                if row.position == 0
                    || row.local_seq < 1
                    || row.key.is_empty()
                    || material.is_none()
                {
                    return Err("invalid confirmed row descriptor".into());
                }
                let unit = ArchiveUnit {
                    commit_seq: row.local_seq,
                    kind: row.unit,
                    key: row.key,
                };
                if !unit.has_known_kind() {
                    return Err("unknown archive unit".into());
                }
                let key = identity(&unit);
                let position =
                    i64::try_from(row.position).map_err(|_| "position exceeds supported range")?;
                store::original(
                    &mut tx,
                    &key,
                    position,
                    unit.commit_seq,
                    &format!("{}:{}", unit.kind, unit.key),
                )
                .await
                .map_err(|e| e.to_string())?;
                store::select(&mut tx, &key, 0, "", Some(&row.batch), &row.blobs, false)
                    .await
                    .map_err(|e| e.to_string())?;
                store::candidate(
                    &mut tx,
                    producer,
                    &key,
                    &frame(&request.archive, &row.batch)?,
                    &row.batch,
                    Some(event),
                    true,
                )
                .await
                .map_err(|e| e.to_string())?;
                sources(&mut tx, producer, &key, &row.blobs).await?;
                if let Some(publication) = row.excision {
                    if unit.kind != "excision" {
                        return Err("replacement manifest has no excision owner".into());
                    }
                    if unit == request.owner && publication.digest == request.digest {
                        proof = true;
                    }
                    // A replacement can only describe an earlier unit.
                    for replacement in publication.replacements {
                        if replacement.unit >= unit {
                            return Err("replacement is not earlier than its owner".into());
                        }
                        let key = identity(&replacement.unit);
                        store::select(
                            &mut tx,
                            &key,
                            unit.commit_seq,
                            &unit.key,
                            replacement.batch.as_deref(),
                            &replacement.blobs,
                            true,
                        )
                        .await
                        .map_err(|e| e.to_string())?;
                        sources(&mut tx, producer, &key, &replacement.blobs).await?;
                        if let Some(hash) = replacement.batch {
                            store::candidate(
                                &mut tx,
                                producer,
                                &key,
                                &frame(&request.archive, &hash)?,
                                &hash,
                                None,
                                false,
                            )
                            .await
                            .map_err(|e| e.to_string())?;
                        }
                    }
                }
            }
        }
        if !proof {
            return Err("no confirmed producer-owned archive/excision/digest proof".into());
        }
        store::validate(&mut tx).await.map_err(|e| e.to_string())?;
        protect_archives(&mut tx).await?;
        let applied = store::apply(&mut tx, blobs)
            .await
            .map_err(|e| e.to_string())?;
        let mut identity = blake3::Hasher::new();
        identity.update(b"sinex-row-retirement-v1\0");
        identity.update(&(producer.len() as u64).to_be_bytes());
        identity.update(producer.as_bytes());
        identity.update(&serde_json::to_vec(request).map_err(|e| e.to_string())?);
        let operation = uuid::Uuid::from_bytes(
            identity.finalize().as_bytes()[..16]
                .try_into()
                .expect("16 bytes"),
        );
        store::record(
            &mut tx,
            operation,
            producer,
            serde_json::to_value(request).map_err(|e| e.to_string())?,
        )
        .await
        .map_err(|e| e.to_string())?;
        tx.commit().await.map_err(|e| e.to_string())?;
        // Every request retries real unlink work, including an already applied
        // proof whose reply was lost. Startup uses this same durable queue.
        sinex_store::excision::finish_deletions(conn, blobs)
            .await
            .map_err(|e| e.to_string())?;
        Ok(Retirement {
            retired_materials: applied.retired,
            retained_materials: applied.retained,
            pending_files: store::pending(conn).await.map_err(|e| e.to_string())? as u64,
            retained_files: store::retained(conn).await.map_err(|e| e.to_string())? as u64,
            refusal: None,
        })
    }
}
