//! Scoped preparation. Only encrypted bodies, metadata and value replacements
//! leave this module; the Writer revalidates and atomically publishes them.
use super::*;
use crate::sealed_access::Authority;
use serde_json::Value;
use sinex_model::MaterialId;
use sinex_store::excision::{
    Candidate, KeyEnvelope,
    sealed::{KeyRotation, PreparedSealed, ReplayReplacement},
};
use sinex_store::sealed::{self as crypto, metadata};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Seek, Write};
use std::sync::Arc;
mod values;
mod walk;
use walk::Archive;

pub(super) struct Prepared {
    pub plan: Plan,
    pub rewrites: Vec<Rewrite>,
    pub sealed: PreparedSealed,
}

async fn select(
    conn: &mut PgConnection,
    blobs: &Blobs,
    target: Target,
    authority: &Authority,
) -> Result<(Archive, values::Positions, Plan), String> {
    authority.check()?;
    let initial = excision::plan(conn, target)
        .await
        .map_err(|e| e.to_string())?;
    let source = source_scope(conn, &initial)
        .await?
        .ok_or("sealed excision has no native source")?;
    let parser = initial
        .materials
        .first()
        .ok_or("sealed excision needs a native record material")?
        .parser
        .clone();
    if initial.materials.iter().any(|m| m.parser != parser) {
        return Err("sealed excision spans incompatible native parsers".into());
    }
    let archive = Archive::load(conn, &source, &parser).await?;
    // An epoch cannot be silently omitted because its current class changed.
    // Plain stateful carry/replay needs its own corresponding protected rewrite.
    if archive.candidates.iter().any(|c| c.key.is_none()) {
        return Err("mixed plain and sealed source epochs require a combined carry rewrite".into());
    }
    let mut selected: BTreeMap<MaterialId, BTreeSet<i64>> = initial
        .materials
        .iter()
        .map(|m| (m.id, m.anchors.clone()))
        .collect();
    let mut native_keys = BTreeSet::new();
    // Recover native equivalence identities instead of comparing the keyed
    // names of two independently encrypted epochs.
    for material in &initial.materials {
        archive
            .walk(conn, blobs, material.id, authority, |record| {
                if material.whole || material.anchors.contains(&(record.start as i64)) {
                    selected
                        .entry(material.id)
                        .or_default()
                        .insert(record.start as i64);
                    add_keys(&mut native_keys, record.events);
                }
                Ok(())
            })
            .await?;
    }
    loop {
        let before = (
            selected.values().map(BTreeSet::len).sum::<usize>(),
            native_keys.len(),
        );
        for candidate in &archive.candidates {
            archive
                .walk(conn, blobs, candidate.material, authority, |record| {
                    let matches = selected
                        .get(&candidate.material)
                        .is_some_and(|a| a.contains(&(record.start as i64)))
                        || record.events.iter().any(|e| {
                            e.equivalence_key.as_ref().is_some_and(|k| {
                                native_keys.contains(&(e.event_type.to_string(), k.0.clone()))
                            })
                        });
                    if matches {
                        selected
                            .entry(candidate.material)
                            .or_default()
                            .insert(record.start as i64);
                        add_keys(&mut native_keys, record.events);
                    }
                    Ok(())
                })
                .await?;
        }
        if before
            == (
                selected.values().map(BTreeSet::len).sum::<usize>(),
                native_keys.len(),
            )
        {
            break;
        }
    }
    let mut seeds = initial.seeds.clone();
    for (material, anchors) in &selected {
        let anchors = anchors.iter().copied().collect::<Vec<_>>();
        let ids: Vec<uuid::Uuid> = sqlx::query_scalar(
            "SELECT id FROM core.events WHERE material_id=$1 AND anchor=ANY($2)",
        )
        .bind(material.0)
        .bind(anchors)
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| e.to_string())?;
        seeds.extend(ids);
    }
    let positions =
        values::Positions::load(conn, blobs, &source, &archive, &selected, authority).await?;
    // A carry can outlive the selected material while still being encrypted
    // under a later material's key. Retire that key too, rewriting its body
    // unchanged; this is not a request to erase any of its record ranges.
    for owner in positions.data_owners() {
        include_unchanged_body_rekey(&mut selected, owner);
    }
    let mut plan = excision::plan_records(conn, target, &seeds, &selected)
        .await
        .map_err(|e| e.to_string())?;
    plan.candidate_source = Some(source.clone());
    plan.candidate_parser = Some(parser);
    plan.candidates = archive.candidates.clone();
    Ok((archive, positions, plan))
}

pub(crate) async fn plan(
    conn: &mut PgConnection,
    blobs: &Blobs,
    target: Target,
    authority: &Authority,
) -> Result<Plan, String> {
    Ok(select(conn, blobs, target, authority).await?.2)
}

pub(super) async fn prepare(
    conn: &mut PgConnection,
    blobs: &Blobs,
    target: Target,
    authority: &Authority,
) -> Result<Prepared, String> {
    let (archive, positions, plan) = select(conn, blobs, target, authority).await?;
    let source = plan
        .candidate_source
        .clone()
        .ok_or("missing frozen source")?;
    let mut changes = tempfile::tempfile().map_err(|_| "cannot prepare sealed replacements")?;
    let mut rotations = Vec::new();
    let mut rewrites = Vec::new();
    let mut replay = Vec::new();
    let mut keys = BTreeMap::new();
    for material in &plan.materials {
        authority.check()?;
        let candidate = archive
            .candidates
            .iter()
            .find(|c| c.material == material.id)
            .ok_or("sealed material is outside its frozen source candidates")?;
        let old = candidate
            .key
            .as_ref()
            .ok_or("sealed material lacks its key owner")?;
        let names = authority.unwrap_secrets(&old.wrapped)?;
        if names.role().as_str() != old.role || old.class != "sealed" {
            return Err("sealed material capability role or class changed".into());
        }
        let surviving_values: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM core.events WHERE class='sealed' AND payload->>'key'=$1 AND NOT(id=ANY($2))) OR EXISTS(SELECT 1 FROM telemetry.events WHERE class='sealed' AND payload->>'key'=$1)")
            .bind(&old.reference).bind(plan.events.iter().copied().collect::<Vec<_>>()).fetch_one(&mut *conn).await.map_err(|e| e.to_string())?;
        let fresh = if material.whole && !surviving_values {
            None
        } else {
            Some(crypto::new_key().map_err(|e| e.to_string())?)
        };
        let replacement_names = match &fresh {
            Some(key) => names.with_data_key(key),
            None => names.naming_only(),
        };
        let reference = fresh
            .as_ref()
            .map_or_else(|| old.reference.clone(), |k| k.key_ref.clone());
        let replacement = KeyEnvelope {
            reference,
            wrapped: crypto::wrap_secrets(&replacement_names, &authority.recipient())
                .map_err(|e| e.to_string())?,
            role: replacement_names.role().as_str().into(),
            class: "sealed".into(),
        };
        let mut opened =
            crate::retained::open(conn, blobs, &source, material.id, Some(authority)).await?;
        let mut records = std::mem::take(&mut opened.metadata);
        archive
            .walk(conn, blobs, material.id, authority, |record| {
                if material.whole || material.anchors.contains(&(record.start as i64)) {
                    let context = sinex_sources::RecordContext {
                        manifest: &candidate.manifest,
                        member: sinex_sources::drain::member_at(&candidate.manifest, record.start),
                    };
                    let transition = if record.failed {
                        None
                    } else {
                        record
                            .source
                            .erase_record(record.raw, record.after, &context)
                            .map_err(|e| e.to_string())?
                            .replay
                    };
                    records.push(metadata::ErasedRecord {
                        start: record.start as u64,
                        end: record.end as u64,
                        raw_digest: names.name_raw_record(record.raw),
                        transition,
                    });
                }
                Ok(())
            })
            .await?;
        records.sort_by_key(|r| r.start);
        let scope = metadata::Scope::Replay {
            source: source.clone(),
            material: material.id,
            parser: candidate.parser.clone(),
            parser_version: candidate.parser_version,
            length: candidate.length,
            whole: material.whole,
            append_end: candidate
                .manifest
                .get("append_resume")
                .and_then(|r| r.get("end"))
                .cloned(),
        };
        let mut ciphertext =
            tempfile::tempfile().map_err(|_| "cannot prepare sealed replay metadata")?;
        metadata::seal_replay(
            &mut ciphertext,
            &authority.recipient(),
            &scope,
            records,
            || authority.check().map_err(sinex_store::Error::Invalid),
        )
        .map_err(|e| e.to_string())?;
        let length = ciphertext.stream_position().map_err(|e| e.to_string())?;
        ciphertext.rewind().map_err(|e| e.to_string())?;
        let verified = authority.open_replay_metadata(&mut ciphertext, &scope)?;
        ciphertext.rewind().map_err(|e| e.to_string())?;
        let replay_blob = blobs
            .put_reader(&mut ciphertext)
            .map_err(|e| e.to_string())?;
        replay.push(ReplayReplacement {
            material: material.id,
            old_blob: candidate.erased_replay_blob.clone(),
            blob: replay_blob,
            length,
        });
        if let Some(fresh) = &fresh
            && !material.whole
        {
            let body = opened
                .body
                .take()
                .ok_or("sealed bodyless material cannot acquire a new data key")?;
            let body =
                Arc::try_unwrap(body).map_err(|_| "sealed body has another preparation owner")?;
            let spans = verified
                .iter()
                .map(|r| {
                    Ok((
                        usize::try_from(r.start).map_err(|_| "record exceeds address space")?,
                        usize::try_from(r.end).map_err(|_| "record exceeds address space")?,
                    ))
                })
                .collect::<Result<Vec<_>, &str>>()?;
            let reader = crate::retained::Blanked::new(body.into_reader()?, spans);
            let blob = blobs
                .put_sealed_reader(reader, fresh)
                .map_err(|e| e.to_string())?;
            // Authentication of every installed frame precedes publication.
            let check = authority.clone();
            let mut verified = blobs
                .read_sealed(
                    &blob,
                    Some(candidate.length),
                    fresh,
                    crate::sealed_access::read_limits(candidate.length)?,
                    move || {
                        check.check().map_err(std::io::Error::other)?;
                        Ok(())
                    },
                )
                .map_err(|e| e.to_string())?;
            std::io::copy(&mut verified, &mut std::io::sink())
                .map_err(|_| "replacement sealed body verification failed")?;
            rewrites.push(Rewrite {
                material: material.id,
                blob,
                length: candidate.length,
                manifest: None,
            });
        }
        keys.insert(
            old.reference.clone(),
            (names.data_key(old.reference.clone()).ok(), fresh),
        );
        rotations.push(KeyRotation {
            material: material.id,
            old: old.clone(),
            replacement,
        });
    }
    values::dependents(conn, &plan, &keys, &mut changes, authority).await?;
    positions.prepare(&keys, &mut changes, authority).await?;
    authority.check()?;
    Ok(Prepared {
        plan,
        rewrites,
        sealed: PreparedSealed {
            source,
            rotations,
            replay,
            changes: Arc::new(changes),
        },
    })
}

fn add_keys(out: &mut BTreeSet<(String, String)>, events: &[sinex_model::Event]) {
    out.extend(events.iter().filter_map(|e| {
        e.equivalence_key
            .as_ref()
            .map(|k| (e.event_type.to_string(), k.0.clone()))
    }));
}

fn include_unchanged_body_rekey(
    records: &mut BTreeMap<MaterialId, BTreeSet<i64>>,
    owner: MaterialId,
) {
    records.entry(owner).or_default();
}

fn write_change(
    file: &mut std::fs::File,
    change: &sinex_store::excision::sealed::CiphertextChange,
) -> Result<(), String> {
    serde_json::to_writer(&mut *file, change).map_err(|_| "cannot encode prepared ciphertext")?;
    file.write_all(b"\n")
        .map_err(|_| "cannot write prepared ciphertext".into())
}

#[cfg(test)]
mod tests;
