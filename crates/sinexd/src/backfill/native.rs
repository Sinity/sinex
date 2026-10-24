//! One scoped native acquisition owns preparation, publication and first admission.
use super::*;
use crate::excision::{AcquisitionGuard, exclusions::Exclusions};
use crate::publication::{NativeStage, Publication, StagingSink, Verification};
use sinex_model::{BatchKind, Class};
use sinex_store::native_materials::{self, Pending};
use std::sync::Arc;

pub(super) struct Pipeline<'a> {
    pub writer: &'a writer::WriterHandle,
    pub blobs: &'a sinex_store::Blobs,
    pub source: &'a dyn Source,
    pub binding: &'a str,
    pub run: RunId,
    pub class: Class,
    pub sealing: Option<&'a crate::one_shot::SealingSink>,
    pub authority: Option<&'a crate::sealed_access::Authority>,
    pub check: Arc<dyn Fn() -> Result<(), String> + Send + Sync>,
}
impl Pipeline<'_> {
    fn queued_check(&self) -> crate::writer::NativeCheck {
        let check = self.check.clone();
        let authority = self.authority.cloned();
        std::sync::Arc::new(move || {
            check()?;
            if let Some(authority) = &authority {
                authority.check()?;
            }
            Ok(())
        })
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn submit(
        &self,
        guard: AcquisitionGuard,
        exclusions: Exclusions,
        cursor: serde_json::Value,
        base_carry: serde_json::Value,
        carry: serde_json::Value,
        acquired: drivers::Acquired,
        totals: &mut Totals,
    ) -> Result<(), String> {
        let mut guard = Some(guard);
        let result = self
            .submit_owned(
                &mut guard, exclusions, cursor, base_carry, carry, acquired, totals,
            )
            .await;
        if let Some(guard) = guard {
            guard.release().await?;
        }
        result
    }
    #[allow(clippy::too_many_arguments)]
    async fn submit_owned(
        &self,
        guard: &mut Option<AcquisitionGuard>,
        exclusions: Exclusions,
        cursor: serde_json::Value,
        base_carry: serde_json::Value,
        carry: serde_json::Value,
        mut acquired: drivers::Acquired,
        totals: &mut Totals,
    ) -> Result<(), String> {
        (self.check)()?;
        let ids =
            native_materials::binding_pending(guard.as_mut().unwrap().connection(), self.binding)
                .await
                .map_err(|e| e.to_string())?;
        if ids.len() > 1 {
            return Err("binding has multiple native publications".into());
        }
        let frozen = match ids.first() {
            Some(id) => native_materials::load(guard.as_mut().unwrap().connection(), *id)
                .await
                .map_err(|e| e.to_string())?,
            None => None,
        };
        let stages = StagingSink::new(self.blobs.clone());
        let stored = if let Some(p) = &frozen {
            if p.progress.exclusions.as_ref() != Some(exclusions.version()) {
                return Err("the source was excised since this read; read it again".into());
            }
            let opened = crate::retained::open_pending(self.blobs, p, self.authority)?;
            let material = acquired
                .material
                .as_mut()
                .ok_or("pending native material was not restored before provider acquisition")?;
            if material.id != p.material {
                return Err("provider cannot replace the frozen pending native version".into());
            }
            if self.sealing.is_some() != opened.key.is_some() {
                return Err("pending native class differs from current acquisition class".into());
            }
            if let Some(sealing) = self.sealing {
                let key = p
                    .progress
                    .material_keys
                    .iter()
                    .find(|key| key.material == p.material)
                    .ok_or("pending native data key is unavailable")?;
                sealing.restore_reserved(
                    opened.reference.blob.clone(),
                    key,
                    self.authority
                        .ok_or("pending sealed material requires explicit unlock")?,
                )?;
            }
            material.stored = Some(drivers::StoredBytes::FramedReader(opened.records()));
            material.bytes.clear();
            material.manifest = opened.reference.manifest.clone();
            false
        } else {
            reopen_stored(
                guard.as_mut().unwrap().connection(),
                self.blobs,
                self.source,
                &mut acquired,
                self.sealing,
                self.authority,
            )
            .await?
        };
        let filtered = exclusions.filter(self.source, acquired, &carry, self.class, stored)?;
        let endpoints = filtered
            .material
            .as_ref()
            .map(|m| {
                let length = crate::excision::exclusions::byte_length(&m.record_bytes())?;
                let end =
                    usize::try_from(length).map_err(|_| "native material exceeds address space")?;
                Ok::<_, String>((
                    (m.cursor_at)(0).map_err(|e| e.to_string())?,
                    (m.cursor_at)(end).map_err(|e| e.to_string())?,
                ))
            })
            .transpose()?;
        let mut extra_stages = Vec::new();
        let sink: &dyn drivers::MaterialSink = match self.sealing {
            Some(sealing) => sealing,
            None => &stages,
        };
        let prepared = drivers::prepare(
            self.source,
            self.binding,
            self.run,
            filtered,
            carry,
            sink,
            Limits::default(),
        )
        .map_err(|e| e.to_string())?
        .with_exclusions(exclusions.version().clone());
        totals.polls += 1;
        if let Some(length) = prepared.material_len() {
            totals.materials += 1;
            totals.material_bytes += length;
        }
        let mut first = true;
        for batch in prepared.batches().map_err(|e| e.to_string())? {
            let mut batch = batch.map_err(|e| e.to_string())?;
            for record in &mut batch.records {
                if let sinex_model::RecordOutcome::Events { events, .. } = record {
                    for event in events {
                        event.class = event.class.max(self.class);
                    }
                }
            }
            if let Some(sealing) = self.sealing {
                sealing.seal(std::slice::from_mut(&mut batch))?;
                if stored && let BatchKind::Source(p) = &mut batch.kind {
                    p.material_keys.clear();
                }
            }
            (self.check)()?;
            if let Some(authority) = self.authority {
                authority.check()?;
            }
            totals.batches += 1;
            totals.events_offered += batch.event_count();
            let started = Instant::now();
            let out = if first && !stored {
                let BatchKind::Source(progress) = &mut batch.kind else {
                    return Err("native preparation did not produce source progress".into());
                };
                if let Some(p) = &frozen {
                    **progress = p
                        .first_progress(progress.cursor.clone(), progress.carry.clone())
                        .map_err(|e| e.to_string())?;
                    batch.archive = p.first_archive();
                }
                let id = progress.material.as_ref().map(|m| m.id);
                if let Some(material) = id {
                    let (start_cursor, end_cursor) = endpoints
                        .clone()
                        .unwrap_or((cursor.clone(), progress.cursor.clone()));
                    let pending = frozen.clone().unwrap_or(Pending {
                        material,
                        source: self.source.spec().name.into(),
                        binding: self.binding.into(),
                        progress: (**progress).clone(),
                        removed: native_materials::removal_plan(&batch.archive)
                            .map_err(|e| e.to_string())?,
                        cited: Vec::new(),
                        base_cursor: cursor.clone(),
                        base_carry: base_carry.clone(),
                        start_cursor,
                        end_cursor,
                        state: native_materials::State::Reserved,
                    });
                    let key = self
                        .sealing
                        .and_then(|s| progress.material.as_ref().map(|m| s.key(&m.blob)))
                        .transpose()?;
                    extra_stages.extend(stages.take()?.into_iter().map(NativeStage::Plain));
                    if let Some(sealing) = self.sealing {
                        extra_stages
                            .extend(sealing.take_stages()?.into_iter().map(NativeStage::Sealed));
                    }
                    let permit = self
                        .writer
                        .reserve_native(Priority::Bulk)
                        .await
                        .map_err(|e| e.to_string())?;
                    let publication = Publication {
                        pending,
                        stages: std::mem::take(&mut extra_stages),
                        verification: Verification {
                            key,
                            authority: self.authority.cloned(),
                            check: self.check.clone(),
                        },
                        blobs: self.blobs.clone(),
                        batch,
                        permit,
                        guard: guard.take().unwrap(),
                    };
                    publication
                        .start()
                        .await
                        .map_err(|e| e.to_string())?
                        .map_err(|e| e.to_string())?
                } else {
                    self.writer
                        .submit_checked(Priority::Bulk, batch, self.queued_check())
                        .await
                        .map_err(|e| e.to_string())?
                }
            } else {
                self.writer
                    .submit_checked(Priority::Bulk, batch, self.queued_check())
                    .await
                    .map_err(|e| e.to_string())?
            };
            totals.admit_time += started.elapsed();
            totals.add_outcome(out);
            first = false;
        }
        Ok(())
    }
}

/// A driver that offers a material already in the store (a restart before all
/// of it was admitted) reads it from the store, not from the source again.
/// True when it did.
async fn reopen_stored(
    conn: &mut PgConnection,
    blobs: &sinex_store::Blobs,
    source: &dyn Source,
    acquired: &mut drivers::Acquired,
    sealing: Option<&crate::one_shot::SealingSink>,
    authority: Option<&crate::sealed_access::Authority>,
) -> Result<bool, String> {
    let Some(material) = &mut acquired.material else {
        return Ok(false);
    };
    let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM raw.materials WHERE id=$1)")
        .bind(material.id.0)
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| e.to_string())?;
    if !exists {
        return Ok(false);
    }
    let opened = crate::retained::open(conn, blobs, material.id, authority).await?;
    if opened.reference.parser != source.spec().parser
        || opened.reference.parser_version != source.spec().parser_version
    {
        return Err("a stored material was read by another parser version".into());
    }
    if let (Some(sealing), Some(key)) = (sealing, &opened.key) {
        sealing.restore_current(material.id, opened.reference.blob.clone(), key.clone())?;
    }
    material.stored = Some(drivers::StoredBytes::FramedReader(opened.records()));
    material.bytes.clear();
    material.manifest = opened.reference.manifest.clone();
    Ok(true)
}
