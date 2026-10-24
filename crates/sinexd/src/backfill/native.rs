//! One scoped native acquisition owns preparation, publication and first admission.
use super::*;
use crate::source_excision::{
    AcquisitionGuard, AcquisitionPolicy, Filtered,
    publication::{ImageVerification, NativeStage, Publication, StagingSink, Verification},
};
use sinex_model::{BatchKind, Class};
use sinex_store::native_materials::{self, Pending, PendingImage};
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
    pub async fn submit(
        &self,
        guard: AcquisitionGuard,
        policy: AcquisitionPolicy,
        cursor: serde_json::Value,
        base_carry: serde_json::Value,
        carry: serde_json::Value,
        acquired: drivers::Acquired,
        totals: &mut Totals,
    ) -> Result<Option<sinex_model::SqliteImagePublication>, String> {
        let mut guard = Some(guard);
        let result = self
            .submit_owned(
                &mut guard, policy, cursor, base_carry, carry, acquired, totals,
            )
            .await;
        if let Some(guard) = guard {
            guard.release().await?;
        }
        result
    }
    async fn submit_owned(
        &self,
        guard: &mut Option<AcquisitionGuard>,
        policy: AcquisitionPolicy,
        cursor: serde_json::Value,
        base_carry: serde_json::Value,
        carry: serde_json::Value,
        mut acquired: drivers::Acquired,
        totals: &mut Totals,
    ) -> Result<Option<sinex_model::SqliteImagePublication>, String> {
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
        let filtered = if let Some(p) = &frozen {
            if p.progress.excision_basis.as_ref() != Some(&policy.basis) {
                return Err("pending native exclusion basis changed; authoritative reconciliation is required".into());
            }
            if p.progress.material.is_none() {
                if acquired.material.is_some() {
                    return Err("provider cannot replace a pending image-only publication".into());
                }
                Filtered {
                    acquired,
                    erased: None,
                    publication: None,
                    registered: false,
                    bodyless: false,
                }
            } else {
                let opened = crate::retained::open_pending(self.blobs, p, self.authority)?;
                let material = acquired.material.as_mut().ok_or(
                    "pending native material was not restored before provider acquisition",
                )?;
                if material.id != p.material {
                    return Err("provider cannot replace the frozen pending native version".into());
                }
                if self.sealing.is_some()
                    != self
                        .blobs
                        .is_sealed(&opened.reference.blob)
                        .map_err(|e| e.to_string())?
                {
                    return Err(
                        "pending native class differs from current acquisition class".into(),
                    );
                }
                if let Some(sealing) = self.sealing {
                    let key = p
                        .progress
                        .material_keys
                        .iter()
                        .find(|key| key.material == p.material)
                        .ok_or("pending native data capability is unavailable")?;
                    sealing.restore_reserved(
                        opened.reference.blob.clone(),
                        key,
                        self.authority
                            .ok_or("pending sealed material requires scoped authority")?,
                    )?;
                }
                material.stored = Some(drivers::StoredBytes::FramedReader(opened.records()));
                material.bytes.clear();
                material.manifest = opened.reference.manifest.clone();
                let erased = opened.transitions(0)?;
                Filtered {
                    acquired,
                    erased,
                    publication: p.progress.erased_replay.clone(),
                    registered: false,
                    bodyless: false,
                }
            }
        } else {
            policy
                .reconcile(
                    guard.as_mut().unwrap().connection(),
                    self.blobs,
                    self.source,
                    acquired,
                    &cursor,
                    self.sealing,
                )
                .await?
        };
        let registered = filtered.registered;
        if registered
            && self.source.spec().driver == sinex_sources::Driver::AppendFile
            && filtered.erased.is_some()
        {
            return Err(
                "erased append capture requires scoped cursor restoration before progress".into(),
            );
        }
        let mut filtered = policy.filter(
            self.blobs,
            self.source,
            filtered,
            &carry,
            self.class,
            self.sealing,
            &stages,
        )?;
        let endpoints = filtered
            .acquired
            .material
            .as_ref()
            .map(|m| {
                let length = crate::source_excision::byte_length(&m.record_bytes())?;
                let end =
                    usize::try_from(length).map_err(|_| "native material exceeds address space")?;
                Ok::<_, String>((
                    (m.cursor_at)(0).map_err(|e| e.to_string())?,
                    (m.cursor_at)(end).map_err(|e| e.to_string())?,
                ))
            })
            .transpose()?;
        let mut image = None;
        let mut image_plan = None;
        let mut image_ready = None;
        let mut extra_stages = Vec::new();
        if let Some(p) = &frozen {
            filtered.acquired.image = None;
            if let Some(plan) = &p.image {
                let PendingImage::AwaitingCosts(expected) = plan;
                let previous = match expected.metadata.previous {
                    Some(id) => {
                        let material: Option<uuid::Uuid> = sqlx::query_scalar(
                            "SELECT material_id FROM raw.sqlite_images WHERE revision_id=$1",
                        )
                        .bind(id.0)
                        .fetch_optional(guard.as_mut().unwrap().connection())
                        .await
                        .map_err(|e| e.to_string())?;
                        match material {
                            Some(material) => sinex_store::sqlite_images::load(
                                guard.as_mut().unwrap().connection(),
                                sinex_model::MaterialId(material),
                            )
                            .await
                            .map_err(|e| e.to_string())?
                            .map(|(m, _, _)| m),
                            None => return Err("pending image predecessor is unavailable".into()),
                        }
                    }
                    None => None,
                };
                image = Some(ImageVerification::Retained {
                    blobs: self.blobs.clone(),
                    plan: plan.clone(),
                    previous,
                });
                image_plan = Some(plan.clone());
            } else {
                image_ready = p.progress.image.clone();
            }
        } else if let Some(view) = filtered.acquired.image.take() {
            if let Some(ready) = view.stored().map_err(|e| e.to_string())? {
                image_ready = Some(ready);
            } else {
                let prepared = if view.metadata.class == Class::Sealed {
                    let recipient = self
                        .sealing
                        .ok_or("sealed image requires its configured recipient")?
                        .recipient();
                    let (prepared, stage) = crate::sqlite_images::prepare_sealed(
                        self.blobs,
                        view,
                        self.source.spec().parser,
                        self.source.spec().parser_version,
                        recipient,
                    )?;
                    extra_stages.push(NativeStage::Sealed(stage));
                    prepared
                } else {
                    let (prepared, stage) = crate::sqlite_images::prepare_plain(
                        self.blobs,
                        view,
                        self.source.spec().parser,
                        self.source.spec().parser_version,
                    )?;
                    extra_stages.push(NativeStage::Plain(stage));
                    prepared
                };
                image_plan = Some(prepared.plan()?);
                image = Some(ImageVerification::Pinned(prepared));
            }
        }
        let image_revision = image_ready
            .as_ref()
            .map(|i| i.metadata.revision)
            .or_else(|| {
                image_plan
                    .as_ref()
                    .map(|PendingImage::AwaitingCosts(i)| i.metadata.revision)
            });
        let sink: &dyn drivers::MaterialSink = match self.sealing {
            Some(sealing) => sealing,
            None => &stages,
        };
        let mut prepared = drivers::prepare(
            self.source,
            self.binding,
            self.run,
            filtered.acquired,
            carry,
            sink,
            Limits::default(),
        )
        .map_err(|e| e.to_string())?
        .with_excision_basis(policy.basis.clone());
        if let Some(revision) = image_revision {
            prepared = prepared.with_image_revision(revision);
        }
        if let Some(erased) = filtered.erased {
            prepared = prepared
                .with_erased(erased, filtered.publication)
                .map_err(|e| e.to_string())?;
        }
        totals.polls += 1;
        if let Some(length) = prepared.material_len() {
            totals.materials += 1;
            totals.material_bytes += length;
        }
        let mut first = true;
        let mut published_image = None;
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
                if registered && let BatchKind::Source(p) = &mut batch.kind {
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
            let out = if first && !registered {
                let BatchKind::Source(progress) = &mut batch.kind else {
                    return Err("native preparation did not produce source progress".into());
                };
                if image_ready.is_some() {
                    progress.image = image_ready.take();
                }
                if let Some(p) = &frozen {
                    **progress = p
                        .first_progress(progress.cursor.clone(), progress.carry.clone())
                        .map_err(|e| e.to_string())?;
                    batch.archive = p.first_archive();
                }
                let id = progress
                    .material
                    .as_ref()
                    .map(|m| m.id)
                    .or_else(|| progress.image.as_ref().map(|i| i.material.id))
                    .or_else(|| {
                        image_plan
                            .as_ref()
                            .map(|PendingImage::AwaitingCosts(i)| i.material.id)
                    });
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
                        image: image_plan.take(),
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
                        image: image.take(),
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
                    let published = publication
                        .start()
                        .await
                        .map_err(|e| e.to_string())?
                        .map_err(|e| e.to_string())?;
                    published_image = published.image;
                    published.outcome
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
        if let Some(image) = &published_image {
            totals.physical_images += 1;
            totals.physical_evidence_bytes += image.metadata.evidence_bytes;
            totals.physical_database_bytes += image.metadata.image_bytes;
            totals.physical_map_bytes += image.metadata.map_bytes;
        }
        Ok(published_image)
    }
}
