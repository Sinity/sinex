//! Authored records and their cited bytes use the same publication owner as capture.
use super::*;
use crate::excision::exclusions::Exclusions;
use crate::writer::{Priority, WriterHandle};
use serde_json::Value;
use sinex_model::{Class, MaterialId, ReadingOrder, RunId};
use sinex_sources::{Limits, Source, drivers};
use sinex_store::{Outcome, bindings, positions};
use std::sync::Arc;

type R<T> = Result<T, String>;
fn error(e: impl std::fmt::Display) -> String {
    e.to_string()
}

pub(crate) struct AuthoredOutcome {
    pub outcome: Outcome,
    pub material: Option<MaterialId>,
    pub recovered: Vec<MaterialId>,
}

/// Recovery commits an earlier request separately; its identity/result never
/// substitutes for the fresh request supplied to `submit_authored`.
pub(crate) async fn resume_authored(
    writer: &WriterHandle,
    blobs: &Blobs,
    source: Arc<dyn Source>,
    binding: &str,
) -> R<Vec<MaterialId>> {
    let mut guard = Some(writer.source_guard(source.spec().name).await?);
    let result = resume_owned(writer, blobs, source, binding, &mut guard).await;
    finish(guard, result).await
}

async fn resume_owned(
    writer: &WriterHandle,
    blobs: &Blobs,
    source: Arc<dyn Source>,
    binding: &str,
    guard: &mut Option<AcquisitionGuard>,
) -> R<Vec<MaterialId>> {
    let loaded = async {
        let ids = native_materials::binding_pending(guard.as_mut().unwrap().connection(), binding)
            .await
            .map_err(error)?;
        if ids.len() > 1 {
            return Err("authored binding has multiple pending publications".into());
        }
        match ids.first() {
            None => Ok(None),
            Some(id) => native_materials::load(guard.as_mut().unwrap().connection(), *id)
                .await
                .map_err(error),
        }
    }
    .await;
    let pending = match loaded {
        Ok(Some(pending)) => pending,
        Ok(None) => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let recovery = async {
        if pending.source != source.spec().name || pending.binding != binding {
            return Err("pending authored publication has a different source or binding".into());
        }
        let exclusions =
            Exclusions::load(guard.as_mut().unwrap().connection(), source.spec().name).await?;
        if pending.progress.exclusions.as_ref() != Some(exclusions.version()) {
            return Err("the source was excised since this request; send it again".into());
        }
        let opened = crate::retained::open_pending(blobs, &pending, None)?;
        let reference = &opened.reference;
        if reference.parser != source.spec().parser
            || reference.parser_version != source.spec().parser_version
        {
            return Err("pending authored material was read by another parser version".into());
        }
        let order = reference.manifest["read_at_ns"]
            .as_i64()
            .ok_or("authored material lacks its recorded reading order")?;
        let records = opened
            .body
            .records(&reference.manifest, source.spec().framer)?;
        let end = pending.end_cursor.clone();
        let acquired = drivers::Acquired {
            input: pending.progress.input.clone(),
            observation: pending.progress.observation.clone(),
            revision: pending.progress.revision,
            material: Some(drivers::NewMaterial {
                id: reference.id,
                bytes: Vec::new(),
                stored: Some(drivers::StoredBytes::FramedReader(opened.records())),
                records,
                part_index: reference.part_index,
                previous_part: reference.previous_part,
                manifest: reference.manifest.clone(),
                reading_order: Box::new(move |_| ReadingOrder(order)),
                cursor_at: Box::new(move |_| Ok(end.clone())),
                aligned: false,
                vanished: Vec::new(),
            }),
            cursor_unchanged: pending.end_cursor.clone(),
        };
        prepare(
            source.clone(),
            binding.to_owned(),
            acquired,
            blobs.clone(),
            None,
            source.spec().class,
        )
        .await
    }
    .await;
    let (mut batch, stages) = match recovery {
        Ok(value) => value,
        Err(e) => return Err(e),
    };
    let BatchKind::Source(progress) = &mut batch.kind else {
        unreachable!()
    };
    **progress = pending
        .first_progress(progress.cursor.clone(), progress.carry.clone())
        .map_err(error)?;
    batch.archive = pending.first_archive();
    let id = pending.material;
    let permit = writer.reserve_native(Priority::Live).await.map_err(error)?;
    Publication {
        pending,
        stages,
        verification: Verification::default(),
        blobs: blobs.clone(),
        batch,
        permit,
        guard: guard.take().unwrap(),
    }
    .start()
    .await
    .map_err(error)?
    .map_err(error)?;
    Ok(vec![id])
}

pub(crate) async fn submit_authored(
    writer: &WriterHandle,
    blobs: &Blobs,
    source: Arc<dyn Source>,
    binding: &str,
    acquired: drivers::Acquired,
    companions: Vec<PreparedPlain>,
) -> R<AuthoredOutcome> {
    let mut guard = Some(writer.source_guard(source.spec().name).await?);
    let recovered = match resume_owned(writer, blobs, source.clone(), binding, &mut guard).await {
        Ok(ids) => ids,
        Err(e) => return finish(guard, Err(e)).await,
    };
    let result = async {
        if guard.is_none() {
            guard = Some(writer.source_guard(source.spec().name).await?);
        }
        submit_owned(
            writer, blobs, source, binding, acquired, companions, &mut guard,
        )
        .await
    }
    .await;
    match finish(guard, result).await {
        Ok(mut outcome) => {
            outcome.recovered = recovered;
            Ok(outcome)
        }
        Err(e) if !recovered.is_empty() => Err(format!(
            "recovered prior authored materials {recovered:?}; new request failed: {e}"
        )),
        Err(e) => Err(e),
    }
}

#[allow(clippy::too_many_arguments)]
async fn submit_owned(
    writer: &WriterHandle,
    blobs: &Blobs,
    source: Arc<dyn Source>,
    binding: &str,
    acquired: drivers::Acquired,
    companions: Vec<PreparedPlain>,
    guard: &mut Option<AcquisitionGuard>,
) -> R<AuthoredOutcome> {
    let preflight = async {
        if let Some(reason) = &acquired.observation.unavailable {
            return Err(format!("authored acquisition unavailable: {reason}"));
        }
        if !matches!(
            source.spec().driver,
            sinex_sources::Driver::ApiRequest | sinex_sources::Driver::DirSnapshot
        ) {
            return Err("this driver has no authored publication contract".into());
        }
        if let Some(material) = &acquired.material {
            let order = material.manifest["read_at_ns"]
                .as_i64()
                .ok_or("authored material lacks its recorded reading order")?;
            if material
                .records
                .iter()
                .any(|(start, _)| (material.reading_order)(*start).0 != order)
            {
                return Err("authored material has an unsupported per-record reading order".into());
            }
        }
        bindings::save(
            guard.as_mut().unwrap().connection(),
            &bindings::Stored {
                binding: binding.to_owned(),
                source: source.spec().name.into(),
                input: acquired.input.locator.clone(),
                settings: serde_json::json!({"host":acquired.input.host}),
            },
        )
        .await
        .map_err(error)?;
        let exclusions =
            Exclusions::load(guard.as_mut().unwrap().connection(), source.spec().name).await?;
        let privacy = sinex_store::privacy::load(
            guard.as_mut().unwrap().connection(),
            std::iter::once(source.spec().name),
        )
        .await
        .map_err(error)?;
        let class = source.spec().class.max(
            privacy
                .floors
                .get(source.spec().name)
                .copied()
                .unwrap_or_default(),
        );
        if class == Class::Sealed {
            return Err("authored sealed publication requires scoped storage support".into());
        }
        let base = positions::load(guard.as_mut().unwrap().connection(), binding)
            .await
            .map_err(error)?
            .unwrap_or((Value::Null, Value::Null));
        Ok((exclusions, base, class))
    }
    .await;
    let (exclusions, (base_cursor, base_carry), class) = match preflight {
        Ok(value) => value,
        Err(e) => return Err(e),
    };
    let exclusions = Arc::new(exclusions);
    let prepared = prepare(
        source.clone(),
        binding.into(),
        acquired,
        blobs.clone(),
        Some(exclusions.clone()),
        class,
    )
    .await;
    let (mut batch, mut stages) = match prepared {
        Ok(value) => value,
        Err(e) => return Err(e),
    };
    for record in &mut batch.records {
        if let sinex_model::RecordOutcome::Events { events, .. } = record {
            for event in events {
                event.class = event.class.max(class);
            }
        }
    }
    let cited = batch
        .events()
        .flat_map(|event| event.cites.iter().cloned())
        .collect::<Vec<_>>();
    let citations = native_materials::citation_set(&cited).map_err(error)?;
    exclusions.check_cited(&cited)?;
    if companions.iter().any(|stage| {
        citations
            .get(&stage.name())
            .is_some_and(|size| *size != stage.length())
    }) {
        return Err("staged companion length differs from its recorded citation".into());
    }
    stages.extend(
        companions
            .into_iter()
            .filter(|stage| citations.get(&stage.name()) == Some(&stage.length()))
            .map(NativeStage::Plain),
    );
    let BatchKind::Source(progress) = &mut batch.kind else {
        unreachable!()
    };
    progress.exclusions = Some(exclusions.version().clone());
    let material = progress.material.as_ref().map(|m| m.id);
    let permit = writer.reserve_native(Priority::Live).await.map_err(error)?;
    let outcome = if let Some(id) = material {
        let pending = native_materials::Pending {
            material: id,
            source: source.spec().name.into(),
            binding: binding.into(),
            progress: (**progress).clone(),
            removed: native_materials::removal_plan(&batch.archive).map_err(error)?,
            cited,
            base_cursor,
            base_carry,
            start_cursor: Value::Null,
            end_cursor: progress.cursor.clone(),
            state: native_materials::State::Reserved,
        };
        Publication {
            pending,
            stages,
            verification: Verification::default(),
            blobs: blobs.clone(),
            batch,
            permit,
            guard: guard.take().unwrap(),
        }
        .start()
        .await
        .map_err(error)?
        .map_err(error)?
    } else {
        permit
            .admit(batch, guard.take().unwrap(), Arc::new(|| Ok(())))
            .await
            .map_err(error)?
    };
    Ok(AuthoredOutcome {
        outcome,
        material,
        recovered: Vec::new(),
    })
}

async fn prepare(
    source: Arc<dyn Source>,
    binding: String,
    acquired: drivers::Acquired,
    blobs: Blobs,
    exclusions: Option<Arc<Exclusions>>,
    class: Class,
) -> R<(Batch, Vec<NativeStage>)> {
    tokio::task::spawn_blocking(move || {
        let sink = StagingSink::new(blobs.clone());
        let acquired = match exclusions {
            Some(exclusions) => {
                exclusions.filter(source.as_ref(), acquired, &Value::Null, class, false)?
            }
            None => acquired,
        };
        let prepared = drivers::prepare(
            source.as_ref(),
            &binding,
            RunId::new(),
            acquired,
            Value::Null,
            &sink,
            Limits {
                max_events: usize::MAX,
                max_bytes: usize::MAX,
            },
        )
        .map_err(error)?;
        let mut batches = prepared.batches().map_err(error)?;
        let batch = batches
            .next()
            .ok_or("authored publication produced no batch")?
            .map_err(error)?;
        if batches.next().is_some() {
            return Err("authored publication must fit one atomic admission".into());
        }
        if matches!(&batch.kind, BatchKind::Source(progress) if !progress.carry.is_null()) {
            return Err("authored publication requires stateless record interpretation".into());
        }
        if batch
            .records
            .iter()
            .any(|record| matches!(record, sinex_model::RecordOutcome::Failure { .. }))
        {
            return Err("authored records failed validation before publication".into());
        }
        Ok((
            batch,
            sink.take()?.into_iter().map(NativeStage::Plain).collect(),
        ))
    })
    .await
    .map_err(error)?
}

async fn finish<T>(guard: Option<AcquisitionGuard>, result: R<T>) -> R<T> {
    if let Some(guard) = guard {
        guard.release().await?;
    }
    result
}
