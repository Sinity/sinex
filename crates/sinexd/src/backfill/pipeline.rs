//! One acquisition of a binding the daemon reads itself: bytes stored, then
//! the batches admitted.
use super::*;
use sinex_model::{BatchKind, Class};
use std::sync::Arc;

pub(super) struct Pipeline<'a> {
    pub writer: &'a writer::WriterHandle,
    pub blobs: &'a sinex_store::Blobs,
    pub source: &'a dyn Source,
    pub binding: &'a str,
    pub run: RunId,
    pub class: Class,
    pub sealing: Option<&'a crate::material_sink::SealingSink>,
    pub unlock: Option<&'a crate::sealed_access::Unlock>,
    pub check: Arc<dyn Fn() -> Result<(), String> + Send + Sync>,
}
impl Pipeline<'_> {
    fn queued_check(&self) -> crate::writer::AdmissionCheck {
        let check = self.check.clone();
        let unlock = self.unlock.cloned();
        Arc::new(move || {
            check()?;
            if let Some(unlock) = &unlock {
                unlock.check()?;
            }
            Ok(())
        })
    }

    pub async fn submit(
        &self,
        conn: &mut PgConnection,
        carry: serde_json::Value,
        mut acquired: drivers::Acquired,
        totals: &mut Totals,
    ) -> Result<(), String> {
        (self.check)()?;
        let stored = reopen_stored(
            conn,
            self.blobs,
            self.source,
            &mut acquired,
            self.sealing,
            self.unlock,
        )
        .await?;
        let plain = crate::material_sink::Sink(self.blobs.clone());
        let sink: &dyn drivers::MaterialSink = match self.sealing {
            Some(sealing) => sealing,
            None => &plain,
        };
        let prepared = drivers::prepare(
            self.source,
            self.binding,
            self.run,
            acquired,
            carry,
            sink,
            Limits::default(),
        )
        .map_err(|e| e.to_string())?;
        totals.polls += 1;
        if let Some(length) = prepared.material_len() {
            totals.materials += 1;
            totals.material_bytes += length;
        }
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
                    // The stored material's key row exists already.
                    p.data_keys.clear();
                }
            }
            (self.check)()?;
            if let Some(unlock) = self.unlock {
                unlock.check()?;
            }
            totals.batches += 1;
            totals.events_offered += batch.event_count();
            let started = Instant::now();
            let outcome = self
                .writer
                .submit_checked(Priority::Bulk, batch, self.queued_check())
                .await
                .map_err(|e| e.to_string())?;
            totals.admit_time += started.elapsed();
            totals.add_outcome(outcome);
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
    sealing: Option<&crate::material_sink::SealingSink>,
    unlock: Option<&crate::sealed_access::Unlock>,
) -> Result<bool, String> {
    let Some(material) = &mut acquired.material else {
        return Ok(false);
    };
    let registered = sinex_store::material_bytes::registered(conn, &[material.id.0])
        .await
        .map_err(|e| e.to_string())?;
    if registered.is_empty() {
        return Ok(false);
    }
    let opened = crate::stored::open(conn, blobs, material.id, unlock).await?;
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
