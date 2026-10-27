//! One acquisition of a binding the daemon reads itself: bytes stored, then
//! the batches admitted.
use super::*;

use std::sync::Arc;

pub(super) struct Pipeline<'a> {
    pub writer: &'a writer::WriterHandle,
    pub source: &'a dyn Source,
    pub binding: &'a str,
    pub run: RunId,
    pub material: &'a crate::sealing::Materials,
    pub check: Arc<dyn Fn() -> Result<(), String> + Send + Sync>,
}
impl Pipeline<'_> {
    fn queued_check(&self) -> crate::writer::AdmissionCheck {
        self.material.admission_check(self.check.clone())
    }

    pub(crate) async fn submit(
        &self,
        conn: &mut PgConnection,
        carry: serde_json::Value,
        mut acquired: drivers::Acquired,
        totals: &mut Totals,
    ) -> Result<(), String> {
        (self.check)()?;
        let stored = self
            .material
            .reopen(conn, self.source, &mut acquired)
            .await?;
        let prepared = drivers::prepare(
            self.source,
            self.binding,
            self.run,
            acquired,
            carry,
            self.material.sink(),
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
            self.material.prepare_batch(&mut batch, stored)?;
            (self.check)()?;
            self.material.check()?;
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
