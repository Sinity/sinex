//! A polled reader keeps its state when its owned blocking poll returns.
use crate::{Error, Result};
use serde_json::Value;
use sinex_source_core::{Reader, StoredMaterial, drivers};
use std::sync::{Arc, Mutex};

pub(crate) struct PolledReader(Arc<Mutex<Box<dyn Reader>>>);

impl PolledReader {
    pub(super) fn new(reader: Box<dyn Reader>) -> Self {
        Self(Arc::new(Mutex::new(reader)))
    }

    pub(crate) async fn acquire(
        &self,
        cursor: Value,
        priority: crate::writer::Priority,
    ) -> Result<(drivers::Acquired, bool)> {
        let reader = self.0.clone();
        crate::submit::blocking::run(priority, move || {
            let mut reader = reader
                .lock()
                .map_err(|_| Error::internal("the reader stopped in a panic"))?;
            let acquired = reader.poll(&cursor)?;
            let more = reader.has_more();
            Ok::<_, Error>((acquired, more))
        })
        .await?
    }

    pub(super) fn as_mut(&mut self) -> &mut dyn Reader {
        self
    }
}

impl Reader for PolledReader {
    fn history_from(&self, cursor: &Value) -> Option<sinex_model::MaterialId> {
        self.0.lock().expect("reader state").history_from(cursor)
    }
    fn restore(
        &mut self,
        cursor: &Value,
        material: StoredMaterial,
    ) -> Result<bool, drivers::DriverError> {
        self.0
            .lock()
            .expect("reader state")
            .restore(cursor, material)
    }
    fn restore_memory(
        &mut self,
        memory: &[u8],
        memory_part: Option<&sinex_model::MaterialRef>,
        admitted: Option<u64>,
    ) -> Result<bool, drivers::DriverError> {
        self.0
            .lock()
            .expect("reader state")
            .restore_memory(memory, memory_part, admitted)
    }
    fn restored(&mut self) -> Result<(), drivers::DriverError> {
        self.0.lock().expect("reader state").restored()
    }
    fn poll(&mut self, cursor: &Value) -> Result<drivers::Acquired, drivers::DriverError> {
        self.0.lock().expect("reader state").poll(cursor)
    }
    fn has_more(&self) -> bool {
        self.0.lock().expect("reader state").has_more()
    }
    fn committed(&mut self, cursor: &Value) -> Result<usize, drivers::DriverError> {
        self.0.lock().expect("reader state").committed(cursor)
    }
}

#[cfg(test)]
mod tests;
