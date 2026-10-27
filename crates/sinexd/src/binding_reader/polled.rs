//! A polled reader keeps its state when its owned blocking poll returns.
use serde_json::Value;
use sinex_source_core::{Reader, StoredMaterial, drivers};
use std::sync::{Arc, Mutex};

pub(crate) struct PolledReader(Arc<Mutex<Box<dyn Reader>>>);

impl PolledReader {
    pub(super) fn new(reader: Box<dyn Reader>) -> Self {
        Self(Arc::new(Mutex::new(reader)))
    }

    pub(crate) async fn acquire(&self, cursor: Value) -> Result<drivers::Acquired, String> {
        let reader = self.0.clone();
        crate::intake::blocking::run(crate::writer::Priority::Bulk, move || {
            reader
                .lock()
                .map_err(|e| e.to_string())?
                .poll(&cursor)
                .map_err(|e| e.to_string())
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
    fn restore_memory(&mut self, memory: &[u8]) -> Result<(), drivers::DriverError> {
        self.0.lock().expect("reader state").restore_memory(memory)
    }
    fn restored(&mut self) -> Result<(), drivers::DriverError> {
        self.0.lock().expect("reader state").restored()
    }
    fn poll(&mut self, cursor: &Value) -> Result<drivers::Acquired, drivers::DriverError> {
        self.0.lock().expect("reader state").poll(cursor)
    }
    fn remove_passed(&self, cursor: &Value) -> Result<usize, drivers::DriverError> {
        self.0.lock().expect("reader state").remove_passed(cursor)
    }
}

#[cfg(test)]
mod tests;
