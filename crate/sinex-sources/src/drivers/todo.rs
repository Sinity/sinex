//! Drivers filled by the Stage 2 drivers lane (architecture §5.3). Each
//! returns `Acquired` like the built drivers and needs no change to the drain.
use super::{Acquired, DriverError};
use serde_json::Value;

/// Screen and audio capture segments as parts with segment manifests (runs in `sinex-session`).
pub struct Media;
impl Media {
    pub fn poll(&self, _cursor: &Value) -> Result<Acquired, DriverError> {
        todo!("architecture §5.3 Media")
    }
}
