//! Drivers filled by the Stage 2 drivers lane (architecture §5.3). Each
//! returns `Acquired` like the built drivers and needs no change to the drain.
use super::{Acquired, DriverError};
use serde_json::Value;

/// An export as received (Takeout, mbox, page archives), with an archive index;
/// records are addressed in the member stream.
pub struct ExportArchive;
impl ExportArchive {
    pub fn poll(&self, _cursor: &Value) -> Result<Acquired, DriverError> {
        todo!(
            "architecture §5.3 ExportArchive: archive hash, member index, ZIP/tar/gzip with restart points"
        )
    }
}

/// A directory walk: per-file classification; completed scans record what vanished.
pub struct DirSnapshot;
impl DirSnapshot {
    pub fn poll(&self, _cursor: &Value) -> Result<Acquired, DriverError> {
        todo!("architecture §5.3 DirSnapshot")
    }
}

/// Screen and audio capture segments as parts with segment manifests (runs in `sinex-session`).
pub struct Media;
impl Media {
    pub fn poll(&self, _cursor: &Value) -> Result<Acquired, DriverError> {
        todo!("architecture §5.3 Media")
    }
}
