pub mod blob;
pub mod event;
pub mod source_material;

// Explicit re-exports to avoid ambiguity
pub use blob::BlobRecord;
pub use event::{Event, EventRecord, SourceMaterial as EventSourceMaterial};
pub use source_material::{Blob as SourceMaterialBlob, SourceMaterial, SourceMaterialRecord};
