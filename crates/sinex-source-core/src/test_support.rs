//! Helpers for reading material produced by drivers in tests.
use crate::drivers::{NewMaterial, StoredBytes};

/// The bytes a driver supplied, including framed in-memory material.
pub fn body(material: &NewMaterial) -> &[u8] {
    match &material.stored {
        Some(StoredBytes::FramedMemory(bytes)) => bytes,
        _ => &material.bytes,
    }
}
