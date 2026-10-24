//! Compact producer-byte custody types are shared with admission.
pub use sinex_model::producer_bytes::*;

pub mod spool;

/// Encode a typed indirect frame consistently across serde map configurations.
/// Material body bytes are independent and never pass through this encoding.
pub fn encode_frame(frame: &crate::Ingress) -> Vec<u8> {
    let mut value = serde_json::to_value(frame).expect("frame serializes");
    value.sort_all_objects();
    serde_json::to_vec(&value).expect("canonical frame serializes")
}
