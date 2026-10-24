//! Compact producer-byte custody types are shared with admission.
pub use sinex_model::producer_bytes::*;

/// Preserve the producer's existing typed frame serialization. The declared
/// serde_json preserve_order feature retains payload object order through a
/// frozen-artifact round trip; ingress reading hashes depend on that order.
/// Material descriptor commitments independently sort their metadata objects.
pub fn encode_frame(frame: &crate::Ingress) -> Vec<u8> {
    serde_json::to_vec(frame).expect("frame serializes")
}
