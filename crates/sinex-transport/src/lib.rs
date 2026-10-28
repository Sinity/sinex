//! The filesystem side of the producer protocol, shared by the daemon and
//! outside producers: where a producer leaves a frame's bytes. The wire
//! types stay in sinex-protocol.
pub mod frame_bytes;
