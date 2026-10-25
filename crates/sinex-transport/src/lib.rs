//! The filesystem side of the producer protocol, shared by the daemon and
//! outside producers: where a producer leaves a frame's bytes, and the
//! private-mode switch. The wire types stay in sinex-protocol.
pub mod private_mode;
pub mod spool;
