//! Filesystem transport shared by the daemon and outside producers. Wire/data
//! types and pure encoding stay in sinex-protocol; these helpers own private
//! staging and the capture switch, without queue or source-controller policy.
pub mod private_mode;
pub mod spool;
