//! Producer transport shared by the daemon and outside producers: staged frame
//! bytes, verified framed RPC replies and idempotent admission. Wire types stay
//! in sinex-protocol; capture journals stay with their owning product.
pub mod frame_bytes;

pub mod replies;

pub mod producer;
