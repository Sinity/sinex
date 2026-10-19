//! Google Takeout products, each its own source over the same archives
//! (`spec/port-lynchpin-sources.md` §13.3). Chrome history is
//! `takeout_chrome`, mail is `mail`, calendars are `calendar`.
pub mod activity;
pub mod cells;
pub mod keep;
pub mod purchases;
pub mod tasks;
