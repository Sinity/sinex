//! Terminal source units.
//!
//! Wave B: all five adapter-backed terminal source units are registered here.
//! `terminal.monitor` (fire-once startup event) is wired in `monitor.rs`.

pub mod atuin_history;
pub mod bash_history;
pub mod fish_history;
pub mod monitor;
pub mod text_history;
pub mod zsh_history;
