//! Terminal source contracts.
//!
//! Wave B: all five adapter-backed terminal source contracts are registered here.
//! `terminal.monitor` (fire-once startup event) is wired in `monitor.rs`.

pub mod asciinema;
pub mod atuin_history;
pub mod bash_history;
pub mod fish_history;
pub mod kitty_osc;
pub mod monitor;
pub mod text_history;
pub mod zsh_history;
