//! Wave B production-path obligation tests for terminal source contracts.
//!
//! Source contracts covered:
//! - `terminal.atuin-history`  (`SqliteRowAdapter` + `AtuinHistoryParser`)
//! - `terminal.bash-history`   (`AppendOnlyFileAdapter` + `BashHistoryParser`)
//! - `terminal.zsh-history`    (`AppendOnlyFileAdapter` + `ZshHistoryParser`)
//! - `terminal.text-history`   (`AppendOnlyFileAdapter` + `TextHistoryParser`)
//! - `terminal.fish-history`   (`SqliteRowAdapter` + `FishHistoryParser`)
//! - `terminal.kitty-osc-live` (`UnixSocketStreamAdapter` + `KittyOscParser`)
//!
//! SQLite-backed fixtures (atuin, fish) pass pre-serialised JSON rows
//! as fixture bytes, matching what `SqliteRowAdapter` would produce.
//! `AppendOnlyFile` fixtures (bash, zsh, text) pass plain-text lines.

#[cfg(test)]
#[path = "terminal_test.rs"]
mod tests;
