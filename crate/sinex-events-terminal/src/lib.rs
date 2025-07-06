pub mod asciinema;
pub mod atuin;
pub mod kitty;
pub mod scrollback;
pub mod shell_history;
pub mod terminal;

// Re-export terminal event types and payloads
pub use asciinema::{AsciinemaSessionEnded, AsciinemaSessionStarted, AsciinemaSessionStartedPayload, AsciinemaSessionEndedPayload};
pub use atuin::{CommandExecutedAtuin, CommandExecutedAtuinPayload};
pub use kitty::{
    KittyCommandExecuted, KittyCommandCompleted, KittyScrollbackIncremental, KittyTabCreated, 
    KittyTabFocused, KittyTabClosed, KittyProcessChanged,
    KittyCommandExecutedPayload, KittyCommandCompletedPayload, KittyScrollbackIncrementalPayload,
    KittyTabCreatedPayload, KittyTabFocusedPayload, KittyTabClosedPayload, KittyProcessChangedPayload,
};
pub use scrollback::{CommandOutputCaptured, TerminalScrollbackCaptured, CommandOutputCapturedPayload, TerminalScrollbackCapturedPayload};
pub use shell_history::{ShellHistoryCommand, ShellHistoryCommandPayload};
pub use terminal::{CommandExecuted, CommandExecutedPayload};

use sinex_core::register_events;

// Register all terminal event types using the macro
register_events! {
    "recording.started" => (shell.recording, AsciinemaSessionStartedPayload),
    "recording.ended" => (shell.recording, AsciinemaSessionEndedPayload),
    "command.imported" => (shell.atuin, CommandExecutedAtuinPayload),
    "command.executed" => (shell.kitty, KittyCommandExecutedPayload),
    "command.completed" => (shell.kitty, KittyCommandCompletedPayload),
    "scrollback.incremental" => (shell.kitty, KittyScrollbackIncrementalPayload),
    "tab.created" => (shell.kitty, KittyTabCreatedPayload),
    "tab.focused" => (shell.kitty, KittyTabFocusedPayload),
    "tab.closed" => (shell.kitty, KittyTabClosedPayload),
    "process.changed" => (shell.kitty, KittyProcessChangedPayload),
    "command.output" => (shell.scrollback, CommandOutputCapturedPayload),
    "scrollback.full" => (shell.scrollback, TerminalScrollbackCapturedPayload),
    "command.imported" => (shell.history, ShellHistoryCommandPayload),
}
