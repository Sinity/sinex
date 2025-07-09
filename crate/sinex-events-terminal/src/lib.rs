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
    KittyCommandCompleted, KittyScrollbackIncremental, KittyTabCreated, 
    KittyTabFocused, KittyTabClosed, KittyProcessChanged,
    KittyCommandCompletedPayload, KittyScrollbackIncrementalPayload,
    KittyTabCreatedPayload, KittyTabFocusedPayload, KittyTabClosedPayload, KittyProcessChangedPayload,
    KittyEventSource, KittyConfig, KittyProcessInfo,
};
pub use scrollback::{CommandOutputCaptured, TerminalScrollbackCaptured, CommandOutputCapturedPayload, TerminalScrollbackCapturedPayload};
pub use shell_history::{ShellHistoryCommand, ShellHistoryCommandPayload};
pub use terminal::{CommandExecuted, CommandExecutedPayload};

use sinex_core::register_events;

// Register all terminal event types using the macro
register_events! {
    // Terminal recording sessions
    "session.started" => (shell.recording, AsciinemaSessionStartedPayload),
    "session.ended" => (shell.recording, AsciinemaSessionEndedPayload),
    
    // Command execution (rich metadata from Atuin)
    "command.executed" => (shell.atuin, CommandExecutedAtuinPayload),
    
    // Command execution (discovered from history files)
    "command.hist" => (shell.history, ShellHistoryCommandPayload),
    
    // Real-time terminal events from Kitty
    "command.completed" => (shell.kitty, KittyCommandCompletedPayload),
    "tab.created" => (shell.kitty, KittyTabCreatedPayload),
    "tab.focused" => (shell.kitty, KittyTabFocusedPayload),
    "tab.closed" => (shell.kitty, KittyTabClosedPayload),
    "process.changed" => (shell.kitty, KittyProcessChangedPayload),
    "content.streamed" => (shell.kitty, KittyScrollbackIncrementalPayload),
    
    // Terminal content capture
    "output.captured" => (shell.scrollback, CommandOutputCapturedPayload),
    "content.captured" => (shell.scrollback, TerminalScrollbackCapturedPayload),
}
