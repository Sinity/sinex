pub mod asciinema;
pub mod atuin;
pub mod scrollback;
pub mod shell_history;
pub mod terminal;

// Re-export terminal event types
pub use asciinema::{AsciinemaSessionEnded, AsciinemaSessionStarted};
pub use atuin::CommandExecutedAtuin;
pub use scrollback::{CommandOutputCaptured, TerminalScrollbackCaptured};
pub use shell_history::ShellHistoryCommand;
pub use terminal::CommandExecuted;
