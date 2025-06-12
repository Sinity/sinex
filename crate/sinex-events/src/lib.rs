pub mod filesystem;
pub mod terminal;
pub mod window_manager;
pub mod atuin;
pub mod shell_history;
pub mod asciinema;
pub mod scrollback;

// Re-export all event types
pub use filesystem::{FileCreated, FileModified, FileDeleted};
pub use terminal::CommandExecuted;
pub use window_manager::{
    WindowFocused, WindowOpened, WindowClosed, WindowMoved,
    WorkspaceChanged, MonitorFocused, StateSnapshot
};
pub use atuin::CommandExecutedAtuin;
pub use shell_history::ShellHistoryCommand;
pub use asciinema::{AsciinemaSessionStarted, AsciinemaSessionEnded};
pub use scrollback::{TerminalScrollbackCaptured, CommandOutputCaptured};