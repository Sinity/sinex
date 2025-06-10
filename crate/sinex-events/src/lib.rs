pub mod filesystem;
pub mod terminal;
pub mod window_manager;

// Re-export all event types
pub use filesystem::{FileCreated, FileModified, FileDeleted};
pub use terminal::CommandExecuted;
pub use window_manager::{
    WindowFocused, WindowOpened, WindowClosed, WindowMoved,
    WorkspaceChanged, MonitorFocused, StateSnapshot
};