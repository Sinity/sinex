use std::path::PathBuf;

/// Simple event output configuration
#[derive(Debug, Clone, Default)]
pub struct EventOutput {
    pub write_to_db: bool,
    pub log_events: bool,
    pub debug_file: Option<PathBuf>,
}

impl EventOutput {
    pub fn database() -> Self {
        Self {
            write_to_db: true,
            log_events: false,
            debug_file: None,
        }
    }
    
    pub fn dry_run() -> Self {
        Self {
            write_to_db: false,
            log_events: true,
            debug_file: None,
        }
    }
    
    pub fn with_debug_file(mut self, path: PathBuf) -> Self {
        self.debug_file = Some(path);
        self
    }
}