//! File watcher for hot reload support.
//!
//! Watches Rust source files for changes and debounces events
//! to avoid triggering multiple rebuilds.

use camino::Utf8PathBuf;
use color_eyre::eyre::{Result, eyre};
use notify::{RecommendedWatcher, RecursiveMode};
use notify_debouncer_mini::{DebouncedEvent, Debouncer, new_debouncer};
use std::path::Path;
use std::time::Duration;
use tokio::sync::mpsc;

/// File watcher that monitors source files for changes
pub struct FileWatcher {
    _debouncer: Debouncer<RecommendedWatcher>,
}

impl FileWatcher {
    /// Create a new file watcher for the given path.
    ///
    /// The watcher will monitor:
    /// - *.rs files (Rust sources)
    /// - Cargo.toml (dependencies)
    /// - Cargo.lock (locked dependencies)
    ///
    /// Changes are debounced for 300ms to avoid rapid rebuild triggers.
    pub fn new(path: &Utf8PathBuf, tx: mpsc::Sender<WatchEvent>) -> Result<Self> {
        let path_clone = path.clone();

        let mut debouncer = new_debouncer(
            Duration::from_millis(300),
            move |res: Result<Vec<DebouncedEvent>, notify::Error>| match res {
                Ok(events) => {
                    for event in events {
                        if should_trigger_rebuild(&event.path)
                            && tx
                                .blocking_send(WatchEvent::FileChanged(event.path.clone()))
                                .is_err()
                        {
                            break;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Watch error: {e:?}");
                }
            },
        )
        .map_err(|e| eyre!("Failed to create file watcher: {e}"))?;

        // Watch the src directory
        let src_path = path_clone.join("src");
        if src_path.exists() {
            debouncer
                .watcher()
                .watch(src_path.as_std_path(), RecursiveMode::Recursive)
                .map_err(|e| eyre!("Failed to watch {src_path}: {e}"))?;
        }

        // Watch Cargo.toml
        let cargo_toml = path_clone.join("Cargo.toml");
        if cargo_toml.exists() {
            debouncer
                .watcher()
                .watch(cargo_toml.as_std_path(), RecursiveMode::NonRecursive)
                .map_err(|e| eyre!("Failed to watch {cargo_toml}: {e}"))?;
        }

        Ok(Self {
            _debouncer: debouncer,
        })
    }

    /// Create a file watcher for the workspace root (watches all src/**/*.rs)
    pub fn for_workspace(workspace_root: &Path, tx: mpsc::Sender<WatchEvent>) -> Result<Self> {
        let mut debouncer = new_debouncer(
            Duration::from_millis(300),
            move |res: Result<Vec<DebouncedEvent>, notify::Error>| match res {
                Ok(events) => {
                    for event in events {
                        if should_trigger_rebuild(&event.path)
                            && tx
                                .blocking_send(WatchEvent::FileChanged(event.path.clone()))
                                .is_err()
                        {
                            break;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Watch error: {e:?}");
                }
            },
        )
        .map_err(|e| eyre!("Failed to create file watcher: {e}"))?;

        // Watch the entire workspace but filter in the callback
        debouncer
            .watcher()
            .watch(workspace_root, RecursiveMode::Recursive)
            .map_err(|e| eyre!("Failed to watch {}: {e}", workspace_root.display()))?;

        Ok(Self {
            _debouncer: debouncer,
        })
    }
}

/// Events emitted by the file watcher
#[derive(Debug, Clone)]
pub enum WatchEvent {
    /// A file was changed (path included for debugging/logging)
    FileChanged(std::path::PathBuf),
}

/// Check if a file change should trigger a rebuild
fn should_trigger_rebuild(path: &Path) -> bool {
    // Get the file name
    let Some(file_name) = path.file_name().and_then(|n| n.to_str()) else {
        return false;
    };

    // Check for Rust files
    if file_name.ends_with(".rs") {
        // Ignore target directory
        if path.components().any(|c| c.as_os_str() == "target") {
            return false;
        }
        return true;
    }

    // Check for Cargo files
    if file_name == "Cargo.toml" || file_name == "Cargo.lock" {
        return true;
    }

    // Ignore hidden files
    if file_name.starts_with('.') {
        return false;
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;
    use std::path::PathBuf;

    #[sinex_test]
    async fn test_should_trigger_rebuild() -> TestResult<()> {
        // Should trigger
        assert!(should_trigger_rebuild(&PathBuf::from("src/main.rs")));
        assert!(should_trigger_rebuild(&PathBuf::from("src/lib.rs")));
        assert!(should_trigger_rebuild(&PathBuf::from("src/foo/bar.rs")));
        assert!(should_trigger_rebuild(&PathBuf::from("Cargo.toml")));
        assert!(should_trigger_rebuild(&PathBuf::from("Cargo.lock")));

        // Should not trigger
        assert!(!should_trigger_rebuild(&PathBuf::from("target/debug/foo")));
        assert!(!should_trigger_rebuild(&PathBuf::from(".gitignore")));
        assert!(!should_trigger_rebuild(&PathBuf::from("README.md")));
        Ok(())
    }
}
