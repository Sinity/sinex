//! Private mode (spec `privacy.md` §1 rule 7): one switch every capture path
//! checks before capturing. The switch is a file: present means on. If the
//! switch cannot be checked (its directory unreadable, or not creatable), the
//! answer is "on": capture stays off rather than capturing by accident.
//!
//! Live captures drop what arrives while it is on; pollers skip their poll. A
//! program that keeps its own history (a shell history file, a browser's
//! database) still writes it, and Sinex reads it after private mode ends.
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Where the switch lives: `$SINEX_PRIVATE_MODE` if set, else
/// `$XDG_RUNTIME_DIR/sinex/private-mode`, else `/run/sinex/private-mode`.
pub fn default_path() -> PathBuf {
    if let Some(p) = std::env::var_os("SINEX_PRIVATE_MODE") {
        return PathBuf::from(p);
    }
    std::env::var_os("XDG_RUNTIME_DIR")
        .map(|d| PathBuf::from(d).join("sinex"))
        .unwrap_or_else(|| PathBuf::from("/run/sinex"))
        .join("private-mode")
}

#[derive(Clone, Debug)]
pub struct PrivateMode {
    path: PathBuf,
}

impl Default for PrivateMode {
    fn default() -> Self {
        Self::new(default_path())
    }
}

impl PrivateMode {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Whether capture must stay off now.
    pub fn on(&self) -> bool {
        match std::fs::symlink_metadata(&self.path) {
            Ok(_) => true,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Off only if the directory that would hold the switch is (or
                // can be made) there and readable; otherwise the switch cannot
                // be checked.
                match self.path.parent() {
                    Some(dir) => {
                        !(std::fs::create_dir_all(dir).is_ok() && std::fs::read_dir(dir).is_ok())
                    }
                    None => true,
                }
            }
            Err(_) => true,
        }
    }

    /// Turn private mode on or off. Creates the switch's directory if needed.
    pub fn set(&self, on: bool) -> std::io::Result<()> {
        if let Some(dir) = self.path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        if on {
            std::fs::write(&self.path, b"private mode is on: Sinex captures nothing\n")
        } else {
            match std::fs::remove_file(&self.path) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
                _ => Ok(()),
            }
        }
    }
}

/// A switch checked at most every `every`, for paths that see many records a
/// second (a live stream's lines).
#[derive(Debug)]
pub struct Cached {
    mode: PrivateMode,
    every: Duration,
    last: Option<(Instant, bool)>,
}

impl Cached {
    pub fn new(mode: PrivateMode, every: Duration) -> Self {
        Self {
            mode,
            every,
            last: None,
        }
    }

    pub fn on(&mut self) -> bool {
        match self.last {
            Some((at, on)) if at.elapsed() < self.every => on,
            _ => {
                let on = self.mode.on();
                self.last = Some((Instant::now(), on));
                on
            }
        }
    }
}

#[cfg(test)]
mod tests;
