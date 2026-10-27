//! Private mode: one switch every capture path checks before capturing. The
//! switch is a file: present means on. If the switch cannot be checked (its
//! directory unreadable, or not creatable), capture stays off rather than
//! capturing by accident; the process says so once on standard error, with
//! the reason, and again when the switch can be checked.
//!
//! Live captures drop what arrives while it is on; pollers skip their poll. A
//! program that keeps its own history (a shell history file, a browser's
//! database) still writes it, and Sinex reads it after private mode ends.
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// What a switch says.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum State {
    /// Capture runs.
    Off,
    /// The operator turned private mode on: nothing is captured.
    On,
    /// The switch cannot be checked, for the reason given: nothing is
    /// captured until it can.
    Unchecked(String),
}

impl State {
    /// `off`, `on` or `unchecked`, as status output names it.
    pub fn name(&self) -> &'static str {
        match self {
            State::Off => "off",
            State::On => "on",
            State::Unchecked(_) => "unchecked",
        }
    }

    pub fn reason(&self) -> Option<&str> {
        match self {
            State::Unchecked(reason) => Some(reason),
            _ => None,
        }
    }
}

/// Switches this process found uncheckable, with the reason last reported.
static UNCHECKED: Mutex<Vec<(PathBuf, String)>> = Mutex::new(Vec::new());

/// The line to log for `state` of the switch at `path`, when it differs from
/// what this process last said about that switch: the first time it cannot be
/// checked (or for a new reason), and when it can be checked again. Checks
/// that change nothing return `None`, so a poller checking every second logs
/// once.
pub fn report(path: &Path, state: &State) -> Option<String> {
    let mut unchecked = UNCHECKED.lock().unwrap_or_else(|e| e.into_inner());
    let known = unchecked.iter().position(|(p, _)| p == path);
    match (state, known) {
        (State::Unchecked(reason), Some(i)) if unchecked[i].1 == *reason => None,
        (State::Unchecked(reason), Some(i)) => {
            unchecked[i].1 = reason.clone();
            Some(uncheckable(path, reason))
        }
        (State::Unchecked(reason), None) => {
            unchecked.push((path.to_owned(), reason.clone()));
            Some(uncheckable(path, reason))
        }
        (_, Some(i)) => {
            unchecked.remove(i);
            Some(format!(
                "private-mode switch {} can be checked again; capture follows it",
                path.display()
            ))
        }
        (_, None) => None,
    }
}

fn uncheckable(path: &Path, reason: &str) -> String {
    format!(
        "private-mode switch {} cannot be checked ({reason}); capturing nothing until it can",
        path.display()
    )
}

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

    /// What the switch says now.
    pub fn state(&self) -> State {
        match std::fs::symlink_metadata(&self.path) {
            Ok(metadata) if !metadata.is_file() => {
                State::Unchecked(format!("switch {} is not a file", self.path.display()))
            }
            Ok(_) => match std::fs::File::open(&self.path) {
                Ok(file) => match file.metadata() {
                    Ok(metadata) if metadata.is_file() => State::On,
                    Ok(_) => {
                        State::Unchecked(format!("switch {} is not a file", self.path.display()))
                    }
                    Err(e) => State::Unchecked(format!("cannot read {}: {e}", self.path.display())),
                },
                Err(e) => State::Unchecked(format!("cannot read {}: {e}", self.path.display())),
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Off only if the directory that would hold the switch is (or
                // can be made) there and readable; otherwise the switch cannot
                // be checked.
                let Some(dir) = self.path.parent() else {
                    return State::Unchecked("the switch path names no directory".into());
                };
                if let Err(e) = std::fs::create_dir_all(dir) {
                    return State::Unchecked(format!("cannot create {}: {e}", dir.display()));
                }
                match std::fs::read_dir(dir) {
                    Ok(_) => State::Off,
                    Err(e) => State::Unchecked(format!("cannot read {}: {e}", dir.display())),
                }
            }
            Err(e) => State::Unchecked(format!("cannot check {}: {e}", self.path.display())),
        }
    }

    /// Whether capture must stay off now. A switch that cannot be checked is
    /// reported once (see [`report`]).
    pub fn on(&self) -> bool {
        let state = self.state();
        if let Some(message) = report(&self.path, &state) {
            eprintln!("{message}");
        }
        state != State::Off
    }

    /// Turn private mode on or off. Creates the switch's directory if needed.
    pub fn set(&self, on: bool) -> std::io::Result<()> {
        if let Some(dir) = self.path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        if on {
            use std::io::Write;
            use std::os::unix::fs::OpenOptionsExt;
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o660)
                .open(&self.path)?;
            file.write_all(b"private mode is on: Sinex captures nothing\n")
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
