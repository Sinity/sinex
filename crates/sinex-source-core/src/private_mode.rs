//! The capture gate. Every check reads the switch; quiet streams are checked
//! too, so their gaps and health do not depend on a record arriving.
use serde::{Deserialize, Serialize};
use sinex_model::{EventPayload, KeyKind, ReplaceRule, TimeQuality, Timestamp, TsOrig};
mod switch;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
pub use switch::{Cached, PrivateMode, State, default_path, report};

/// Local integration point for the model registry's `capture.gap` declaration.
/// This record uses that event name and is not a separate event kind.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Gap {
    pub start: Option<Timestamp>,
    pub end: Option<Timestamp>,
    pub stream: String,
    pub reason: String,
}

impl EventPayload for Gap {
    const EVENT_TYPE: &'static str = "capture.gap";
    const KEY: KeyKind = KeyKind::Location;
    const REPLACE: ReplaceRule = ReplaceRule::SuppressRepeat;
    fn ts_orig(&self) -> TsOrig {
        TsOrig::maybe(self.start, TimeQuality::Captured)
    }
    fn payload_schema() -> serde_json::Value {
        serde_json::json!({"type": "object", "required": ["start", "end", "stream", "reason"]})
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Health {
    pub stream: String,
    pub error: Option<String>,
    pub state: String,
}

type Sink = Box<dyn FnMut(Gap) -> io::Result<()> + Send>;
struct Inner {
    mode: PrivateMode,
    stream: String,
    health_path: PathBuf,
    gap_path: PathBuf,
    health: Health,
    gap: Option<Gap>,
    checked: bool,
    generation: u64,
    sink: Sink,
    stopped: bool,
}

pub struct Gate {
    shared: Arc<(Mutex<Inner>, Condvar)>,
    watch: Option<std::thread::JoinHandle<()>>,
}

#[derive(Clone)]
pub struct GateAccess(Arc<(Mutex<Inner>, Condvar)>);

impl GateAccess {
    pub fn permit(&self) -> io::Result<ReadPermit> {
        let mut inner = self.0.0.lock().unwrap_or_else(|e| e.into_inner());
        let allowed = inner.check()?;
        Ok(ReadPermit(allowed.then_some(inner.generation)))
    }
}

/// A read may only be stored if private mode stayed off throughout it.
#[derive(Clone, Copy)]
pub struct ReadPermit(Option<u64>);

impl Gate {
    pub fn open(
        spool: &Path,
        stream: impl Into<String>,
        mode: PrivateMode,
        sink: impl FnMut(Gap) -> io::Result<()> + Send + 'static,
    ) -> io::Result<Self> {
        std::fs::create_dir_all(spool)?;
        let stream = stream.into();
        let gap_path = spool.join(".private-gap.json");
        let gap = match std::fs::read(&gap_path) {
            Ok(bytes) => Some(serde_json::from_slice(&bytes)?),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        let mut inner = Inner {
            mode,
            stream: stream.clone(),
            health_path: spool.join(".capture-health.json"),
            health: Health {
                stream,
                error: None,
                state: "off".into(),
            },
            gap_path,
            gap,
            checked: false,
            generation: 0,
            sink: Box::new(sink),
            stopped: false,
        };
        if inner.gap.is_some() && inner.mode.state() == State::Off {
            inner.close_gap(None)?;
        }
        inner.check()?;
        let shared = Arc::new((Mutex::new(inner), Condvar::new()));
        let watched = shared.clone();
        let watch = std::thread::spawn(move || {
            let (lock, changed) = &*watched;
            let mut inner = lock.lock().unwrap_or_else(|e| e.into_inner());
            while !inner.stopped {
                if let Err(error) = inner.check() {
                    inner.health.error = Some(format!("{}: {error}", inner.stream));
                    let _ = std::fs::write(
                        &inner.health_path,
                        serde_json::to_vec(&inner.health).unwrap_or_default(),
                    );
                    eprintln!("capture health: {}: {error}", inner.stream);
                }
                inner = changed
                    .wait_timeout(inner, Duration::from_millis(20))
                    .unwrap_or_else(|e| e.into_inner())
                    .0;
            }
        });
        Ok(Self {
            shared,
            watch: Some(watch),
        })
    }

    pub fn media(spool: &Path, stream: &str) -> io::Result<Self> {
        let dir = spool.to_owned();
        Self::open(spool, stream, PrivateMode::default(), move |gap| {
            let stem = format!(
                "{:020}-{:020}.gap",
                Timestamp::now().unix_ns(),
                Timestamp::now().unix_ns()
            );
            let partial = dir.join(format!(".{stem}.partial"));
            std::fs::write(&partial, serde_json::to_vec(&gap)?)?;
            std::fs::File::open(&partial)?.sync_all()?;
            crate::drivers::media::publish_segment(
                &dir,
                &partial,
                &stem,
                "gap",
                &serde_json::json!({"capture_gap": gap}),
            )?;
            Ok(())
        })
    }

    pub(crate) fn set_switch(&self, mode: PrivateMode) -> io::Result<()> {
        let mut inner = self.shared.0.lock().unwrap_or_else(|e| e.into_inner());
        inner.close_gap(Some(Timestamp::now()))?;
        inner.mode = mode;
        inner.generation += 1;
        inner.check()?;
        Ok(())
    }

    pub fn access(&self) -> GateAccess {
        GateAccess(self.shared.clone())
    }

    pub fn permit(&self) -> io::Result<ReadPermit> {
        let mut inner = self.shared.0.lock().unwrap_or_else(|e| e.into_inner());
        let allowed = inner.check()?;
        Ok(ReadPermit(allowed.then_some(inner.generation)))
    }

    pub fn accepts(&self, permit: ReadPermit) -> io::Result<bool> {
        let mut inner = self.shared.0.lock().unwrap_or_else(|e| e.into_inner());
        Ok(inner.check()? && permit.0 == Some(inner.generation))
    }

    pub fn allowed(&self) -> io::Result<bool> {
        let mut inner = self.shared.0.lock().unwrap_or_else(|e| e.into_inner());
        inner.check()
    }

    pub fn health(&self) -> Health {
        self.shared
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .health
            .clone()
    }

    /// Interrupt a recorder if private mode begins, and discard its result.
    /// The caller removes unfinished files when this returns None.
    pub fn record<T: Send>(
        &self,
        parent: &crate::capture::CaptureStop,
        capture: impl FnOnce(&crate::capture::CaptureStop) -> io::Result<T> + Send,
    ) -> io::Result<Option<T>> {
        let permit = self.permit()?;
        if permit.0.is_none() || parent.stopped() {
            return Ok(None);
        }
        let stop = crate::capture::CaptureStop::new(None);
        std::thread::scope(|scope| {
            let task = scope.spawn(|| capture(&stop));
            while !task.is_finished() {
                if parent.stopped() || !self.accepts(permit).unwrap_or(false) {
                    stop.stop();
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            let result = task
                .join()
                .map_err(|_| io::Error::other("capture panicked"))?;
            if stop.stopped() || !self.accepts(permit)? {
                Ok(None)
            } else {
                result.map(Some)
            }
        })
    }
}

impl Inner {
    fn check(&mut self) -> io::Result<bool> {
        let state = self.mode.state();
        let health = Health {
            stream: self.stream.clone(),
            state: state.name().into(),
            error: state
                .reason()
                .map(|cause| format!("{}: {cause}", self.stream)),
        };
        if health != self.health || !self.health_path.exists() {
            if let Some(error) = &health.error {
                eprintln!("capture health: {error}");
            }
            std::fs::write(&self.health_path, serde_json::to_vec(&health)?)?;
            self.health = health;
        }
        let now = Timestamp::now();
        if state != State::Off {
            if self.gap.is_none() {
                let gap = Gap {
                    stream: self.stream.clone(),
                    start: self.checked.then_some(now),
                    end: None,
                    reason: "private_mode".into(),
                };
                let temporary = self.gap_path.with_extension("partial");
                let file = std::fs::File::create(&temporary)?;
                serde_json::to_writer(&file, &gap)?;
                file.sync_all()?;
                std::fs::rename(&temporary, &self.gap_path)?;
                std::fs::File::open(self.gap_path.parent().expect("spool directory"))?
                    .sync_all()?;
                self.gap = Some(gap);
                self.generation += 1;
            }
            self.checked = true;
            return Ok(false);
        }
        self.checked = true;
        self.close_gap(Some(now))?;
        Ok(true)
    }

    fn close_gap(&mut self, end: Option<Timestamp>) -> io::Result<()> {
        if let Some(mut gap) = self.gap.clone() {
            gap.end = end;
            (self.sink)(gap)?;
            std::fs::remove_file(&self.gap_path)?;
            std::fs::File::open(self.gap_path.parent().expect("spool directory"))?.sync_all()?;
            self.gap = None;
        }
        Ok(())
    }
}

impl Drop for Gate {
    fn drop(&mut self) {
        {
            let mut inner = self.shared.0.lock().unwrap_or_else(|e| e.into_inner());
            inner.stopped = true;
            if let Err(error) = inner.close_gap(Some(Timestamp::now())) {
                eprintln!("capture health: {}: {error}", inner.stream);
            }
            self.shared.1.notify_all();
        }
        if let Some(watch) = self.watch.take() {
            let _ = watch.join();
        }
    }
}

#[cfg(test)]
mod tests;
