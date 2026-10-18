//! Test hook: pause the writer once at a named point so a test can inspect or kill it.
//!
//! The daemon connects to the controller's Unix socket, sends one JSON line naming the
//! point, its pid and evidence, then waits. The controller replies `c` to continue or
//! kills the process. Any other reply, EOF or timeout fails the step; the pause is never
//! skipped silently.
use crate::{Error, Result};
use serde_json::{Value, json};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::time::Duration;

const COLLECTOR_SCHEDULED: &str = "collector-scheduled";
const BROWSER_COPY_BEFORE_CHECK: &str = "browser-copy-before-check";
const FIXED_POINTS: &[&str] = &[
    COLLECTOR_SCHEDULED,
    BROWSER_COPY_BEFORE_CHECK,
    "retained-fsynced",
    "transaction-staged",
    "transaction-committed",
    "delete-staged",
    "delete-committed",
    "fusion-replacement-committed",
    "session-replacement-committed",
];
/// Points numbered by step, such as `replay-staged-3`.
const STEP_PREFIXES: &[&str] = &[
    "replay-staged-",
    "replay-committed-",
    "projection-staged-",
    "projection-committed-",
];
const MAX_STEP: usize = 64;
const RELEASE_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Clone)]
pub struct TestPause {
    point: String,
    socket: PathBuf,
}
impl TestPause {
    pub fn new(point: String, socket: PathBuf) -> Result<Self> {
        let stepped = STEP_PREFIXES.iter().any(|prefix| {
            point
                .strip_prefix(prefix)
                .is_some_and(|step| step.parse::<usize>().is_ok_and(|n| n < MAX_STEP))
        });
        if !(stepped || FIXED_POINTS.contains(&point.as_str())) || !socket.is_absolute() {
            return Err(Error::Invalid(
                "test pause requires a known pause point and an absolute Unix socket",
            ));
        }
        Ok(Self { point, socket })
    }
    /// Pauses only the collector task; the writer and API keep serving.
    pub async fn collector_scheduled(pause: &mut Option<Self>, evidence: Value) -> Result<()> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let Some(pause) = Self::take_if(pause, COLLECTOR_SCHEDULED) else {
            return Ok(());
        };
        let notice = Self::notice("sinex-collector", COLLECTOR_SCHEDULED, evidence);
        tokio::time::timeout(RELEASE_TIMEOUT, async move {
            let mut socket = tokio::net::UnixStream::connect(pause.socket).await?;
            let mut bytes = serde_json::to_vec(&notice)
                .map_err(|_| Error::Invalid("cannot encode test pause notice"))?;
            bytes.push(b'\n');
            socket.write_all(&bytes).await?;
            let mut release = [0];
            socket.read_exact(&mut release).await?;
            Self::check_release(release)
        })
        .await
        .map_err(|_| Error::Invalid("test pause timed out"))?
    }
    /// Pauses after copying the browser database, before the copy is validated.
    pub async fn browser_copy_before_check(
        pause: &mut Option<Self>,
        evidence: Value,
    ) -> Result<()> {
        Self::reach(pause, BROWSER_COPY_BEFORE_CHECK, evidence).await
    }
    pub(crate) fn observes_atuin_queue(&self) -> bool {
        self.point == "fusion-replacement-committed"
    }
    /// Blocks the writer at `point` when this is the configured pause; otherwise returns.
    pub(crate) async fn reach(
        pause: &mut Option<Self>,
        point: &str,
        evidence: Value,
    ) -> Result<()> {
        let Some(pause) = Self::take_if(pause, point) else {
            return Ok(());
        };
        let notice = Self::notice("sinex-store", point, evidence);
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut socket = std::os::unix::net::UnixStream::connect(pause.socket)?;
            socket.set_read_timeout(Some(RELEASE_TIMEOUT))?;
            socket.set_write_timeout(Some(Duration::from_secs(10)))?;
            serde_json::to_writer(&mut socket, &notice)
                .map_err(|_| Error::Invalid("cannot encode test pause notice"))?;
            socket.write_all(b"\n")?;
            socket.flush()?;
            eprintln!("{notice}");
            let mut release = [0];
            socket.read_exact(&mut release)?;
            Self::check_release(release)
        })
        .await??;
        Ok(())
    }
    fn take_if(pause: &mut Option<Self>, point: &str) -> Option<Self> {
        pause.take_if(|pause| pause.point == point)
    }
    fn notice(component: &str, point: &str, evidence: Value) -> Value {
        json!({"component":component,"pause_point":point,"pid":std::process::id(),"evidence":evidence})
    }
    fn check_release(release: [u8; 1]) -> Result<()> {
        if release == *b"c" {
            Ok(())
        } else {
            Err(Error::Invalid("test pause release refused"))
        }
    }
}
