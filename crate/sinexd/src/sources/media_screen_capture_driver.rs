//! Live screen-region capture source driver.
//!
//! This implements the `source:media.screen-ocr.on-demand-region` /
//! `live-session` capture modes through the GENERAL source runtime
//! ([`SourceDriver`] + [`SourceDriverRuntime`]) — the same machinery every
//! source uses. There is no capture-specific pipeline: a capture produces raw
//! PNG bytes, which are registered as ordinary source material via
//! [`AcquisitionManager`], and a `media.screen.screenshot_observed` event is
//! emitted through `runtime.emit_event`.
//!
//! The device call (the actual screen grab) sits behind the
//! [`ScreenCaptureBackend`] trait. The production backend
//! ([`CommandScreenCaptureBackend`]) shells out to an operator-configured
//! capture command (default `grim -` on Wayland, writing PNG to stdout); tests
//! inject a fake backend. Hardware verification happens on the live host, not
//! CI — the testable surface (command execution, PNG dimension parsing, event
//! construction) is exercised in unit tests.

use std::collections::HashMap;
use std::process::Stdio;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::runtime::{
    RuntimeResult, SourceDriver,
    acquisition_manager::RotationPolicy,
    stream::{
        Checkpoint, ContinuousStart, RuntimeCapabilities, RuntimeContext, ScanArgs, ScanReport,
        TimeHorizon,
    },
};
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::media::ScreenScreenshotObservedPayload;
use sinex_primitives::events::{Event, SourceMaterial};
use sinex_primitives::ids::Id;
use sinex_primitives::{JsonValue, SinexError, Timestamp};

const SOURCE_ID: &str = "media.screen-ocr";
/// Live-session mode whose operator session-control state gates continuous
/// capture. Matches the binding subject and the
/// `media.screen-ocr.{enable,disable,pause,resume}-session` operations.
const LIVE_SESSION_MODE_ID: &str = "source:media.screen-ocr.live-session";
const DEFAULT_CAPTURE_TIMEOUT_MS: u64 = 10_000;
const MAX_CAPTURE_BYTES: usize = 64 * 1024 * 1024;

/// A captured still image plus its decoded geometry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedImage {
    pub bytes: Vec<u8>,
    pub width_px: u32,
    pub height_px: u32,
}

/// Parameters for one screen-region capture.
#[derive(Debug, Clone, Default)]
pub struct ScreenCaptureRequest {
    pub region: Option<String>,
    pub display: Option<String>,
}

/// The device seam: how a screenshot is actually grabbed. Behind a trait so the
/// production command backend and a test fake are interchangeable.
#[async_trait]
pub trait ScreenCaptureBackend: Send + Sync {
    async fn capture(&self, request: &ScreenCaptureRequest) -> RuntimeResult<CapturedImage>;
}

/// Production backend: run an operator-configured command that writes PNG bytes
/// to stdout (default `grim -`). The command is bounded by a timeout.
#[derive(Debug, Clone)]
pub struct CommandScreenCaptureBackend {
    program: String,
    args: Vec<String>,
    timeout: Duration,
}

impl CommandScreenCaptureBackend {
    #[must_use]
    pub fn new(program: String, args: Vec<String>, timeout: Duration) -> Self {
        Self {
            program,
            args,
            timeout,
        }
    }

    fn from_config(config: &MediaScreenCaptureConfig) -> Self {
        let mut command = config.capture_command.clone();
        let program = if command.is_empty() {
            "grim".to_string()
        } else {
            command.remove(0)
        };
        Self {
            program,
            args: command,
            timeout: Duration::from_millis(config.capture_timeout_ms.max(1)),
        }
    }
}

#[async_trait]
impl ScreenCaptureBackend for CommandScreenCaptureBackend {
    async fn capture(&self, request: &ScreenCaptureRequest) -> RuntimeResult<CapturedImage> {
        let mut args = self.args.clone();
        // Pass a region as a trailing `-g <region>` when the operator did not
        // already encode it in the configured command.
        if let Some(region) = &request.region {
            if !self.args.iter().any(|arg| arg == "-g") {
                args.push("-g".to_string());
                args.push(region.clone());
            }
        }
        let mut child = Command::new(&self.program)
            .args(&args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    SinexError::invalid_state(format!(
                        "screen capture program '{}' not found",
                        self.program
                    ))
                } else {
                    SinexError::io("failed to spawn screen capture command")
                        .with_std_error(&error)
                }
            })?;

        let capture = async move {
            let mut stdout = child
                .stdout
                .take()
                .ok_or_else(|| SinexError::io("failed to capture screen-capture stdout"))?;
            let mut bytes = Vec::new();
            stdout
                .take(MAX_CAPTURE_BYTES as u64)
                .read_to_end(&mut bytes)
                .await
                .map_err(|error| {
                    SinexError::io("failed to read screen capture output").with_std_error(&error)
                })?;
            let status = child.wait().await.map_err(|error| {
                SinexError::io("screen capture command failed").with_std_error(&error)
            })?;
            Ok::<(Vec<u8>, std::process::ExitStatus), SinexError>((bytes, status))
        };
        let (bytes, status) = tokio::time::timeout(self.timeout, capture)
            .await
            .map_err(|_| SinexError::io("screen capture command timed out"))??;
        if !status.success() {
            return Err(SinexError::io(format!(
                "screen capture command exited with status {status}"
            )));
        }

        let (width_px, height_px) = png_dimensions(&bytes).ok_or_else(|| {
            SinexError::parse("screen capture output is not a recognizable PNG image")
        })?;
        Ok(CapturedImage {
            bytes,
            width_px,
            height_px,
        })
    }
}

/// Parse the width/height from a PNG IHDR chunk (big-endian u32 at byte offsets
/// 16 and 20). Returns `None` for non-PNG input.
#[must_use]
pub fn png_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    const PNG_SIGNATURE: [u8; 8] = [0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a];
    if bytes.len() < 24 || bytes[..8] != PNG_SIGNATURE {
        return None;
    }
    let width = u32::from_be_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]);
    let height = u32::from_be_bytes([bytes[20], bytes[21], bytes[22], bytes[23]]);
    Some((width, height))
}

/// Build the `media.screen.screenshot_observed` event for a captured image
/// anchored to its registered material. Pure (no IO) so it is unit-testable.
fn build_screenshot_event(
    captured: &CapturedImage,
    material_id: Id<SourceMaterial>,
    config: &MediaScreenCaptureConfig,
    request: &ScreenCaptureRequest,
    observed_at: Timestamp,
) -> RuntimeResult<Event<JsonValue>> {
    let region = request.region.as_ref().and_then(|region| parse_region(region));
    let payload = ScreenScreenshotObservedPayload {
        raw_material_id: material_id.to_string(),
        display_id: request.display.clone(),
        window_title: None,
        region,
        width_px: captured.width_px,
        height_px: captured.height_px,
        capture_session_id: None,
        source_file: None,
        policy_posture: config.policy_posture.clone(),
        observed_at,
    };
    let event = payload.from_material(material_id).build().map_err(|error| {
        SinexError::invalid_state(format!("failed to build screenshot event: {error}"))
    })?;
    event.to_json_event().map_err(|error| {
        SinexError::serialization(format!("failed to serialize screenshot event: {error}"))
    })
}

/// Parse a `x,y WxH` region string into `[x, y, w, h]`. Best-effort; `None`
/// when the shape is not recognized.
fn parse_region(region: &str) -> Option<Vec<i64>> {
    let (origin, size) = region.split_once(' ')?;
    let (x, y) = origin.split_once(',')?;
    let (w, h) = size.split_once('x')?;
    Some(vec![
        x.trim().parse().ok()?,
        y.trim().parse().ok()?,
        w.trim().parse().ok()?,
        h.trim().parse().ok()?,
    ])
}

/// Operator configuration for the screen-capture source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaScreenCaptureConfig {
    /// Command (argv) that writes PNG bytes to stdout. Default: `grim -`.
    #[serde(default = "default_capture_command")]
    pub capture_command: Vec<String>,
    /// Optional region (`x,y WxH`) passed to the capture command.
    #[serde(default)]
    pub region: Option<String>,
    /// Optional display/output identifier.
    #[serde(default)]
    pub display: Option<String>,
    /// Disclosure/storage posture recorded on each screenshot event.
    #[serde(default = "default_policy_posture")]
    pub policy_posture: String,
    /// Capture command timeout.
    #[serde(default = "default_capture_timeout_ms")]
    pub capture_timeout_ms: u64,
    /// Interval between captures in continuous (`live-session`) mode.
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,
}

fn default_capture_command() -> Vec<String> {
    vec!["grim".to_string(), "-".to_string()]
}

fn default_policy_posture() -> String {
    "operator_default".to_string()
}

fn default_capture_timeout_ms() -> u64 {
    DEFAULT_CAPTURE_TIMEOUT_MS
}

fn default_interval_secs() -> u64 {
    60
}

impl Default for MediaScreenCaptureConfig {
    fn default() -> Self {
        Self {
            capture_command: default_capture_command(),
            region: None,
            display: None,
            policy_posture: default_policy_posture(),
            capture_timeout_ms: default_capture_timeout_ms(),
            interval_secs: default_interval_secs(),
        }
    }
}

/// Checkpoint state — screenshots carry no resumable position (each capture is a
/// fresh real-world observation).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MediaScreenCaptureState {}

/// The screen-capture source driver. Generic over the device backend so tests
/// inject a fake; production registers it with [`CommandScreenCaptureBackend`].
pub struct MediaScreenCaptureDriver<B: ScreenCaptureBackend = CommandScreenCaptureBackend> {
    backend: Option<B>,
    config: MediaScreenCaptureConfig,
    runtime: Option<RuntimeContext>,
}

impl Default for MediaScreenCaptureDriver<CommandScreenCaptureBackend> {
    fn default() -> Self {
        Self {
            backend: None,
            config: MediaScreenCaptureConfig::default(),
            runtime: None,
        }
    }
}

impl<B: ScreenCaptureBackend> MediaScreenCaptureDriver<B> {
    /// Construct with an explicit backend (used by tests).
    #[must_use]
    pub fn with_backend(backend: B, config: MediaScreenCaptureConfig) -> Self {
        Self {
            backend: Some(backend),
            config,
            runtime: None,
        }
    }

    fn request(&self) -> ScreenCaptureRequest {
        ScreenCaptureRequest {
            region: self.config.region.clone(),
            display: self.config.display.clone(),
        }
    }

    /// Capture once and emit the screenshot event through the general pipeline.
    async fn capture_once(&self, runtime: &RuntimeContext) -> RuntimeResult<()> {
        let backend = self
            .backend
            .as_ref()
            .ok_or_else(|| SinexError::invalid_state("screen capture backend not initialized"))?;
        let request = self.request();
        let captured = backend.capture(&request).await?;

        let acq = runtime.acquisition_manager(RotationPolicy::default(), SOURCE_ID)?;
        let mut handle = acq.begin_material(SOURCE_ID).await?;
        let material_id: Id<SourceMaterial> = Id::from_uuid(handle.material_id);
        acq.append_slice(&mut handle, &captured.bytes).await?;
        acq.finalize(handle, "screenshot-captured").await?;

        let event = build_screenshot_event(
            &captured,
            material_id,
            &self.config,
            &request,
            Timestamp::now(),
        )?;
        runtime.emit_event(event).await?;
        info!(
            source_id = SOURCE_ID,
            width = captured.width_px,
            height = captured.height_px,
            "captured screenshot region"
        );
        Ok(())
    }
}

impl SourceDriver for MediaScreenCaptureDriver<CommandScreenCaptureBackend> {
    type Config = MediaScreenCaptureConfig;
    type State = MediaScreenCaptureState;

    fn name(&self) -> &str {
        SOURCE_ID
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            supports_snapshot: true,
            supports_historical: false,
            supports_continuous: true,
            supports_interactive: false,
            max_scan_size: None,
            supports_concurrent: false,
            manages_own_continuous_loop: true,
            manages_own_checkpoints: false,
        }
    }

    async fn initialize(
        &mut self,
        config: Self::Config,
        runtime: &RuntimeContext,
        _state: &mut Self::State,
    ) -> RuntimeResult<()> {
        self.backend = Some(CommandScreenCaptureBackend::from_config(&config));
        self.config = config;
        self.runtime = Some(runtime.clone());
        Ok(())
    }

    async fn scan_snapshot(
        &mut self,
        _state: &mut Self::State,
        _args: ScanArgs,
    ) -> RuntimeResult<ScanReport> {
        let started = Instant::now();
        let runtime = self
            .runtime
            .clone()
            .ok_or_else(|| SinexError::invalid_state("screen capture runtime not initialized"))?;
        self.capture_once(&runtime).await?;
        Ok(snapshot_report(1, started.elapsed()))
    }

    async fn scan_historical(
        &mut self,
        _state: &mut Self::State,
        from: Checkpoint,
        _until: TimeHorizon,
        _args: ScanArgs,
    ) -> RuntimeResult<ScanReport> {
        // Screenshots are real-time observations; there is nothing to backfill.
        Ok(ScanReport {
            events_processed: 0,
            duration: Duration::ZERO,
            final_checkpoint: from,
            time_range: None,
            runtime_stats: HashMap::new(),
            failed_targets: Vec::new(),
            successful_targets: Vec::new(),
            warnings: vec!["screen capture has no historical backfill".to_string()],
        })
    }

    async fn run_continuous(
        &mut self,
        _state: &mut Self::State,
        _start: ContinuousStart,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> RuntimeResult<ScanReport> {
        let started = Instant::now();
        let runtime = self
            .runtime
            .clone()
            .ok_or_else(|| SinexError::invalid_state("screen capture runtime not initialized"))?;
        let period = Duration::from_secs(self.config.interval_secs.max(1));
        let private_mode_state_dir =
            sinex_primitives::privacy::resolve_private_mode_state_dir(None);
        let mut captures = 0_u64;
        loop {
            let gate = match runtime.handles().db_pool() {
                Some(pool) => {
                    crate::sources::session_gate::evaluate_capture_gate(
                        pool,
                        &private_mode_state_dir,
                        SOURCE_ID,
                        LIVE_SESSION_MODE_ID,
                        "default",
                    )
                    .await
                }
                None => crate::sources::session_gate::CaptureGateDecision::active(),
            };
            if gate.is_suspended() {
                debug!(
                    source_id = SOURCE_ID,
                    reason = gate.reason_label(),
                    "screen capture suspended"
                );
            } else if let Err(error) = self.capture_once(&runtime).await {
                warn!(source_id = SOURCE_ID, error = %error, "screenshot capture failed");
            } else {
                captures += 1;
            }
            tokio::select! {
                biased;
                result = shutdown_rx.changed() => {
                    if result.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
                () = tokio::time::sleep(period) => {}
            }
        }
        Ok(snapshot_report(captures, started.elapsed()))
    }
}

fn snapshot_report(events: u64, duration: Duration) -> ScanReport {
    ScanReport {
        events_processed: events,
        duration,
        final_checkpoint: Checkpoint::None,
        time_range: None,
        runtime_stats: HashMap::new(),
        failed_targets: Vec::new(),
        successful_targets: Vec::new(),
        warnings: Vec::new(),
    }
}

crate::register_source!(
    source_id: "media.screen-ocr",
    driver: MediaScreenCaptureDriver<CommandScreenCaptureBackend>,
);

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::prelude::sinex_test;

    fn fake_png(width: u32, height: u32) -> Vec<u8> {
        let mut bytes = vec![0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a];
        // length + "IHDR" (8 bytes) then width/height big-endian.
        bytes.extend_from_slice(&[0, 0, 0, 13]);
        bytes.extend_from_slice(b"IHDR");
        bytes.extend_from_slice(&width.to_be_bytes());
        bytes.extend_from_slice(&height.to_be_bytes());
        bytes.extend_from_slice(&[8, 2, 0, 0, 0]); // bit depth, color type, etc.
        bytes
    }

    #[sinex_test]
    async fn png_dimensions_reads_ihdr() -> xtask::sandbox::TestResult<()> {
        assert_eq!(png_dimensions(&fake_png(1920, 1080)), Some((1920, 1080)));
        assert_eq!(png_dimensions(b"not a png"), None);
        assert_eq!(png_dimensions(&[]), None);
        Ok(())
    }

    #[sinex_test]
    async fn parse_region_reads_x_y_w_h() -> xtask::sandbox::TestResult<()> {
        assert_eq!(parse_region("10,20 800x600"), Some(vec![10, 20, 800, 600]));
        assert_eq!(parse_region("garbage"), None);
        Ok(())
    }

    #[sinex_test]
    async fn build_screenshot_event_anchors_material_and_geometry()
    -> xtask::sandbox::TestResult<()> {
        let captured = CapturedImage {
            bytes: fake_png(800, 600),
            width_px: 800,
            height_px: 600,
        };
        let material_id = Id::<SourceMaterial>::from_uuid(uuid::Uuid::new_v4());
        let config = MediaScreenCaptureConfig {
            policy_posture: "metadata_only".to_string(),
            ..MediaScreenCaptureConfig::default()
        };
        let request = ScreenCaptureRequest {
            region: Some("10,20 800x600".to_string()),
            display: Some("DP-1".to_string()),
        };
        let event =
            build_screenshot_event(&captured, material_id, &config, &request, Timestamp::now())?;
        let payload = &event.payload;
        assert_eq!(payload["raw_material_id"], material_id.to_string());
        assert_eq!(payload["width_px"], 800);
        assert_eq!(payload["height_px"], 600);
        assert_eq!(payload["display_id"], "DP-1");
        assert_eq!(payload["policy_posture"], "metadata_only");
        assert_eq!(payload["region"], serde_json::json!([10, 20, 800, 600]));
        Ok(())
    }

    struct FakeBackend {
        image: CapturedImage,
    }

    #[async_trait]
    impl ScreenCaptureBackend for FakeBackend {
        async fn capture(&self, _request: &ScreenCaptureRequest) -> RuntimeResult<CapturedImage> {
            Ok(self.image.clone())
        }
    }

    #[sinex_test]
    async fn command_backend_reads_stdout_png() -> xtask::sandbox::TestResult<()> {
        // A fake "capture command" (`cat <fixture>`) emits a PNG to stdout,
        // exercising the real command backend without a display.
        let dir = std::env::temp_dir().join(format!("sinex-screen-capture-{}", std::process::id()));
        tokio::fs::create_dir_all(&dir).await?;
        let png_path = dir.join("fixture.png");
        tokio::fs::write(&png_path, fake_png(640, 480)).await?;
        let backend = CommandScreenCaptureBackend::new(
            "cat".to_string(),
            vec![png_path.to_string_lossy().into_owned()],
            Duration::from_secs(5),
        );
        let captured = backend.capture(&ScreenCaptureRequest::default()).await?;
        assert_eq!((captured.width_px, captured.height_px), (640, 480));
        assert_eq!(captured.bytes, fake_png(640, 480));
        tokio::fs::remove_dir_all(&dir).await.ok();
        Ok(())
    }

    #[sinex_test]
    async fn command_backend_reports_missing_program() -> xtask::sandbox::TestResult<()> {
        let backend = CommandScreenCaptureBackend::new(
            "sinex-nonexistent-grim-binary".to_string(),
            Vec::new(),
            Duration::from_secs(5),
        );
        let error = backend
            .capture(&ScreenCaptureRequest::default())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not found"));
        Ok(())
    }

    #[sinex_test]
    async fn fake_backend_capture_is_usable() -> xtask::sandbox::TestResult<()> {
        let backend = FakeBackend {
            image: CapturedImage {
                bytes: fake_png(100, 50),
                width_px: 100,
                height_px: 50,
            },
        };
        let captured = backend.capture(&ScreenCaptureRequest::default()).await?;
        assert_eq!(captured.width_px, 100);
        Ok(())
    }
}
