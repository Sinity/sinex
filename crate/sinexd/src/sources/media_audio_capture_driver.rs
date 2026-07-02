//! Live audio-recording capture source driver.
//!
//! Implements `source:media.audio-transcript.on-demand-session` /
//! `live-session` through the GENERAL source runtime ([`SourceDriver`] +
//! [`SourceDriverRuntime`]) — the same machinery the screen-capture driver and
//! every other source use. A recording produces raw audio bytes, registered as
//! ordinary source material via [`AcquisitionManager`], and a
//! `media.audio.recording_observed` event is emitted through
//! `runtime.emit_event`.
//!
//! The device call sits behind the [`AudioCaptureBackend`] trait. The production
//! backend ([`CommandAudioCaptureBackend`]) shells out to an operator-configured
//! recorder command that writes one self-terminating segment (default an
//! `ffmpeg` PulseAudio/PipeWire capture). Tests inject a fake backend; the real
//! device path is verified on the host, not CI.

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
use sinex_primitives::events::payloads::media::AudioRecordingObservedPayload;
use sinex_primitives::events::{Event, SourceMaterial};
use sinex_primitives::ids::Id;
use sinex_primitives::{JsonValue, SinexError, Timestamp};

const SOURCE_ID: &str = "media.audio-transcript";
/// Live-session mode whose operator session-control state gates continuous
/// capture. Matches the binding subject and the
/// `media.audio-transcript.{enable,disable,pause,resume}-session` operations.
const LIVE_SESSION_MODE_ID: &str = "source:media.audio-transcript.live-session";
const DEFAULT_CAPTURE_TIMEOUT_MS: u64 = 60_000;
const MAX_CAPTURE_BYTES: usize = 256 * 1024 * 1024;

/// A captured audio segment plus decoded metadata (best-effort from the WAV
/// header; all optional).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedAudio {
    pub bytes: Vec<u8>,
    pub file_format: Option<String>,
    pub codec: Option<String>,
    pub channel_count: Option<u32>,
    pub sample_rate_hz: Option<u32>,
    pub duration_ms: Option<u64>,
}

/// Parameters for one audio capture (reserved for device selection).
#[derive(Debug, Clone, Default)]
pub struct AudioCaptureRequest {
    pub device: Option<String>,
}

/// The device seam: how an audio segment is actually recorded.
#[async_trait]
pub trait AudioCaptureBackend: Send + Sync {
    async fn capture(&self, request: &AudioCaptureRequest) -> RuntimeResult<CapturedAudio>;
}

/// Production backend: run an operator-configured recorder command that writes
/// one self-terminating audio segment (e.g. WAV) to stdout, bounded by a
/// timeout.
#[derive(Debug, Clone)]
pub struct CommandAudioCaptureBackend {
    program: String,
    args: Vec<String>,
    timeout: Duration,
}

impl CommandAudioCaptureBackend {
    #[must_use]
    pub fn new(program: String, args: Vec<String>, timeout: Duration) -> Self {
        Self {
            program,
            args,
            timeout,
        }
    }

    fn from_config(config: &MediaAudioCaptureConfig) -> Self {
        let mut command = config.capture_command.clone();
        let program = if command.is_empty() {
            "ffmpeg".to_string()
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
impl AudioCaptureBackend for CommandAudioCaptureBackend {
    async fn capture(&self, _request: &AudioCaptureRequest) -> RuntimeResult<CapturedAudio> {
        let mut child = Command::new(&self.program)
            .args(&self.args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    SinexError::invalid_state(format!(
                        "audio capture program '{}' not found",
                        self.program
                    ))
                } else {
                    SinexError::io("failed to spawn audio capture command").with_std_error(&error)
                }
            })?;

        let capture = async move {
            let stdout = child
                .stdout
                .take()
                .ok_or_else(|| SinexError::io("failed to capture audio-capture stdout"))?;
            let mut bytes = Vec::new();
            stdout
                .take(MAX_CAPTURE_BYTES as u64)
                .read_to_end(&mut bytes)
                .await
                .map_err(|error| {
                    SinexError::io("failed to read audio capture output").with_std_error(&error)
                })?;
            let status = child.wait().await.map_err(|error| {
                SinexError::io("audio capture command failed").with_std_error(&error)
            })?;
            Ok::<(Vec<u8>, std::process::ExitStatus), SinexError>((bytes, status))
        };
        let (bytes, status) = tokio::time::timeout(self.timeout, capture)
            .await
            .map_err(|_| SinexError::io("audio capture command timed out"))??;
        if !status.success() {
            return Err(SinexError::io(format!(
                "audio capture command exited with status {status}"
            )));
        }
        if bytes.is_empty() {
            return Err(SinexError::io("audio capture command produced no audio"));
        }
        Ok(decode_audio_metadata(bytes))
    }
}

/// Best-effort metadata extraction from a captured buffer. Recognizes a WAV
/// (RIFF/WAVE) header and reads channels/sample-rate from the `fmt ` chunk and
/// duration from the `data` chunk; otherwise returns the bytes with `None`
/// metadata.
#[must_use]
pub fn decode_audio_metadata(bytes: Vec<u8>) -> CapturedAudio {
    if let Some(meta) = parse_wav_header(&bytes) {
        return CapturedAudio {
            bytes,
            file_format: Some("wav".to_string()),
            codec: Some("pcm".to_string()),
            channel_count: Some(meta.channels),
            sample_rate_hz: Some(meta.sample_rate),
            duration_ms: meta.duration_ms,
        };
    }
    CapturedAudio {
        bytes,
        file_format: None,
        codec: None,
        channel_count: None,
        sample_rate_hz: None,
        duration_ms: None,
    }
}

struct WavMeta {
    channels: u32,
    sample_rate: u32,
    duration_ms: Option<u64>,
}

fn parse_wav_header(bytes: &[u8]) -> Option<WavMeta> {
    if bytes.len() < 44 || &bytes[0..4] != b"RIFF" || &bytes[8..12] != b"WAVE" {
        return None;
    }
    // Walk chunks from offset 12 to locate `fmt ` and `data`.
    let mut offset = 12usize;
    let mut channels = None;
    let mut sample_rate = None;
    let mut byte_rate = None;
    let mut data_len = None;
    while offset + 8 <= bytes.len() {
        let chunk_id = &bytes[offset..offset + 4];
        let chunk_size =
            u32::from_le_bytes([bytes[offset + 4], bytes[offset + 5], bytes[offset + 6], bytes[offset + 7]])
                as usize;
        let body = offset + 8;
        if chunk_id == b"fmt " && body + 16 <= bytes.len() {
            channels = Some(u32::from(u16::from_le_bytes([bytes[body + 2], bytes[body + 3]])));
            sample_rate = Some(u32::from_le_bytes([
                bytes[body + 4],
                bytes[body + 5],
                bytes[body + 6],
                bytes[body + 7],
            ]));
            byte_rate = Some(u32::from_le_bytes([
                bytes[body + 8],
                bytes[body + 9],
                bytes[body + 10],
                bytes[body + 11],
            ]));
        } else if chunk_id == b"data" {
            // Streaming recorders may write a placeholder size; fall back to the
            // remaining buffer length.
            let remaining = bytes.len().saturating_sub(body);
            data_len = Some(if chunk_size == 0 || chunk_size > remaining {
                remaining
            } else {
                chunk_size
            });
            break;
        }
        offset = body + chunk_size + (chunk_size & 1);
    }
    let channels = channels?;
    let sample_rate = sample_rate?;
    let duration_ms = match (data_len, byte_rate) {
        (Some(len), Some(rate)) if rate > 0 => Some((len as u64 * 1000) / u64::from(rate)),
        _ => None,
    };
    Some(WavMeta {
        channels,
        sample_rate,
        duration_ms,
    })
}

/// Build the `media.audio.recording_observed` event for a captured segment
/// anchored to its registered material. Pure (no IO) so it is unit-testable.
fn build_recording_event(
    captured: &CapturedAudio,
    material_id: Id<SourceMaterial>,
    config: &MediaAudioCaptureConfig,
    observed_at: Timestamp,
) -> RuntimeResult<Event<JsonValue>> {
    let payload = AudioRecordingObservedPayload {
        raw_material_id: material_id.to_string(),
        file_format: captured.file_format.clone(),
        codec: captured.codec.clone(),
        duration_ms: captured.duration_ms,
        channel_count: captured.channel_count,
        sample_rate_hz: captured.sample_rate_hz,
        capture_session_id: None,
        source_file: None,
        policy_posture: config.policy_posture.clone(),
        observed_at,
    };
    let event = payload.from_material(material_id).build().map_err(|error| {
        SinexError::invalid_state(format!("failed to build recording event: {error}"))
    })?;
    event.to_json_event().map_err(|error| {
        SinexError::serialization(format!("failed to serialize recording event: {error}"))
    })
}

/// Operator configuration for the audio-capture source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaAudioCaptureConfig {
    /// Command (argv) that records one self-terminating audio segment to stdout.
    /// Default: an `ffmpeg` PulseAudio/PipeWire capture of a 10s WAV segment.
    #[serde(default = "default_capture_command")]
    pub capture_command: Vec<String>,
    /// Optional input device identifier (reserved; encode in the command today).
    #[serde(default)]
    pub device: Option<String>,
    /// Disclosure/storage posture recorded on each recording event.
    #[serde(default = "default_policy_posture")]
    pub policy_posture: String,
    /// Per-capture command timeout. Must exceed the segment length.
    #[serde(default = "default_capture_timeout_ms")]
    pub capture_timeout_ms: u64,
    /// Interval between captures in continuous (`live-session`) mode.
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,
}

fn default_capture_command() -> Vec<String> {
    [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "pulse",
        "-i",
        "default",
        "-t",
        "10",
        "-f",
        "wav",
        "-",
    ]
    .iter()
    .map(ToString::to_string)
    .collect()
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

impl Default for MediaAudioCaptureConfig {
    fn default() -> Self {
        Self {
            capture_command: default_capture_command(),
            device: None,
            policy_posture: default_policy_posture(),
            capture_timeout_ms: default_capture_timeout_ms(),
            interval_secs: default_interval_secs(),
        }
    }
}

/// Checkpoint state — recordings are real-time observations with no resumable
/// position.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MediaAudioCaptureState {}

/// The audio-capture source driver. Generic over the device backend so tests
/// inject a fake; production registers it with [`CommandAudioCaptureBackend`].
pub struct MediaAudioCaptureDriver<B: AudioCaptureBackend = CommandAudioCaptureBackend> {
    backend: Option<B>,
    config: MediaAudioCaptureConfig,
    runtime: Option<RuntimeContext>,
}

impl Default for MediaAudioCaptureDriver<CommandAudioCaptureBackend> {
    fn default() -> Self {
        Self {
            backend: None,
            config: MediaAudioCaptureConfig::default(),
            runtime: None,
        }
    }
}

impl<B: AudioCaptureBackend> MediaAudioCaptureDriver<B> {
    /// Construct with an explicit backend (used by tests).
    #[must_use]
    pub fn with_backend(backend: B, config: MediaAudioCaptureConfig) -> Self {
        Self {
            backend: Some(backend),
            config,
            runtime: None,
        }
    }

    /// Record one segment and emit the recording event through the general
    /// pipeline.
    async fn capture_once(&self, runtime: &RuntimeContext) -> RuntimeResult<()> {
        let backend = self
            .backend
            .as_ref()
            .ok_or_else(|| SinexError::invalid_state("audio capture backend not initialized"))?;
        let request = AudioCaptureRequest {
            device: self.config.device.clone(),
        };
        let captured = backend.capture(&request).await?;

        let acq = runtime.acquisition_manager(RotationPolicy::default(), SOURCE_ID)?;
        let mut handle = acq.begin_material(SOURCE_ID).await?;
        let material_id: Id<SourceMaterial> = Id::from_uuid(handle.material_id);
        acq.append_slice(&mut handle, &captured.bytes).await?;
        acq.finalize(handle, "audio-segment-captured").await?;

        let event = build_recording_event(&captured, material_id, &self.config, Timestamp::now())?;
        runtime.emit_event(event).await?;
        info!(
            source_id = SOURCE_ID,
            bytes = captured.bytes.len(),
            duration_ms = captured.duration_ms,
            "captured audio segment"
        );
        Ok(())
    }
}

impl SourceDriver for MediaAudioCaptureDriver<CommandAudioCaptureBackend> {
    type Config = MediaAudioCaptureConfig;
    type State = MediaAudioCaptureState;

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
        self.backend = Some(CommandAudioCaptureBackend::from_config(&config));
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
            .ok_or_else(|| SinexError::invalid_state("audio capture runtime not initialized"))?;
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
        Ok(ScanReport {
            events_processed: 0,
            duration: Duration::ZERO,
            final_checkpoint: from,
            time_range: None,
            runtime_stats: HashMap::new(),
            failed_targets: Vec::new(),
            successful_targets: Vec::new(),
            warnings: vec!["audio capture has no historical backfill".to_string()],
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
            .ok_or_else(|| SinexError::invalid_state("audio capture runtime not initialized"))?;
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
                    "audio capture suspended"
                );
            } else if let Err(error) = self.capture_once(&runtime).await {
                warn!(source_id = SOURCE_ID, error = %error, "audio capture failed");
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
    source_id: "media.audio-transcript",
    driver: MediaAudioCaptureDriver<CommandAudioCaptureBackend>,
);

#[cfg(test)]
#[path = "media_audio_capture_driver_test.rs"]
mod tests;
