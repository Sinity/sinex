use super::*;
use crate::runtime::{
    CheckpointManager, EventTransport,
    stream::{EventEmitter, RuntimeContext, RuntimeHandles, ServiceInfo},
};
use camino::Utf8PathBuf;
use sinex_primitives::{HostName, SinexError, Uuid};
use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use xtask::sandbox::prelude::sinex_test;

/// Minimal 44-byte WAV header + `data` body for `channels`/`sample_rate`.
fn fake_wav(channels: u16, sample_rate: u32, data_bytes: usize) -> Vec<u8> {
    let bits_per_sample = 16u16;
    let byte_rate = sample_rate * u32::from(channels) * u32::from(bits_per_sample) / 8;
    let block_align = channels * bits_per_sample / 8;
    let mut b = Vec::new();
    b.extend_from_slice(b"RIFF");
    b.extend_from_slice(&((36 + data_bytes) as u32).to_le_bytes());
    b.extend_from_slice(b"WAVE");
    b.extend_from_slice(b"fmt ");
    b.extend_from_slice(&16u32.to_le_bytes());
    b.extend_from_slice(&1u16.to_le_bytes()); // PCM
    b.extend_from_slice(&channels.to_le_bytes());
    b.extend_from_slice(&sample_rate.to_le_bytes());
    b.extend_from_slice(&byte_rate.to_le_bytes());
    b.extend_from_slice(&block_align.to_le_bytes());
    b.extend_from_slice(&bits_per_sample.to_le_bytes());
    b.extend_from_slice(b"data");
    b.extend_from_slice(&(data_bytes as u32).to_le_bytes());
    b.extend(std::iter::repeat(0u8).take(data_bytes));
    b
}

struct CountingBackend {
    captures: Arc<AtomicUsize>,
}

#[async_trait]
impl AudioCaptureBackend for CountingBackend {
    async fn capture(&self, _request: &AudioCaptureRequest) -> RuntimeResult<CapturedAudio> {
        self.captures.fetch_add(1, Ordering::SeqCst);
        Ok(decode_audio_metadata(fake_wav(1, 16_000, 16_000)))
    }
}

async fn edge_runtime(
    ctx: &xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<RuntimeContext> {
    let checkpoint_manager = Arc::new(CheckpointManager::new(
        ctx.checkpoint_kv().await?,
        "audio-capture-gate-test".to_string(),
        "test-group".to_string(),
        format!("test-consumer-{}", Uuid::now_v7().simple()),
    ));
    let (event_sender, _event_receiver) = tokio::sync::mpsc::channel(1);
    let handles = RuntimeHandles::new_edge(
        checkpoint_manager,
        EventEmitter::new(event_sender, false),
        EventTransport::new_noop_direct(),
        None,
    );
    let work_dir = tempfile::tempdir()?;
    let work_dir_path = work_dir.keep();
    let work_dir_utf8 = Utf8PathBuf::from_path_buf(work_dir_path.clone()).map_err(|path| {
        SinexError::validation("temporary work dir should be UTF-8")
            .with_context("path", path.display().to_string())
    })?;
    Ok(RuntimeContext::new(
        ServiceInfo::new(
            "audio-capture-gate-test".to_string(),
            "audio-capture-gate-test".to_string(),
            HostName::from_static("test-host"),
            work_dir_path,
            false,
            format!("instance-{}", Uuid::now_v7().simple()),
            env!("CARGO_PKG_VERSION").to_string(),
            None,
        ),
        handles,
        HashMap::new(),
        work_dir_utf8,
    ))
}

#[sinex_test]
async fn startup_gate_blocks_audio_backend_without_database_pool(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let captures = Arc::new(AtomicUsize::new(0));
    let driver = MediaAudioCaptureDriver::with_backend(
        CountingBackend {
            captures: Arc::clone(&captures),
        },
        MediaAudioCaptureConfig::default(),
    );
    let runtime = edge_runtime(&ctx).await?;
    let state_dir = tempfile::tempdir()?;

    let captured = driver
        .capture_if_allowed(&runtime, state_dir.path())
        .await?;

    assert!(!captured);
    assert_eq!(captures.load(Ordering::SeqCst), 0);
    Ok(())
}

#[sinex_test]
async fn wav_metadata_is_decoded() -> xtask::sandbox::TestResult<()> {
    // 1 second of mono 16-bit @ 16kHz = 32000 data bytes.
    let captured = decode_audio_metadata(fake_wav(1, 16_000, 32_000));
    assert_eq!(captured.channel_count, Some(1));
    assert_eq!(captured.sample_rate_hz, Some(16_000));
    assert_eq!(captured.file_format.as_deref(), Some("wav"));
    assert_eq!(captured.duration_ms, Some(1000));
    Ok(())
}

#[sinex_test]
async fn non_wav_bytes_have_no_metadata() -> xtask::sandbox::TestResult<()> {
    let captured = decode_audio_metadata(b"not audio".to_vec());
    assert_eq!(captured.channel_count, None);
    assert_eq!(captured.file_format, None);
    Ok(())
}

#[sinex_test]
async fn build_recording_event_anchors_material_and_metadata()
-> xtask::sandbox::TestResult<()> {
    let captured = decode_audio_metadata(fake_wav(2, 48_000, 96_000));
    let material_id = Id::<SourceMaterial>::from_uuid(uuid::Uuid::new_v4());
    let config = MediaAudioCaptureConfig {
        policy_posture: "metadata_only".to_string(),
        ..MediaAudioCaptureConfig::default()
    };
    let event = build_recording_event(&captured, material_id, &config, Timestamp::now())?;
    let payload = &event.payload;
    assert_eq!(payload["raw_material_id"], material_id.to_string());
    assert_eq!(payload["channel_count"], 2);
    assert_eq!(payload["sample_rate_hz"], 48_000);
    assert_eq!(payload["file_format"], "wav");
    assert_eq!(payload["policy_posture"], "metadata_only");
    Ok(())
}

#[sinex_test]
async fn command_backend_reads_stdout_segment() -> xtask::sandbox::TestResult<()> {
    let dir = std::env::temp_dir().join(format!("sinex-audio-capture-{}", std::process::id()));
    tokio::fs::create_dir_all(&dir).await?;
    let wav_path = dir.join("fixture.wav");
    tokio::fs::write(&wav_path, fake_wav(1, 16_000, 16_000)).await?;
    let backend = CommandAudioCaptureBackend::new(
        "cat".to_string(),
        vec![wav_path.to_string_lossy().into_owned()],
        Duration::from_secs(5),
    );
    let captured = backend.capture(&AudioCaptureRequest::default()).await?;
    assert_eq!(captured.sample_rate_hz, Some(16_000));
    tokio::fs::remove_dir_all(&dir).await.ok();
    Ok(())
}

#[sinex_test]
async fn command_backend_reports_missing_program() -> xtask::sandbox::TestResult<()> {
    let backend = CommandAudioCaptureBackend::new(
        "sinex-nonexistent-recorder-binary".to_string(),
        Vec::new(),
        Duration::from_secs(5),
    );
    let error = backend
        .capture(&AudioCaptureRequest::default())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("not found"));
    Ok(())
}
