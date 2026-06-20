//! Production-path obligation tests for media capture packages (#1043).
//!
//! These cases exercise accepted staged media parser modes through the shared
//! source host obligation harness rather than only parser-local unit tests.

#[cfg(test)]
mod tests {
    const AUDIO_TRANSCRIPT_MANIFEST: &[u8] = br#"{
      "recording": {
        "format": "flac",
        "duration_ms": 4100,
        "source_file": "audio/session-a.flac",
        "policy_posture": "operator-controlled-raw-material"
      },
      "transcription_run": {
        "producer_run_id": "transcribe-run-a",
        "model_id": "whisper-large-v3",
        "model_version": "2026-06",
        "input_material_ids": ["raw-audio-a"],
        "output_refs": ["artifact:media.audio.transcript/run-a"],
        "duration_ms": 980,
        "resource_posture": "bounded-local-worker"
      },
      "segments": [
        {"text":"run-backed segment","start_ms":0,"end_ms":4100}
      ]
    }"#;

    const SCREEN_OCR_MANIFEST: &[u8] = br#"{
      "screenshot": {
        "display_id": "DP-2",
        "region": [0, 0, 800, 600],
        "width": 800,
        "height": 600,
        "source_file": "screens/session-a.png",
        "policy_posture": "operator-controlled-image-material"
      },
      "ocr_run": {
        "producer_run_id": "ocr-run-a",
        "engine_id": "tesseract",
        "engine_version": "5.5",
        "input_material_ids": ["raw-screen-a"],
        "output_refs": ["artifact:media.screen.ocr/run-a"],
        "duration_ms": 330,
        "resource_posture": "bounded-local-worker"
      },
      "segments": [
        {"text":"run-backed OCR","bbox":[4,8,160,24],"confidence":0.95}
      ]
    }"#;

    const AUDIO_TRANSCRIPT_CASE: crate::ProductionPathCase = crate::ProductionPathCase::new(
        "media.audio-transcript",
        "media.audio-transcript",
        crate::AdapterKind::StaticFile,
        AUDIO_TRANSCRIPT_MANIFEST,
        &[
            "media.audio.recording_observed",
            "media.audio.transcription_run_observed",
            "media.audio.transcript_segment_observed",
        ],
    );

    const SCREEN_OCR_CASE: crate::ProductionPathCase = crate::ProductionPathCase::new(
        "media.screen-ocr",
        "media.screen-ocr",
        crate::AdapterKind::StaticFile,
        SCREEN_OCR_MANIFEST,
        &[
            "media.screen.screenshot_observed",
            "media.screen.ocr_run_observed",
            "media.screen.ocr_segment_observed",
        ],
    );

    crate::production_path_case_test!(media_audio_transcript_obligations, AUDIO_TRANSCRIPT_CASE);
    crate::production_path_case_test!(media_screen_ocr_obligations, SCREEN_OCR_CASE);
}
