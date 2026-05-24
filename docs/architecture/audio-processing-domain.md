# Audio Processing Domain

Status: design contract. Implementation tracking is in #443 (media pipeline —
audio) and #388 (capture). Live audio capture is deferred; this record covers
the audio-as-source-material contract for the historical archive and for any
future live capture surface.

Audio in Sinex is treated as source material. Recordings are bytes registered
in `raw.source_material_registry`; everything derived from them (transcripts,
diarization, fingerprints, music identifications) is a synthesis derivation
linked back to the original recording.

## What This Owns

- Audio recordings as source material: WAV/Opus/FLAC blobs, content-addressed
  in local BLAKE3 CAS, with their own provenance records.
- Recording lifecycle events: `recording.started`, `recording.completed`.
- Transcription events: `transcript.completed` per recording.
- Voice-activity (energy-based VAD) analysis events.
- Music-fingerprint identification events (AcoustID/chromaprint).

## What This Does Not Own

- The capture device subsystem (PipeWire, phone offload, etc.). The capture
  surface is `audio.capture/recording.*`; the device-side mechanics are
  deferred until historical processing works end-to-end.
- Document/note storage for transcripts as searchable text. That belongs to
  the document layer (`docs/architecture/document-layer-v1.md`) via the
  same annotation/embedding paths.
- Speaker identification. v1 is voice-activity detection only; no speaker
  diarization with identities. Speaker identity is a future entity-resolution
  problem.
- Cloud transcription. Whisper runs locally; cloud transcription is rejected
  on privacy grounds for the recordings Sinex captures.

## Source-Material Layering

```
recording bytes (blob) ──┐
                         ├── recording.completed (material event)
                         │
                         ├── transcript.completed (synthesis, parent: recording)
                         │     └── annotation: transcript text
                         │     └── annotation: transcript_segments JSON
                         │
                         ├── activity.analyzed (synthesis, parent: recording)
                         │
                         └── fingerprint.matched (synthesis, parent: recording)
```

The transcript is a derivation, not the source. Re-running Whisper on the
same blob with a different model produces a superseding synthesis event, not
a new recording. This is the same replay/re-derivation pattern used elsewhere
in `docs/architecture/staged-source-parser-substrate.md`.

## Event Surfaces

| Source | Event type | Anchor |
| --- | --- | --- |
| `audio.capture` | `recording.started` | session_id |
| `audio.capture` | `recording.completed` | session_id (links to blob_id) |
| `audio.transcription` | `transcript.completed` | source_material_id of recording |
| `audio.analysis` | `activity.analyzed` | source_material_id of recording |
| `audio.analysis` | `fingerprint.matched` | source_material_id of recording |

### Recording Payload

```rust
pub struct AudioRecordingCompletedPayload {
    pub session_id: String,
    pub blob_id: String,            // BLAKE3 CAS id of audio bytes
    pub duration_ms: u64,
    pub file_size_bytes: u64,
    pub silence_ratio: f32,
}
```

Format normalization to 16kHz mono WAV is a preprocessing step inside the
transcription automaton (`ffmpeg -i input.* -ar 16000 -ac 1 -f wav`). The
normalized form is not stored as a separate blob — the original recording is
the canonical source material.

### Transcript Payload

```rust
pub struct AudioTranscriptCompletedPayload {
    pub source_material_id: String,
    pub recording_blob_id: String,
    pub duration_ms: u64,
    pub language_detected: String,  // BCP 47
    pub segment_count: u32,
    pub word_count: u32,
    pub processing_time_ms: u64,
    pub model_used: String,         // e.g. "ggml-large-v3-turbo"
}
```

Text storage:

1. Full transcript text → event annotation with `annotation_type = "transcript"`
   on the `recording.completed` event.
2. Segment JSON (with word timestamps) → event annotation with
   `annotation_type = "transcript_segments"` for time-anchored search.
3. Privacy engine applied to transcript with `ProcessingContext::Document`.

### Activity Payload

```rust
pub struct AudioActivityAnalyzedPayload {
    pub source_material_id: String,
    pub speech_ms: u64,
    pub silence_ms: u64,
    pub speech_ratio: f32,
    pub segment_count: u32,
}
```

Implemented via `webrtc-vad` (energy-based VAD). Speaker diarization with
identities (pyannote-style) is explicitly out of scope for v1 — it requires
Python + GPU + external model downloads that violate the local-only invariant.

### Fingerprint Payload

```rust
pub struct AudioFingerprintMatchedPayload {
    pub source_material_id: String,
    pub fingerprint_duration_s: u32,
    pub acoustid_score: f32,
    pub track_mbid: Option<String>,
    pub track_title: Option<String>,
    pub artist_name: Option<String>,
    pub album_title: Option<String>,
}
```

The `chromaprint` Rust crate wraps libchromaprint; AcoustID is queried over
HTTP. Successful identifications link to the Spotify-history `media.playback`
timeline when timestamps align — that link is downstream synthesis, not a
property of this event.

## Implementation Order

Per #443, the order is:

1. Historical: walk `archive/phone_audio_recordings.tar`, register each file
   as source material, transcribe through Whisper, store annotations.
2. Voice-activity analysis on the same files.
3. Music fingerprinting on ambient/background segments.
4. Live PipeWire capture deferred until the above is stable.

Whisper invocation: `whisper-rs` crate (whisper.cpp bindings) keeps the
runtime in-process. Subprocess fallback to `whisper.cpp` is acceptable if the
crate's build is awkward inside the Nix sandbox.

## Privacy

Audio is high-sensitivity by default.

| Surface | Policy |
| --- | --- |
| Recording blob | Local CAS, not exported broadly. |
| Transcript text | Privacy engine `ProcessingContext::Document` before storage. |
| Segment JSON | Same privacy context as transcript. |
| Fingerprint metadata | Track titles/artists treated as low sensitivity. |
| Speech-activity statistics | Low sensitivity (no content). |

Continuous ambient capture has additional consent implications and is gated
behind explicit opt-in once the live surface exists.

## Open Questions

- Whether transcripts should automatically flow into the document layer for
  embedding/semantic search. Default expectation: yes, on the same
  embeddability rules as documents.
- How long to retain raw recordings once transcript + activity + fingerprint
  derivations exist. Default expectation: indefinite, since the blob is the
  ground truth for any future re-derivation; a future retention policy may
  prune.
- Whether music fingerprinting should also produce proposals to merge with
  the Spotify-history `media.playback` timeline (e.g. "this ambient minute is
  the same track as your Spotify session"). That belongs to the
  proposal/judgment/finalizer substrate, not to this domain.

## Boundaries

- Do not treat transcripts as canonical text without source-material
  provenance. They are derivations and must reference the recording.
- Do not run cloud transcription. Whisper local-only.
- Do not run speaker identification in v1. Voice-activity only.
- Do not strip word timestamps before storage. Time-anchored search depends
  on them.

**Related:** `docs/architecture/staged-source-parser-substrate.md`,
`docs/architecture/document-layer-v1.md`,
`docs/architecture/proposal-judgment-finalizer.md`,
issues #388, #443.
