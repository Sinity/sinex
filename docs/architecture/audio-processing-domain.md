# Audio Processing Domain

**Status:** dissolved into issue tracking. The substantive contracts
that lived here — audio-as-source-material layering, event surface
table (`recording.started`/`completed`, `transcript.completed`,
`activity.analyzed`, `fingerprint.matched`), payload shapes,
Whisper-via-`whisper-rs` runtime decision, VAD-only-no-diarization-in-v1
invariant, transcript/segment storage rules, fingerprint linkage to
Spotify timeline, implementation order, privacy table, and the
boundaries list — now live in [issue #1043 (feat(capture): add audio
transcripts and screen OCR streams)](https://github.com/Sinity/sinex/issues/1043)
as a design comment.

Originating design issues `#388` (sinex-audio-ingestor) and `#443`
(media pipeline — audio) are closed. `#1043` is the live tracking
issue.

Privacy admission belongs to `#1042`. Source material staging belongs
to `#1065`. Producer-side admission belongs to `#1064`. Event
admission belongs to `#1056`.

**Related:** `docs/architecture/staged-source-parser-substrate.md`,
`docs/architecture/document-layer-v1.md`,
`docs/architecture/proposal-judgment-finalizer.md`.
