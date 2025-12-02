# Audio Transcription with Whisper.cpp

**Status**: Designed, not implemented
**Implementation**: 5% (Database infrastructure exists, Whisper integration needed)
**Priority**: Medium
**Dependencies**: Whisper.cpp binary, audio processing libraries, worker infrastructure, model downloads
**Blocks**: Audio transcription, voice note processing, meeting transcription, accessibility features

## Overview

Whisper.cpp provides a high-performance C++ port of OpenAI's Whisper ASR models, enabling local, private, and offline speech-to-text capabilities. This is crucial for transcribing voice notes, audio from meetings or lectures, and other captured audio content within Sinex.

## Technical Specification

### Whisper.cpp Integration

**Engine**: Whisper.cpp (ggerganov/whisper.cpp on GitHub)
**NixOS Package**: `pkgs.whisper-cpp` (or build from source)

**Model Selection for CPU-based Systems**:
- `ggml-tiny.en.bin` / `ggml-tiny.bin` (~75MB): Fastest, lowest accuracy
- `ggml-base.en.bin` / `ggml-base.bin` (~142MB): Good balance of speed/accuracy for CPU
- `ggml-small.en.bin` / `ggml-small.bin` (~466MB): More accurate, slower on CPU
- Quantized Models (Q4_K_M, Q5_K_M): Reduced size/memory with minimal accuracy loss

### Audio Format Requirements

- **Internal Processing**: 16 kHz sampling rate, mono, 30-second chunks
- **Input Flexibility**: Whisper.cpp uses FFmpeg to decode various formats
- **Optimal Input**: 16kHz, 16-bit Signed Little Endian PCM, mono WAV format

### S2T Agent Implementation

The Speech-to-Text agent will:

1. **Consume Events**: `audio.recording.completed` events with annex_key references
2. **Process Audio**:
   - Retrieve audio blob from git-annex
   - Convert to optimal format if needed (using FFmpeg)
   - Invoke Whisper.cpp with JSON output mode
3. **Store Results**:
   - Save transcript in `core_artifact_contents`
   - Link to original audio blob
   - Emit `audio.transcript.completed` event

### Event Schema

```json
{
  "source": "agent.s2t_whisper",
  "event_type": "transcript_completed",
  "payload": {
    "source_audio_annex_key": "key_of_original_audio_blob",
    "transcript_artifact_id": "ULID_of_transcript_artifact",
    "transcript_content_id": "ULID_of_transcript_content",
    "language_detected": "en",
    "model_used": "ggml-base.en.bin_q5_K_M",
    "processing_duration_ms": 15000,
    "word_count": 250,
    "transcript_snippet": "First few words of transcript..."
  }
}
```

## Implementation Plan

### MVP Features
- [ ] Whisper.cpp installation and configuration in NixOS module
- [ ] Basic audio file transcription pipeline
- [ ] Integration with blob storage for audio files
- [ ] Worker-based transcription processing
- [ ] Simple transcription result storage

### Enhanced Features
- [ ] Real-time audio stream transcription
- [ ] Multiple model support and auto-selection
- [ ] GPU acceleration for faster processing
- [ ] Advanced audio preprocessing and enhancement
- [ ] Speaker identification and diarization
- [ ] Custom vocabulary and fine-tuning support

### Performance Considerations

**CPU Performance**: 
- `base.en` (quantized) can achieve faster-than-real-time on modern multi-core CPUs
- Performance varies with CPU, model, quantization, thread count

**RAM Usage**:
- `ggml-base.en.bin`: ~200-500MB runtime
- `ggml-small.en.bin`: ~0.8-1.5GB runtime
- Quantization significantly reduces memory footprint

**GPU Offloading**: 
- Supports CUDA, OpenCL, Metal (macOS)
- Makes larger models (medium, large) feasible for real-time use

## Integration Points

- **Audio Capture Pipeline**: Integration with PipeWire audio capture
- **Blob Storage**: Git-annex for audio file management
- **Event System**: NATS JetStream for processing coordination
- **Search**: Transcripts indexed for semantic search

## Future Enhancements

### Live Transcription/Dictation
- Continuous audio chunk processing (5-30 second windows)
- Streaming mode with context preservation
- Partial update emission for UI feedback
- Integration with Living Document system

### Advanced Processing
- Multi-language support with auto-detection
- Confidence scoring and quality assessment
- Batch processing for audio archives
- Custom model fine-tuning for specific domains
