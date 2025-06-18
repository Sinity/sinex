# TIM - ASR Whisper.cpp Integration

**Category**: AI Processing  
**Maturity Level**: L2 - Ready for Implementation  
**Implementation Status**: 5% - Database Infrastructure Only  

## Status Dashboard

### MVP Specification
- [ ] Whisper.cpp binary integration (0%)
- [ ] Audio file transcription pipeline (0%)
- [ ] Basic speech-to-text event processing (0%)
- [ ] Transcription result storage in database (10% - tables exist)
- [ ] Audio blob management with git-annex (0%)

### Enhanced Features  
- [ ] Real-time audio stream transcription (0%)
- [ ] Speaker diarization support (0%)
- [ ] Language detection and multi-language support (0%)
- [ ] Confidence scoring and quality metrics (0%)
- [ ] Incremental transcription for long audio (0%)

### Implementation Checklist
- [ ] Install and configure whisper.cpp binary
- [ ] Create `AudioTranscriptionWorker` in sinex-worker
- [ ] Implement audio file detection and queuing
- [ ] Add transcription event schemas
- [ ] Integrate with git-annex for audio blob storage
- [ ] Add audio format support (wav, mp3, m4a, flac)
- [ ] Implement worker queue processing for transcription jobs
- [ ] Add transcription quality assessment
- [ ] Create CLI interface for transcription queries
- [ ] Performance benchmarking and optimization

## Overview

Automatic Speech Recognition (ASR) using Whisper.cpp enables transcription of audio content captured by Sinex. This provides searchable text content from audio files, voice recordings, and potentially real-time audio streams.

## Current Implementation Status

**Verification against codebase:**
- ✅ **Database Infrastructure**: LLM tables exist (`core.llm_models`, `core.ai_generated_content`)
- ✅ **Worker Infrastructure**: Worker pattern exists in `sinex-worker` and `sinex-promo-worker`
- ✅ **Blob Storage**: Git-annex integration exists in `sinex-annex`
- ❌ **ASR Worker**: No whisper-specific worker implementation found
- ❌ **Audio Events**: No audio transcription event types found
- ❌ **Whisper Integration**: No whisper.cpp bindings or integration found
- ❌ **Audio Processing**: No audio file detection or processing logic

## Motivation

Audio transcription capabilities enable:
- Searchable transcripts of recorded meetings and calls
- Voice memo and note transcription
- Audio content analysis and summarization
- Cross-modal event correlation (text + audio)
- Accessibility improvements for audio content

## Technical Requirements

### Core Components

1. **AudioTranscriptionWorker**
   - Whisper.cpp binary execution management
   - Audio file format detection and conversion
   - Transcription job queue processing
   - Result quality assessment and validation

2. **Audio Event Detection**
   - Monitor filesystem for new audio files
   - Detect audio content in various formats
   - Queue transcription jobs for processing
   - Track transcription job status and progress

3. **Transcription Storage**
   - Store transcription results with confidence scores
   - Link transcriptions to original audio blobs
   - Support incremental/chunked transcription results
   - Enable efficient text search across transcriptions

### Integration Points

- **File System Events**: Detect new audio files automatically
- **Git Annex**: Store original audio files as blobs
- **AI Content Tables**: Store transcription results as AI-generated content
- **Work Queue**: Process transcription jobs asynchronously
- **Vector Embeddings**: Generate embeddings for transcribed text

## Implementation Architecture

### Worker Structure
```rust
pub struct AudioTranscriptionWorker {
    pool: PgPool,
    whisper_binary: PathBuf,
    model_path: PathBuf,
    temp_dir: PathBuf,
    max_concurrent_jobs: usize,
}

impl AudioTranscriptionWorker {
    pub async fn process_audio_file(&self, audio_path: &Path) -> Result<TranscriptionResult>;
    pub async fn queue_transcription_job(&self, event_id: Ulid, audio_blob_key: String) -> Result<()>;
    pub async fn claim_and_process_job(&self) -> Result<bool>;
}
```

### Event Schema
```rust
#[derive(Serialize, Deserialize)]
pub struct AudioFileDetected {
    pub file_path: String,
    pub file_size: u64,
    pub duration_seconds: Option<f64>,
    pub format: String,              // "wav", "mp3", "m4a", etc.
    pub sample_rate: Option<u32>,
    pub channels: Option<u16>,
    pub git_annex_key: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct AudioTranscriptionCompleted {
    pub source_event_id: String,     // Original AudioFileDetected event
    pub transcription_text: String,
    pub confidence_score: f64,
    pub language: String,
    pub processing_time_ms: u64,
    pub model_used: String,          // whisper model variant
    pub segments: Vec<TranscriptionSegment>,
}

#[derive(Serialize, Deserialize)]
pub struct TranscriptionSegment {
    pub start_time: f64,
    pub end_time: f64,
    pub text: String,
    pub confidence: f64,
}
```

## Configuration

### Basic Configuration
```toml
[audio_transcription]
enabled = true
whisper_binary_path = "/usr/local/bin/whisper"
model_path = "/var/lib/sinex/models/ggml-base.en.bin"
temp_directory = "/tmp/sinex-audio"
max_concurrent_jobs = 2

[audio_transcription.formats]
supported_extensions = ["wav", "mp3", "m4a", "flac", "ogg"]
auto_convert_to_wav = true
max_file_size_mb = 500

[audio_transcription.quality]
min_confidence_threshold = 0.6
enable_language_detection = true
supported_languages = ["en", "es", "fr", "de"]

[audio_transcription.processing]
chunk_length_seconds = 300  # 5 minutes
overlap_seconds = 5
enable_speaker_diarization = false
```

## Database Schema Extensions

### New Tables
```sql
-- Audio transcription jobs queue
CREATE TABLE IF NOT EXISTS core.audio_transcription_jobs (
    id ulid PRIMARY KEY DEFAULT gen_ulid(),
    source_event_id ulid NOT NULL REFERENCES raw.events(id),
    audio_blob_key TEXT NOT NULL,
    file_format TEXT NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' 
        CHECK (status IN ('pending', 'processing', 'completed', 'failed', 'retrying')),
    priority INTEGER NOT NULL DEFAULT 5,
    whisper_model TEXT NOT NULL DEFAULT 'base.en',
    processing_started_at TIMESTAMPTZ,
    processing_completed_at TIMESTAMPTZ,
    error_message TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Transcription results with segmentation
CREATE TABLE IF NOT EXISTS core.audio_transcriptions (
    id ulid PRIMARY KEY DEFAULT gen_ulid(),
    job_id ulid NOT NULL REFERENCES core.audio_transcription_jobs(id),
    full_text TEXT NOT NULL,
    language TEXT NOT NULL,
    confidence_score FLOAT NOT NULL,
    processing_time_ms INTEGER NOT NULL,
    segments JSONB NOT NULL DEFAULT '[]',
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

## Privacy Considerations

### Audio Content Sensitivity
- **High Privacy**: Personal conversations, meetings, phone calls
- **Medium Privacy**: Voice memos, dictated notes  
- **Low Privacy**: Public media, music, background audio

### Default Privacy Stance
- Transcription disabled by default
- Explicit opt-in required for audio processing
- Configurable exclusions for sensitive directories
- Option to transcribe without storing original audio

### Data Retention
- Configurable retention periods for transcriptions
- Separate retention for audio blobs vs. text transcripts
- Secure deletion of temporary processing files
- Audit logging for transcription activities

## Performance Considerations

### Processing Requirements
- CPU-intensive: Whisper.cpp benefits from multi-core systems
- Memory usage: ~1-8GB depending on model size
- Disk I/O: Temporary files during processing
- Processing time: ~0.1-1x real-time depending on model

### Optimization Strategies
- Queue-based processing to avoid overwhelming system
- Model size selection based on accuracy/speed trade-offs
- Batch processing of multiple short audio files
- Incremental processing for very long audio files

### Resource Management
- Limit concurrent transcription jobs
- Monitor CPU and memory usage
- Implement job prioritization
- Graceful degradation when resources constrained

## Testing Strategy

### Unit Tests
- Audio format detection and validation
- Whisper.cpp binary integration
- Transcription result parsing and validation
- Configuration and error handling

### Integration Tests
- End-to-end audio file processing pipeline
- Git-annex integration for audio storage
- Database persistence of transcription results
- Worker queue processing and job management

### System Tests
- Large audio file processing (>1 hour)
- Multiple concurrent transcription jobs
- Resource usage monitoring and limits
- Error recovery and retry mechanisms

## Success Metrics

### Functional Success
- Accurate transcription of various audio formats and qualities
- Reliable processing of audio files from filesystem monitoring
- Efficient storage and retrieval of transcription results
- Robust error handling and job retry mechanisms

### Performance Success
- <2x real-time processing for most audio content
- <4GB memory usage per transcription job
- >95% job completion rate without manual intervention
- <10 minute queue processing delay for typical audio files

### Quality Success
- >90% transcription accuracy for clear English audio
- Effective confidence scoring correlates with actual accuracy
- Language detection accuracy >95% for supported languages
- Graceful handling of noisy or low-quality audio

## Dependencies

### System Requirements
- **whisper.cpp**: Compiled binary with desired model files
- **FFmpeg**: Audio format conversion and metadata extraction
- **Git Annex**: Large file storage (existing in Sinex)
- **Sufficient CPU**: Multi-core system recommended for performance

### Rust Crates
- `tokio-process` - Async process execution for whisper binary
- `serde_json` - Parsing whisper output
- `uuid` - Job tracking and correlation
- `tempfile` - Secure temporary file management

### Model Files
- Whisper model files (ggml format for whisper.cpp)
- Various sizes: tiny, base, small, medium, large
- Language-specific models for better accuracy

## Future Enhancements

### Advanced Features
- Real-time audio stream transcription
- Speaker identification and diarization
- Emotion and sentiment analysis from audio
- Cross-modal search (find audio by text description)

### Integration Opportunities
- Voice command detection and processing
- Meeting summarization and action item extraction
- Audio-visual content synchronization
- Multi-language transcription and translation

### Model Improvements
- Fine-tuned models for specific domains or speakers
- Quantized models for faster processing
- Custom vocabulary and terminology support
- Continuous learning from transcription corrections

## References

- [Whisper.cpp GitHub Repository](https://github.com/ggerganov/whisper.cpp)
- [OpenAI Whisper Paper](https://arxiv.org/abs/2212.04356)
- [Audio Processing Best Practices](https://realpython.com/python-scipy-fft/)
- [Speech Recognition Evaluation Metrics](https://web.stanford.edu/~jurafsky/slp3/)