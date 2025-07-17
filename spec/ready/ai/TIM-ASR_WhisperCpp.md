# TIM-ASR_WhisperCpp: Content Analysis - ASR with Whisper.cpp

## Status Dashboard
**Maturity Level**: L2 - Ready for Implementation
**Implementation**: 5% (Database infrastructure exists, Whisper integration needed)
**Dependencies**: Whisper.cpp binary, audio processing libraries, worker infrastructure, model downloads
**Blocks**: Audio transcription, voice note processing, meeting transcription, accessibility features

## MVP Specification
- Whisper.cpp installation and model management
- Basic audio file transcription pipeline
- Integration with blob storage for audio files
- Worker-based transcription processing
- Simple transcription result storage

## Enhanced Features
- Real-time audio stream transcription
- Multiple model support and auto-selection
- GPU acceleration for faster processing
- Advanced audio preprocessing and enhancement
- Speaker identification and diarization
- Custom vocabulary and fine-tuning support

## Implementation Checklist
- [ ] Whisper.cpp installation and configuration
- [ ] Audio transcription worker implementation
- [ ] Model download and management system
- [ ] Integration with audio capture pipeline
- [ ] Transcription result storage and indexing
- [ ] Performance optimization and GPU support
- [ ] Quality assessment and confidence scoring
- [ ] Batch processing for audio archives
- [ ] Real-time transcription capabilities

*   **Relevant ADR:** (N/A directly, core for audio processing)
*   **Original UG Context:** Section 18.2

This TIM details the use of Whisper.cpp for local Automatic Speech Recognition (ASR) to transcribe audio content within the Exocortex.

## 1. Rationale Summary

Whisper.cpp provides a high-performance C++ port of OpenAI's Whisper ASR models, enabling local, private, and offline speech-to-text capabilities. This is crucial for transcribing voice notes, audio from meetings or lectures, and other captured audio.

## 2. Whisper.cpp Tooling [UG Sec 18.2, OR2]

*   **Engine:** Whisper.cpp (ggerganov/whisper.cpp on GitHub).
*   **NixOS Package:** `pkgs.whisper-cpp` (or build from source). Ensure it's compiled with necessary features (e.g., BLAS for CPU speedup, CoreML/Metal for macOS GPU, CUDA for NVIDIA GPU, OpenCL for AMD/Intel GPU).
*   **Models (GGUF format):** Download desired Whisper model sizes (e.g., from Hugging Face under `ggerganov/whisper.cpp`).
    *   **For CPU-based Exocortex (Recommended) [OR2]:**
        *   `ggml-tiny.en.bin` / `ggml-tiny.bin` (~75MB): Fastest, lowest accuracy.
        *   `ggml-base.en.bin` / `ggml-base.bin` (~142MB): Good balance of speed/accuracy for CPU. Often near real-time for English.
        *   `ggml-small.en.bin` / `ggml-small.bin` (~466MB): More accurate, slower on CPU.
    *   **Quantized Models:** Use quantized versions (e.g., Q4_K_M, Q5_K_M in GGUF format) for reduced file size, memory usage, and faster CPU inference with minimal accuracy loss.
    *   Larger models (`medium`, `large`) are generally too slow for CPU real-time use without GPU.
*   **Command Line Usage (`main` executable from Whisper.cpp build):**
    ```bash
    /path/to/whisper.cpp/main \
        -m /path/to/models/ggml-base.en.bin \ # Model path
        -f /path/to/input_audio.wav \         # Input audio file (WAV, MP3, M4A, Ogg etc. via FFmpeg)
        -l en \                               # Language ('auto' for multilingual models)
        -t 8 \                                # Number of threads
        -p 4 \                                # Number of processors (for specific parts)
        -otxt \                               # Output format: txt, csv, srt, vtt, json (oj for full detail)
        --no-timestamps                       # Optional: if timestamps not needed (faster)
        # -pc                                 # Optional: print confidence colors
        # -ps                                 # Optional: print special tokens
    ```
    *   Can also take input from `stdin` for some modes, or use specific tools like `stream` for live mic input.

## 3. Audio Format for Whisper.cpp [UG Sec 9.2.4, OR2]

*   **Whisper Internal Requirement:** Processes audio as 16 kHz sampling rate, mono (single channel), typically 30-second chunks.
*   **Input Files:** Whisper.cpp uses FFmpeg internally to decode various audio formats and resample them to 16kHz mono as needed.
*   **For Exocortex Audio Capture (`TIM-AudioIngestionPipeWire.md`):**
    *   When capturing audio (e.g., with `pw-cat` or GStreamer), aim to record or convert directly to **16kHz, 16-bit Signed Little Endian PCM, mono WAV format** before feeding to Whisper.cpp. This avoids Whisper.cpp needing to invoke FFmpeg for conversion, potentially speeding up processing for short chunks.
    *   Example `pw-cat` for direct ASR input:
        `pw-cat -r --format=S16M --rate=16000 --channels=1 - | /path/to/whisper.cpp/main -m model.bin -f - ...` (using `-f -` for stdin).

## 4. Performance and Resource Usage [UG Sec 18.2, OR2]

*   **CPU Performance:** `base.en` (quantized) can often achieve faster-than-real-time on modern multi-core CPUs. Performance varies with CPU, model, quantization, threads.
*   **RAM Usage:**
    *   `ggml-base.en.bin`: Model ~142MB. Runtime RAM ~200-500MB+.
    *   `ggml-small.en.bin`: Model ~466MB. Runtime RAM ~0.8-1.5GB+.
    *   Quantization significantly reduces these.
*   **GPU Offloading:** Whisper.cpp supports CUDA, OpenCL, Metal (macOS). Requires compiling with GPU support. Can make larger models (medium, large) feasible for real-time use.

## 5. Exocortex S2T Agent Implementation (`S2T_Agent`) [UG Sec 18.3]

*   **Trigger:**
    *   Consumes `audio.recording.completed` events (payload: `annex_key` of audio blob, metadata).
    *   Or, for live dictation, directly consumes an audio stream (e.g., from `pw-cat` piped to agent's `stdin`).
*   **Processing Steps:**
    1.  Retrieve audio blob from `git-annex` using `annex_key`.
    2.  **Format Conversion (if needed):** If audio is not already 16kHz mono WAV, use FFmpeg (CLI or library bindings like `ffmpeg-python` or Rust `ffmpeg-next`) to convert it.
        ```bash
        # ffmpeg -i <input_original_audio_blob_path> \
        #   -ar 16000 -ac 1 -c:a pcm_s16le \
        #   /tmp/whisper_input_temp.wav
        ```
    3.  Invoke Whisper.cpp `main` CLI with the (converted) audio file path or pipe raw PCM stream to its `stdin`. Request JSON output (`-oj`) for detailed segments and timestamps.
        ```bash
        # Example:
        # whisper_cpp_cmd = [
        #    "/path/to/whisper.cpp/main", "-m", "/path/to/model.bin",
        #    "-f", "/tmp/whisper_input_temp.wav", "-l", "auto", # Or specific language
        #    "-t", "4", "-oj" # JSON output
        # ]
        # json_output_str = execute_command(whisper_cpp_cmd)
        ```
    4.  Parse the JSON output from Whisper.cpp. This typically contains:
        *   Full transcript text.
        *   Array of segments, each with `text`, `start_ms`, `end_ms`, tokens, confidence scores.
*   **Eventification & Storage:**
    1.  Store the full transcript text and/or detailed segment JSON in `core_artifact_contents` linked to a new `core_artifacts` entry (type `audio_transcript`). This artifact is also linked to the original audio `core_blobs` entry.
    2.  Emit `audio.transcript.completed` event to `core.events`.
        *   `source`: `"agent.s2t_whisper"`
        *   `event_type`: `"transcript_completed"`
        *   `payload`:
            ```json
            // {
            //   "source_audio_annex_key": "key_of_original_audio_blob",
            //   "transcript_artifact_id": "ULID_of_transcript_artifact", // Points to core_artifacts
            //   "transcript_content_id": "ULID_of_transcript_content", // Points to core_artifact_contents
            //   "language_detected": "en", // From Whisper output
            //   "model_used": "ggml-base.en.bin_q5_K_M",
            //   "processing_duration_ms": 15000,
            //   "word_count": 250,
            //   // Optionally, a short snippet of the transcript if full text is in artifact_contents
            //   "transcript_snippet": "First few words of transcript..."
            // }
            ```
    3.  The `core_artifact_contents` entry for the transcript is now available for embedding and semantic search.
*   **Live Transcription / Dictation (Advanced):**
    *   Agent continuously receives audio chunks (e.g., 5-30 seconds).
    *   Each chunk fed to Whisper.cpp (potentially using its streaming mode or providing previous transcript as context/prompt to improve coherence).
    *   Agent stitches together segment transcripts, emits partial updates to UI (e.g., Living Document via `$/exocortex/livingDocAppendText` LSP/RPC call or similar).
    *   Final full transcript handled as above.

