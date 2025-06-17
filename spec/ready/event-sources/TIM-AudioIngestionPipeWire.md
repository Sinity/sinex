# TIM-AudioIngestionPipeWire: Audio Capture via PipeWire

* **Relevant ADR:** (N/A directly, core ingestor for audio modality)
* **Original UG Context:** Section 9.2

This TIM details capturing audio (microphone input, system audio loopback) using PipeWire, which is the standard multimedia framework on modern Linux desktops.

## 1. Rationale Summary

PipeWire provides a unified and flexible way to manage audio streams, offering compatibility with PulseAudio applications and native low-latency capabilities. It's the standard for audio capture in the Exocortex environment.

## 2. Access Methods [UG Sec 9.2.1, OR2]

### 2.1. PulseAudio Compatibility Layer

* PipeWire implements a PulseAudio-compatible server. Most applications designed for PulseAudio work seamlessly.
* Exocortex agents can use standard PulseAudio client libraries (e.g., Python `sounddevice` with PortAudio backend linked against `libpulse`, or Rust crates like `libpulse-binding`).
* **Capture Sources:**
  * "Default" input source (maps to system's default microphone).
  * "Monitor" sources of output sinks for system audio loopback (e.g., `alsa_output.pci-0000_00_1f.3.analog-stereo.monitor` from `pactl list sources short`).

### 2.2. PipeWire Native API & Tools

* **`pw-record` (CLI Tool):**
  * `pw-record my_recording.wav`: Captures from default microphone.
  * `pw-record --target @DEFAULT_AUDIO_SINK@ system_audio_loopback.wav`: Captures system output from default sink.
  * `pw-record --target <node_id_or_name> specific_source.wav`: Captures from a specific PipeWire node.
  * `pw-record --format S16M --rate 16000 --channels 1 my_asr_ready_audio.wav`: Records in 16kHz, 16-bit mono PCM.
* **`pw-cat` (CLI Tool):** General-purpose for connecting PipeWire nodes.
  * `pw-cat -r --format S16M --rate 16000 --channels 1 --target <node_name> -`: Captures raw 16kHz mono PCM from `<node_name>` and pipes to `stdout`. This is ideal for feeding a live ASR engine.

        ```bash
        # Example: PipeWire mic audio directly to a hypothetical Exocortex ASR processor
        # pw-cat -r --format S16M --rate 16000 --channels 1 --target @DEFAULT_AUDIO_SOURCE@ - | \
        #   /opt/sinex/bin/sinex_asr_processor_stdin
        ```

* **GStreamer / FFmpeg:**
  * GStreamer: `pipewiresrc` element.
  * FFmpeg: `-f pipewire -i <node_name_or_id>` input device.

## 3. Node Identification and Targeting [UG Sec 9.2.2, OR2]

* **`wpctl status`:** WirePlumber tool to inspect PipeWire graph (sinks, sources, streams, node IDs, names).
* **Targeting Nodes:** Use Node ID (integer), Node Name (string, e.g., `alsa_input.pci-0000_00_1f.3.analog-stereo`), or special names (`@DEFAULT_AUDIO_SOURCE@`, `@DEFAULT_AUDIO_SINK@`).

## 4. Permissions and Portal Integration [UG Sec 9.2.3, OR2]

* **Unsandboxed Applications:** Typically, user-level apps can connect to user's PipeWire session and access default devices (mic) without interactive prompts.
* **Sandboxed Applications (Flatpak, Snap):** Audio capture usually goes through `xdg-desktop-portal` (e.g., `org.freedesktop.portal.Device`), which handles user consent.
* **Real-time Scheduling (RTKit):** PipeWire/clients may request real-time priorities via `rtkit-daemon`. NixOS: `services.pipewire.rtkitIntegration = true;` (or similar).

## 5. Audio Format for ASR (e.g., Whisper.cpp) [UG Sec 9.2.4, OR2]

* **Recommended for Whisper.cpp:** 16 kHz sampling rate, mono (single channel), 16-bit Linear PCM (Signed Little Endian).
* **PipeWire/Tool Configuration:**
  * `pw-record` / `pw-cat`: Use `--format`, `--rate`, `--channels` flags.
  * GStreamer/FFmpeg: Use caps filters or encoder options to specify target format.
* **Resampling/Downmixing:** If PipeWire provides audio in a different format (e.g., 48kHz stereo), the Exocortex agent must resample (e.g., with `libsamplerate` or SoX via FFmpeg) and downmix (e.g., average channels) before sending to ASR.

## 6. Performance and Latency Configuration [UG Sec 9.2.5, OR2]

* **PipeWire Design:** Low-latency, graph-based. Clients/devices negotiate buffer sizes and latencies.
* **Client Configuration:** Apps can request latency targets (e.g., `pw-record --latency=20ms`). Lower latency = smaller buffers, more frequent wake-ups, potentially higher CPU. Default latencies often adaptive (20-100ms).
* **For Real-Time ASR:** Agent should request low enough latency for responsiveness, ensuring ASR processing can keep up. Audio capture should typically be in a separate thread from ASR processing.

## 7. NixOS Packaging for Audio [UG Sec 9.1.6 (similar to video)]

* `pipewire`, `wireplumber` services enabled.
* CLI tools: `pkgs.pipewire` (for `pw-cat`, `pw-record`), `pkgs.wireplumber` (for `wpctl`), `pkgs.pulseaudio` (for `pactl` if using Pulse compat layer).
* Dev libraries: `pkgs.pipewire.dev`, GStreamer/FFmpeg with PipeWire support.
