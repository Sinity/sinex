# TIM-WaylandScreenCapturePipeWire: Screen & Window Capture on Wayland

*   **Relevant ADR:** (N/A directly, but supports visual capture needs mentioned in ADR-003 and ADR-008 enhancement considerations)
*   **Original UG Context:** Section 9.1

This TIM details the standard mechanism for capturing screen or window content on Wayland desktops (including Hyprland) using PipeWire and the `xdg-desktop-portal` framework.

## 1. Rationale Summary

PipeWire and `xdg-desktop-portal` provide a secure, standardized, and user-consent-driven way for applications (including Exocortex agents) to capture video streams of entire screens/monitors or individual application windows on Wayland. This is the recommended approach for user-initiated recordings or application-level screen sharing.

## 2. Workflow: PipeWire and `xdg-desktop-portal` [UG Sec 9.1.1, CR5, SA4]

1.  **Request (D-Bus Call):**
    *   The Exocortex screen capture agent (or a CLI tool it invokes) makes a D-Bus call to the `org.freedesktop.portal.ScreenCast` interface provided by the active `xdg-desktop-portal` implementation (e.g., `xdg-desktop-portal-hyprland`).
    *   Method: `CreateSession` (requests a new screencast session).
    *   Method: `SelectSources` (allows portal to show UI for user to pick source).
    *   Method: `Start` (starts the stream for the selected source).
2.  **User Consent & Selection UI [UG Sec 9.1.2]:**
    *   The portal implementation presents a native UI dialog to the user.
    *   User selects what to share (entire screen/monitor, specific application window, sometimes a region).
    *   User explicitly grants permission for the capture session.
3.  **Stream Handle & PipeWire Connection:**
    *   If permission is granted, the portal returns a handle to the application. This handle includes information needed to connect to a PipeWire video stream for the selected source (e.g., a PipeWire node ID or remote name, and a PipeWire stream token/FD).
    *   The application uses PipeWire client libraries (or tools like GStreamer/FFmpeg with PipeWire support) to connect to this video stream.
4.  **Frame Capture:**
    *   The application receives video frames from PipeWire. These are often provided as DMA-BUFs for efficiency (see Section 4 below).
    *   Frames can then be encoded to video (H.264, VP9, AV1), saved as image sequences, or processed (e.g., OCR).

## 3. CLI Tools and Libraries

### 3.1. Command-Line Tools for Portal-Based Capture [SA4, CR5]

*   **`wf-recorder`:** Wayland-native screen recording utility. Uses `xdg-desktop-portal`.
    ```bash
    # Example: Record selected screen/window to output.mp4, prompting user with portal UI
    wf-recorder -o /path/to/output_video.mp4

    # Example: Record a specific output (monitor name from `wlr-randr` or similar)
    # wf-recorder -o output.mp4 -x <output_name> # May bypass portal UI if direct output specified

    # Example: Record with specific codec and audio from default PulseAudio source
    # wf-recorder -c libx264rgb -a default_audio_source.monitor -f output_with_audio.mkv
    ```
*   **`grim` (for Screenshots) & `slurp` (for Region/Window Selection) [CR5]:**
    *   `grim` captures screenshots. `slurp` gets geometry.
    *   Portal-based window/screen selection can also be used with `grim` if it supports it, or use compositor-specific methods if portal UI is not desired for quick screenshots.
    *   `grim -g "$(slurp -p)" screenshot_region.png` (select point then region)
    *   `grim -g "$(slurp -w)" screenshot_window.png` (select window)
    *   `grim -o <MONITOR_NAME> screenshot_monitor.png` (capture specific monitor)
*   **FFmpeg with PipeWire Input:**
    *   After obtaining the PipeWire source node ID/name from the portal session:
        ```bash
        # pipewire_node_id_or_name obtained from portal
        # ffmpeg -f pipewire -i <pipewire_node_id_or_name> \
        #   -c:v libx264 -preset ultrafast -crf 22 \
        #   -vf "format=yuv420p" \ # Ensure pixel format compatible with encoder
        #   output_ffmpeg_capture.mp4
        ```
    *   Listing PipeWire sources for FFmpeg/PipeWire tools: `pw-cli ls Node` or `wpctl status`.

### 3.2. Libraries for Programmatic Capture

*   **PipeWire Client Libraries (`libpipewire`):** For C/C++ applications to directly interact with PipeWire for stream negotiation and frame consumption.
*   **GStreamer:** Provides `pipewiresrc` element for capturing from PipeWire streams. Allows building complex media pipelines in various languages (C, Python, Rust via GStreamer bindings).
    ```gst-launch-1.0
    # gst-launch-1.0 pipewiresrc path=<pipewire_node_id_from_portal> ! \
    #   videoconvert ! queue ! \
    #   x264enc tune=zerolatency speed-preset=ultrafast ! \
    #   matroskamux ! filesink location=gstreamer_capture.mkv
    ```
*   **D-Bus Libraries (for Portal Interaction):** Any language with D-Bus bindings (e.g., `python-dbus`, `zbus` for Rust) can make calls to `org.freedesktop.portal.ScreenCast`.

## 4. DMA Buffer Implementation for Efficiency [UG Sec 9.1.5, CR5, SA4]

*   **Mechanism:** PipeWire and Wayland compositors (often via protocols like `zwlr_screencopy_v1` or internal mechanisms) leverage DMA-BUFs for efficient, zero-copy sharing of video frames.
*   **Zero-Copy Path:**
    1.  Compositor renders frame to GPU buffer.
    2.  Buffer exported as DMA-BUF (FDs + metadata like format, strides, modifier).
    3.  PipeWire passes these DMA-BUF FDs/metadata to the client application.
    4.  Client imports DMA-BUF into its GPU context or passes directly to hardware-accelerated video encoder (VAAPI, NVENC, V4L2 M2M).
*   **Benefit:** Avoids costly GPU-CPU-GPU memory copies for video frames, significantly reducing CPU usage and latency. Essential for high-resolution, high-framerate screen capture.

## 5. Video Codec Selection and Frame Rate Optimization [UG Sec 9.1.3, CR5]

*   **Frame Rate:** PipeWire can support high frame rates (e.g., 60fps, matching monitor refresh). Achieved rate depends on system load, GPU, encoder performance.
*   **Video Codecs (for recording):**
    *   **H.264 (`libx264` software, `h264_vaapi`/`h264_nvenc` hardware):** Best compatibility, good hardware acceleration. Recommended default.
    *   **VP9 (`libvpx-vp9`):** Good quality, open. Hardware acceleration less common but available.
    *   **AV1 (`libaom`, `svt-av1`, hardware AV1 encoders on newest GPUs):** Best compression efficiency. Software encoding very CPU-heavy. Hardware encoding emerging.
*   **Exocortex Strategy:** Use hardware-accelerated H.264 by default. Provide options for other codecs if user hardware/preferences dictate. Target frame rate should be configurable (e.g., 15-30fps for general context, up to 60fps for smooth recordings).

## 6. Security Sandboxing with Flatpak [UG Sec 9.1.4, CR5]

*   Flatpak applications *must* use `xdg-desktop-portal` for screen capture.
*   Flatpak manifest needs `org.freedesktop.portal.ScreenCast` permission. Portal handles user consent outside the sandbox.

## 7. NixOS Packaging Requirements [UG Sec 9.1.6, CR5, SA4]

Ensure these are in the NixOS system configuration for Wayland screen capture to function:
*   `pipewire`: `services.pipewire.enable = true;` (with alsa, pulse, jack support if needed).
*   `wireplumber`: `programs.wireplumber.enable = true;` (or pulled in by `services.pipewire`).
*   `xdg-desktop-portal`: `services.xdg.portal.enable = true;` (NixOS 23.11+ path, older might be `services.xdg-desktop-portal.enable`).
*   **Wayland Portal Backend:**
    *   `xdg-desktop-portal-hyprland`: `programs.hyprland.portalPackage` or `services.xdg.portal.extraPortals = [ pkgs.xdg-desktop-portal-hyprland ];`.
    *   (Or `-gnome`, `-kde` equivalents if using those DEs).
*   **Capture Tools:** `pkgs.wf-recorder`, `pkgs.grim`, `pkgs.slurp`, `pkgs.ffmpeg-full` (compiled with PipeWire).
*   **Development Libraries:** `pkgs.pipewire.dev`, `pkgs.gstreamer`, `pkgs.gst_all_plugins`.

