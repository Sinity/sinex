# Desktop Ingestor Implementation Details

The Desktop Ingestor captures real-time activity from the user's graphical environment, focusing on window management and clipboard interactions.

## Clipboard Monitoring

- **Dual API Integration**: Utilizes `arboard` for native Wayland/X11 clipboard access with a fallback to `copypasta` for maximum reliability.
- **Selection Types**: Monitors both the standard `CLIPBOARD` and the X11 `PRIMARY` (mouse selection) buffers.
- **Content Analysis**: Heuristically detects content types (text, file paths, URLs, or potential images) to provide rich metadata for events.

## Window Manager Sensing

- **Hyprland IPC**: Connects directly to the Hyprland event socket (`socket2.sock`) for real-time notifications about window focus, workspace switches, and window lifecycle events.
- **State Augmentation**: Queries the Hyprland command socket for detailed window geometry and class information.
- **Periodic Snapshots**: Captures the full state of all open windows and workspaces every 5 minutes to provide a baseline for system state reconstruction.

## Lifecycle & Reliability

- **Exponential Backoff**: Reconnects to the window manager socket with jittered exponential backoff to handle compositor restarts gracefully.
- **Graceful Shutdown**: Employs a `watch`-based signaling system to ensure that monitoring loops exit cleanly and all active material contexts are finalized before the process terminates.
- **Stale State Cleanup**: Automatically purges window information for entries not seen within 48 hours to prevent unbounded memory growth.
