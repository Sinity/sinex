# TIM-FilesystemMonitoringWatchers: Platform-Specific Filesystem Watchers

*   **Relevant ADR:** (N/A directly, core for filesystem ingestor)
*   **Original UG Context:** Section 12.1

This TIM details the platform-specific filesystem watching mechanisms used by the Exocortex Filesystem Ingestor to monitor configured directories for changes.

## 1. Rationale Summary

Efficient, low-overhead filesystem monitoring is crucial for ingesting new or updated user files, PKM notes, downloads, etc. The choice of watcher is OS-dependent. The Exocortex ingestor will use `inotify` on Linux and FSEvents on macOS.

## 2. `inotify` (Linux) [UG Sec 12.1.1, CR2, SA1, OR2, CR3, SR1]

Linux kernel subsystem for monitoring filesystem events.

### 2.1. Mechanism and Event Types

*   Applications register "watches" on files/directories. Kernel sends events via an inotify file descriptor.
*   **Key Event Types (Bitmasks from `<sys/inotify.h>`):**
    *   `IN_MODIFY`: File modified.
    *   `IN_ATTRIB`: Metadata changed.
    *   `IN_CLOSE_WRITE`: File opened for writing was closed (often preferred over `IN_MODIFY` for completed writes).
    *   `IN_CLOSE_NOWRITE`: File not opened for writing was closed.
    *   `IN_OPEN`: File/directory opened.
    *   `IN_MOVED_FROM`: File/dir moved out of watched directory (rename source).
    *   `IN_MOVED_TO`: File/dir moved into watched directory (rename destination).
    *   `IN_CREATE`: File/dir created in watched directory.
    *   `IN_DELETE`: File/dir deleted from watched directory.
    *   `IN_DELETE_SELF`: Watched item itself deleted.
    *   `IN_MOVE_SELF`: Watched item itself moved.
    *   `IN_ISDIR`: Subject of event is a directory.
    *   `IN_Q_OVERFLOW`: Event queue overflowed (kernel dropped events).
    *   `IN_IGNORED`: Watch removed.
*   **`inotify_event` Structure (C):**
    ```c
    // struct inotify_event {
    //     int      wd;       // Watch descriptor
    //     uint32_t mask;     // Event bitmask
    //     uint32_t cookie;   // Connects IN_MOVED_FROM/TO for renames
    //     uint32_t len;      // Length of 'name' field
    //     char     name[];   // Optional relative filename (if event in watched dir)
    // };
    ```

### 2.2. System Limits and Configuration

Per-user limits, must be increased for extensive monitoring:
*   `/proc/sys/fs/inotify/max_user_watches`: Max watches (default ~8192). **Increase to e.g., 524288** via `sysctl fs.inotify.max_user_watches=524288` and persist in `/etc/sysctl.d/`. Each watch ~0.5-1KB kernel memory [SR1].
*   `/proc/sys/fs/inotify/max_user_instances`: Max inotify FDs (default ~128). Usually sufficient.
*   `/proc/sys/fs/inotify/max_queued_events`: Max queued events per instance (default ~16384). If ingestor is slow, can overflow.

### 2.3. Recursive Watching [SA1]

*   `inotify` is **not natively recursive**.
*   The Exocortex Filesystem Ingestor (or a library it uses, like `notify-rust` or Python's `watchdog`) must implement recursive logic:
    1.  Watch root directory.
    2.  On `IN_CREATE | IN_ISDIR`, add new watch for subdirectory.
    3.  On `IN_DELETE | IN_ISDIR` or `IN_MOVED_FROM | IN_ISDIR`, remove watch for subdirectory.
    4.  Handle `IN_MOVED_TO | IN_ISDIR` (add watch).

### 2.4. Overflow Recovery (`IN_Q_OVERFLOW`) [CR3]

If `IN_Q_OVERFLOW` is received, events were lost.
*   **Recovery Strategy:**
    1.  Log overflow.
    2.  (Optional) Clear existing watches to stop potentially misleading partial events.
    3.  Initiate an **incremental full rescan** of the monitored directory tree(s):
        *   Traverse filesystem.
        *   Compare current state (paths, mtimes, sizes, hashes) with Exocortex DB's last known state for files in watched paths.
        *   Update DB based on discrepancies (new, modified, deleted files).
    4.  Re-establish `inotify` watches.
    5.  Implement exponential backoff for triggering rescans if overflows are frequent.

### 2.5. Rust Implementation (Conceptual using `notify` crate)

The `notify` crate (Rust) provides a cross-platform abstraction, using `inotify` on Linux.

```rust
// use notify::{RecommendedWatcher, RecursiveMode, Watcher, Config, event::*};
// use std::path::Path;
// use std::sync::mpsc::channel; // Or use async channels with tokio

// fn run_inotify_watcher(paths_to_watch: Vec<String>) -> notify::Result<()> {
//     let (tx, rx) = channel(); // Create a channel to receive events

//     // Create a watcher object. RecommendedWatcher automatically selects the best backend.
//     // With `Config::OngoingEvents(Some(Duration::from_secs(1)))` you can get MODIFY events 
//     // that are still ongoing (file still open for write), then IN_CLOSE_WRITE.
//     // For Exocortex, we are primarily interested in IN_CLOSE_WRITE for completed files.
//     // The `notify` crate might abstract some of these details, but knowing inotify flags is useful.
//     let mut watcher = RecommendedWatcher::new(tx, Config::default())?;

//     for path_str in paths_to_watch {
//         let path = Path::new(&path_str);
//         // Watch path recursively
//         watcher.watch(path, RecursiveMode::Recursive)?;
//         println!("Watching path: {:?}", path);
//     }

//     // Event loop
//     for res in rx {
//         match res {
//             Ok(event) => {
//                 // event.kind is a notify::EventKind
//                 // Examples: EventKind::Create(_), EventKind::Modify(ModifyKind::Data(_)), EventKind::Remove(_)
//                 // EventKind::Modify(ModifyKind::Name(RenameMode::From)) -> IN_MOVED_FROM
//                 // EventKind::Modify(ModifyKind::Name(RenameMode::To)) -> IN_MOVED_TO
//                 // EventKind::Modify(ModifyKind::Name(RenameMode::Both)) -> In-place rename of a watched item
//                 // The `notify` crate tries to provide higher-level events.

//                 // For Exocortex, we'd map these `notify::Event`s to our internal representation
//                 // and then to raw.events payloads.
//                 // We are particularly interested in:
//                 // - Create events for new files/dirs.
//                 // - Modify events that signal a completed write (often need to check if file is still open or use IN_CLOSE_WRITE semantics).
//                 //   The `notify` crate might not directly expose IN_CLOSE_WRITE in its cross-platform API.
//                 //   It might send a Modify(DataChange::Content) and then Modify(Metadata(MetadataKind::Any)) when closed.
//                 //   This needs careful handling to correctly detect "file fully written".
//                 // - Remove events for deletions.
//                 // - Rename events (From/To pairs, often correlated by cookies in underlying inotify).

//                 println!("FS Event: {:?}", event);
//                 // TODO: Process event, hash file if Create/Modify, update Exocortex DB
//                 // For Modify, check event.kind. If it's just Metadata change, might not need re-hash.
//                 // If it's Content change, then re-hash.
//                 // The `notify` crate aims to debounce and coalesce events to some extent.
//             }
//             Err(e) => eprintln!("Watch error: {:?}", e),
//         }
//     }
//     Ok(())
// }
```
*   **Handling "Completed Writes":** The `notify` crate might not directly expose the `IN_CLOSE_WRITE` flag in its cross-platform `EventKind`. The ingestor logic needs to reliably determine when a file write is complete before hashing and ingesting. This might involve:
    *   Checking `EventKind::Modify(ModifyKind::Data(_))` and then waiting for a subsequent `EventKind::Access(AccessMode::Close(AccessKind::Write))` if the crate provides such detail.
    *   Or, upon `Modify(DataChange::Content)`, check if the file is still open by trying to open it exclusively. This is unreliable.
    *   Or, debounce modify events: if a file is modified, wait a short period (e.g., 1-2 seconds). If no further modify events for that file occur, assume the write is complete. This is a common heuristic but can delay ingestion.
    *   This is a key area where direct `inotify` access (e.g., via `inotify-rs` crate) allows more control by directly listening for `IN_CLOSE_WRITE`. If `notify` crate's abstraction is insufficient, dropping to a lower-level `inotify`-specific implementation might be needed for Linux.

## 3. `fanotify` (Linux) [UG Sec 12.1.2, CR2, SA1, SR1]

*   **Advantages:** Can monitor entire filesystem/mount point with one mark. Avoids `max_user_watches`. Can monitor permission events (for security tools). Lower memory for very large collections [SR1].
*   **Privileges:** Typically requires `CAP_SYS_ADMIN`, especially for filesystem-wide monitoring or permission events. Some restricted use might be possible unprivileged on newer kernels.
*   **Limitations for Exocortex Sync/Ingestion [SA1]:**
    *   Events often provide FD, not direct path (requires `/proc/self/fd/` lookup, adds overhead/races).
    *   Does not directly report deletes with filename or renames with source/destination path correlation like `inotify`'s cookie.
    *   Less suitable for precise, path-based file synchronization or fine-grained ingestion where knowing the exact name/path of changes within a directory is critical.
*   **Exocortex Use:** Not primary for filesystem ingestor due to path/rename limitations. Could be used by a separate system auditing agent if filesystem-wide open/access logging is desired for other security/observability purposes.

## 4. FSEvents (macOS) [UG Sec 12.1.3, SA1, CR2]

macOS native API for filesystem monitoring.

*   **Mechanism:** Applications create an FSEvents stream for specified paths.
*   **Stream Creation Flags (`FSEventStreamCreate`):**
    *   `kFSEventStreamCreateFlagFileEvents`: **Essential.** Delivers file-level events, not just directory-level. Reliable since macOS 10.7+.
    *   `kFSEventStreamCreateFlagWatchRoot`: Monitor changes to watched root path itself.
    *   `kFSEventStreamCreateFlagIgnoreSelf`: Ignore events from monitoring process.
*   **Features:**
    *   **Automatic Recursive Monitoring:** Default.
    *   **No Per-Directory Watch Limits.**
    *   **Event Coalescing:** Batches rapid changes. Latency configurable (`FSEventStreamSetLatency`).
    *   **Historical Event Access:** Can get events since a specific `FSEventStreamEventId` (per-volume counter) to catch up on changes while agent was offline.
*   **Exocortex Use:** The `notify` crate in Rust uses FSEvents as its backend on macOS, so the conceptual Rust code in 2.5 applies, with `notify` handling FSEvents specifics.

