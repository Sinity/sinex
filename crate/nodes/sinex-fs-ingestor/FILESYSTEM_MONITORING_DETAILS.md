# Filesystem Monitoring Implementation Details

This document contains detailed technical information about filesystem monitoring that supplements the implementation in this crate.

## inotify Event Type Mappings (Linux)

The notify-rs crate abstracts platform-specific events, but understanding the underlying inotify events is crucial for debugging and optimization.

### Core inotify Events

```c
// From <sys/inotify.h> - Event bitmasks
IN_MODIFY        // File modified
IN_ATTRIB        // Metadata changed  
IN_CLOSE_WRITE   // File opened for writing was closed (preferred over IN_MODIFY)
IN_CLOSE_NOWRITE // File not opened for writing was closed
IN_OPEN          // File/directory opened
IN_MOVED_FROM    // File/dir moved out of watched directory (rename source)
IN_MOVED_TO      // File/dir moved into watched directory (rename destination)
IN_CREATE        // File/dir created in watched directory
IN_DELETE        // File/dir deleted from watched directory
IN_DELETE_SELF   // Watched item itself deleted
IN_MOVE_SELF     // Watched item itself moved
IN_ISDIR         // Subject of event is a directory
IN_Q_OVERFLOW    // Event queue overflowed (kernel dropped events)
IN_IGNORED       // Watch removed
```

### inotify_event Structure

```c
struct inotify_event {
    int      wd;       // Watch descriptor
    uint32_t mask;     // Event bitmask
    uint32_t cookie;   // Connects IN_MOVED_FROM/TO for renames
    uint32_t len;      // Length of 'name' field
    char     name[];   // Optional relative filename
};
```

## Overflow Recovery Strategy

When `IN_Q_OVERFLOW` is received (events were lost), the recommended recovery process is:

1. **Log the overflow event** with timestamp and affected paths
2. **Clear existing watches** to stop potentially misleading partial events (optional)
3. **Initiate incremental full rescan**:
   - Traverse filesystem tree
   - Compare current state (paths, mtimes, sizes, hashes) with database
   - Update database based on discrepancies
4. **Re-establish inotify watches**
5. **Implement exponential backoff** if overflows are frequent

## Handling Completed Writes

The notify crate doesn't directly expose `IN_CLOSE_WRITE`, which is crucial for knowing when a file write is complete. Strategies:

1. **Debouncing**: Wait 1-2 seconds after last modify event
2. **Exclusive open attempt**: Try to open file exclusively (unreliable)
3. **Platform-specific handling**: Use lower-level inotify-rs for Linux if needed
4. **Event correlation**: Look for Access(Close(Write)) events after Modify

## System Limits and Tuning

### Linux inotify Limits

- `/proc/sys/fs/inotify/max_user_watches`: Max watches per user (default ~8192)
  - Each watch uses ~0.5-1KB kernel memory
  - Recommended for Sinex: 524288
- `/proc/sys/fs/inotify/max_user_instances`: Max inotify FDs (default ~128, usually sufficient)
- `/proc/sys/fs/inotify/max_queued_events`: Max queued events (default ~16384)

### Setting Persistent Limits

```bash
# /etc/sysctl.d/99-sinex-inotify.conf
fs.inotify.max_user_watches=524288
fs.inotify.max_queued_events=32768
```

## Platform-Specific Considerations

### Linux (inotify)
- Not natively recursive - must manually add watches for subdirectories
- Watch limits can be exhausted on large directory trees
- Symlinks and mount points need special handling
- Network filesystems may not generate events

### macOS (FSEvents)
- Automatically recursive
- No per-directory watch limits
- Event coalescing with configurable latency
- Historical event access via FSEventStreamEventId

### Windows (ReadDirectoryChangesW)
- Buffer size limitations
- Network drive restrictions
- Junction point handling differs from symlinks

## Performance Optimization Tips

1. **Selective Watching**: Don't watch paths like node_modules, .git
2. **Event Filtering**: Filter events at kernel level when possible
3. **Batch Processing**: Process multiple events together
4. **Async I/O**: Use tokio for non-blocking event handling
5. **Connection Pooling**: Reuse database connections for event processing

## Current Stability Rules

- The fs ingestor now defaults to a `524288` watch budget to match the documented Linux recommendation.
- Automatic recursive poll fallback is no longer used for oversized or partially unreadable trees.
- When a tree is too large or contains unreadable descendants, the ingestor first builds a filtered native watch plan and skips ignored heavy descendants such as `.git`, `.direnv`, `node_modules`, and `target`.
- If the filtered native plan still exceeds the configured watch budget, the node fails honestly during initialization instead of recursively polling the whole tree.

## Unimplemented Features

The following features are documented but not yet implemented:

1. **Advanced Throttling Algorithms**: Dynamic rate limiting based on system load
2. **Symlink Resolution**: Following symlinks vs watching link itself
3. **Mount Point Detection**: Handling filesystem boundaries
4. **Network Filesystem Support**: Special handling for NFS, CIFS
5. **fanotify Support**: For system-wide monitoring with CAP_SYS_ADMIN
