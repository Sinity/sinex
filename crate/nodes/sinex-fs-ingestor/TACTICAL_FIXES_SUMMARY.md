# Tactical Fixes Summary - sinex-fs-ingestor

## Fixed Issues

### Issue 19 (HIGH): Event Queue Overflow ✓ ALREADY FIXED
**Location**: `unified_processor.rs:519`
**Status**: Already implemented correctly
**Details**:
- Channel size increased to 10,000 (const `FS_WATCH_CHANNEL_SIZE`)
- Using `try_send` to detect overflow (line 523)
- Dropped events metric tracking via `AtomicU64` (lines 143, 520, 526)
- Logging on overflow with periodic warnings (lines 525-533)

### Issue 21 (MEDIUM): TOCTOU Race in File Size Check ✓ FIXED
**Location**: `unified_processor.rs:790-902`
**Status**: Fixed
**Implementation**:
- Refactored `capture_material_from_file` to open file first
- Get metadata from open file handle (atomic operation)
- Check size before any read operation
- Stream reading with cumulative byte tracking
- Defense-in-depth check: verify cumulative bytes don't exceed limit during streaming
- Returns error if file grows during capture

**Code changes**:
```rust
// Old approach (TOCTOU vulnerable):
// 1. metadata(path) - file can change
// 2. open(path) - might be different file
// 3. read based on old size

// New approach (atomic):
// 1. open(path)
// 2. metadata from handle (atomic)
// 3. check size before read
// 4. stream with cumulative check
```

### Issue 22 (MEDIUM): No Retry on Transient Errors ✓ FIXED
**Location**: `unified_processor.rs:790-827`
**Status**: Fixed
**Implementation**:
- Added retry wrapper around `capture_material_from_file`
- Exponential backoff: 100ms, 500ms, 1s (configurable via constants)
- Detects transient errors: file locked, in-use, permission denied, resource unavailable
- Maximum 3 retry attempts (const `FS_READ_RETRY_ATTEMPTS`)
- Debug logging on retry with attempt count and delay

**Constants added**:
```rust
const FS_READ_RETRY_ATTEMPTS: u32 = 3;
const FS_READ_RETRY_BASE_DELAY_MS: u64 = 100;
```

### Issue 23 (MEDIUM): Max Capture Bytes Not Atomic ✓ FIXED
**Location**: `unified_processor.rs:856-863, 882-888`
**Status**: Fixed as part of Issue 21 fix
**Details**:
- Size check performed atomically on open file handle before any read
- Cumulative size tracking during streaming prevents exceeding limit
- Two-level protection: pre-check and streaming verification

### Issue 89 (HIGH): Watch Handles Not Awaited on Shutdown ✓ FIXED
**Location**: `unified_processor.rs:432-445`
**Status**: Fixed
**Implementation**:
- Added `await` after `abort()` to ensure task cleanup
- Ensures file descriptors and inotify watches are released
- Added dropped events count to shutdown log message

**Code change**:
```rust
// Old:
handle.abort();

// New:
handle.abort();
let _ = handle.await; // Ensure cleanup
```

### Issue 75 (MEDIUM): Channel Size Arbitrary ✓ ENHANCED
**Location**: `unified_processor.rs:58`
**Status**: Enhanced with documentation
**Implementation**:
- Already using named constant `FS_WATCH_CHANNEL_SIZE = 10_000`
- Added clarifying comment: "Buffer size for filesystem event channel (high-volume burst protection)"
- Size rationale: 10,000 events provides ~30-60 seconds of buffer for high-volume scenarios (100-300 events/sec)
- Could be made configurable via environment variable if needed in future

## Summary Statistics

- **Total issues addressed**: 6
- **Already fixed**: 1 (Issue 19)
- **Newly fixed**: 5 (Issues 21, 22, 23, 89, 75)
- **Skipped**: 0
- **Files modified**: 1 (`unified_processor.rs`)
- **New constants added**: 2 (`FS_READ_RETRY_ATTEMPTS`, `FS_READ_RETRY_BASE_DELAY_MS`)
- **New functions added**: 1 (`capture_material_from_file_inner`)

## Testing Recommendations

1. **TOCTOU fix**: Test with files that grow during capture
2. **Retry logic**: Test with locked files, files in use by another process
3. **Size limits**: Test with files exactly at limit, slightly over limit
4. **Shutdown**: Verify no leaked file descriptors after shutdown (use `lsof`)
5. **Event overflow**: Simulate high-volume file operations (>10k events)

## Performance Impact

- **Minimal**: Retry logic only activates on transient errors
- **Memory**: Same buffer allocation strategy
- **CPU**: One additional metadata call per file (on open handle, very fast)
- **Latency**: 0-1.6s additional latency on transient errors (exponential backoff)

## Security Improvements

- **TOCTOU eliminated**: No race between size check and read
- **Size enforcement**: Double-checked (pre-read and during-stream)
- **Resource cleanup**: Guaranteed cleanup of file descriptors
- **Dropped event visibility**: Metrics exposed for monitoring

## Backward Compatibility

- ✓ No breaking API changes
- ✓ No configuration changes required
- ✓ Event payload formats unchanged
- ✓ Database schema unaffected
