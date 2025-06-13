# Additional Adversarial Tests Needed for Sinex

## 🎯 Event-Type-Specific Tests

### Filesystem Event Attacks
```rust
test_filesystem_event_symlink_loop()
- Create A -> B -> C -> A symlink loop
- Watch directory containing loop
- Infinite recursion in watcher?

test_filesystem_unicode_filename_collision()
- Create files: "test.txt" and "tëst.txt" and "test\u0301.txt"
- Different normalization forms of same logical name
- Events might be duplicated or lost

test_filesystem_case_folding_attack()
- On case-insensitive FS: create "File.txt"
- Rapidly create/delete "file.txt", "FILE.txt", "FiLe.txt"
- Race conditions in case normalization

test_filesystem_hardlink_explosion()
- Create file with 65000 hardlinks (near ext4 limit)
- Modify original file
- Should generate 65000 events? System meltdown?

test_filesystem_watch_fifo_write_block()
- Watch a FIFO/named pipe
- Write to FIFO without reader
- Watcher might block forever
```

### Terminal Event Attacks
```rust
test_terminal_ansi_escape_injection()
- Log malicious ANSI: "\x1b[3J\x1b[H\x1b[2J"
- Clears scrollback, resets cursor
- When replayed, corrupts terminal

test_terminal_output_speed_dos()
- Generate 1GB/sec of terminal output
- Continuous "yes" command or /dev/urandom
- Event system should handle gracefully

test_terminal_pty_exhaustion()
- Open 1000+ PTYs rapidly
- Each spawns shell and logs
- System runs out of PTYs

test_terminal_control_char_smuggling()
- Embed ^C, ^Z, ^D in output
- When events replayed, kills processes
- Security boundary violation

test_terminal_utf8_overlong_encoding()
- Send overlong UTF-8 sequences
- e.g., 0xC0 0x80 for NULL
- Parser inconsistencies
```

### Window Manager Event Attacks
```rust
test_window_negative_geometry()
- Create window at (-65536, -65536)
- Size: -100 x -100
- Integer underflow in calculations

test_window_id_wraparound()
- Create/destroy windows until ID wraps
- Old events might reference new windows
- State confusion

test_workspace_switch_flood()
- Switch workspaces 1000 times/second
- Every switch generates events
- Event queue overflow

test_window_recursive_parent()
- Set window A parent to B
- Set window B parent to A
- Circular reference in window tree

test_window_title_binary_data()
- Set window title to raw binary
- Include null bytes, control chars
- Storage/retrieval corruption
```

## 🔄 Cross-Event-Type Interactions

```rust
test_filesystem_triggers_terminal_chaos()
- Watch directory with 10000 files
- Each file change spawns terminal command
- Terminal events flood from filesystem events

test_window_close_during_terminal_output()
- Terminal generating massive output
- Close terminal window mid-stream
- Orphaned events? Crashes?

test_circular_event_generation()
- Filesystem watcher on ~/.config
- Terminal logger writes to ~/.config/log
- Each write triggers filesystem event
- Infinite feedback loop

test_event_type_confusion_attack()
- Send filesystem event to terminal source
- Send window event to filesystem source
- Type safety violations

test_cross_source_timestamp_ordering()
- Generate events from all sources simultaneously
- Each with slightly different clock
- Ordering violations across sources
```

## 🧬 Schema & Evolution Attacks

```rust
test_schema_version_downgrade()
- Insert v2 schema events
- Restart with v1 schema validator
- Schema compatibility broken

test_schema_field_type_mutation()
- Event with {"size": 123}
- Later: {"size": "123 bytes"}
- Type confusion in queries

test_partial_schema_migration()
- Start schema migration
- Kill process at 50%
- Database in inconsistent state

test_schema_registry_corruption()
- Delete random schema from registry
- Existing events become unvalidated
- Queries might fail
```

## 💾 Storage & Persistence Attacks

```rust
test_event_replay_with_modified_timestamps()
- Export events from yesterday
- Modify timestamps to future
- Re-import
- Time travel paradox

test_compression_ratio_attack()
- Create highly compressible events
- 1GB -> 1KB when compressed
- Then incompressible events
- Storage predictions fail

test_chunk_rotation_during_query()
- Configure 1-minute chunks
- Run 2-minute aggregation query
- Chunk rotates mid-query
- Inconsistent results

test_transaction_log_overflow()
- Single transaction with 10M events
- Transaction log grows unbounded
- Disk space exhaustion
```

## 🔐 Advanced Security Scenarios

```rust
test_event_content_exfiltration()
- Hide data in event metadata
- Use timing of events as covert channel
- Steganography via event patterns

test_privilege_escalation_via_events()
- Low-priv user creates specific events
- Events trigger high-priv actions
- Privilege boundary bypass

test_event_based_port_scanning()
- Generate events that trigger network calls
- Use timing to scan internal network
- Information disclosure
```

## 🎪 Chaos Engineering Scenarios

```rust
test_rolling_disaster_recovery()
- Continuous event generation
- Randomly kill components
- Randomly corrupt data
- System should self-heal

test_byzantine_collector_behavior()
- Collector sometimes lies about events
- Sometimes duplicates
- Sometimes drops
- Consensus despite Byzantine behavior

test_cascading_failure_simulation()
- Overload one component
- Should trigger controlled degradation
- Not cascading system failure
```

## 📊 Performance Attack Patterns

```rust
test_query_complexity_explosion()
- Nested CTEs with exponential growth
- Each level doubles complexity
- Single query using 100GB RAM

test_index_thrashing_attack()
- Insert events to fragment indexes
- Queries become 1000x slower
- Performance DoS

test_statistics_poisoning()
- Insert events to skew statistics
- Query planner makes bad decisions
- All queries become slow
```

## 🔧 Implementation Priority

1. **Critical**: Event-type-specific tests (filesystem, terminal, window)
2. **High**: Cross-event-type interactions
3. **Medium**: Schema evolution attacks
4. **Lower**: Advanced scenarios (chaos, Byzantine, etc.)

These tests would significantly improve coverage of Sinex-specific vulnerabilities and edge cases that generic tests miss.