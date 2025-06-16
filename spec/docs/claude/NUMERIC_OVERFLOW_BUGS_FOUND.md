# Numeric Overflow and Arithmetic Bugs Found in Sinex

## Critical Bugs

### 1. ULID Timestamp Conversion Overflow
**Location**: `crate/sinex-ulid/src/lib.rs:66`
```rust
DateTime::from_timestamp_millis(self.0.timestamp_ms() as i64)
```
**Issue**: `timestamp_ms()` returns `u64` but is cast to `i64` without checking. For timestamps with the high bit set (year ~8921 onwards), this causes integer overflow and returns wrong dates.

**Fix**:
```rust
pub fn timestamp(&self) -> DateTime<Utc> {
    let millis = self.0.timestamp_ms();
    if millis > i64::MAX as u64 {
        // Handle overflow - either panic or return max valid date
        return DateTime::from_timestamp_millis(i64::MAX).unwrap();
    }
    DateTime::from_timestamp_millis(millis as i64)
        .unwrap_or_else(|| Utc::now())
}
```

### 2. Monotonic ULID Counter Edge Case
**Location**: `crate/sinex-ulid/src/monotonic.rs:57`
```rust
if counter == u32::MAX {
    // Wait for next millisecond to avoid overflow
    std::thread::sleep(std::time::Duration::from_millis(1));
```
**Issue**: When counter reaches MAX, sleeping breaks monotonicity guarantee. Events generated during sleep are delayed.

**Fix**: Use u64 counter or handle wraparound properly with error return.

### 3. Clipboard Copy Count Overflow
**Location**: `crate/sinex-events/src/clipboard.rs:547`
```rust
entry.copy_count += 1;
```
**Issue**: `copy_count` is `u32` and can overflow after 4.3 billion copies, wrapping to 0.

**Fix**:
```rust
entry.copy_count = entry.copy_count.saturating_add(1);
```

### 4. Database Retry Counter Overflow
**Locations**: 
- `crate/sinex-db/src/queries.rs:270`
- `crate/sinex-db/src/queries.rs:500`
- `crate/sinex-db/src/queries.rs:905`

```sql
attempts = attempts + 1
```
**Issue**: SQL increment without overflow protection. After INT_MAX attempts, wraps to negative.

**Fix**: Use `LEAST(attempts + 1, max_attempts)` or check in application code.

### 5. Process ID Truncation
**Location**: `crate/sinex-ulid/src/monotonic.rs:24`
```rust
let process_id = std::process::id() as u16;
```
**Issue**: Process IDs >65535 get truncated, causing collisions.

**Fix**:
```rust
let process_id = (std::process::id() & 0xFFFF) as u16;
// Or use full u32 and adjust ULID encoding
```

### 6. File Size Casting Issues
**Location**: `crate/sinex-events/src/clipboard.rs:604`
```rust
size_bytes: content.len() as i64,
```
**Issue**: On 32-bit systems, `usize` can be smaller than `i64`. Large sizes could truncate.

**Fix**:
```rust
size_bytes: content.len().try_into().unwrap_or(i64::MAX),
```

## Medium Severity

### 7. Unchecked Batch Size Conversion
**Location**: `crate/sinex-worker/src/worker.rs:70`
```rust
self.processor.batch_size() as i64,
```
**Issue**: No validation that batch_size is reasonable. Huge values cause memory issues.

**Fix**: Add bounds checking and use saturating conversion.

### 8. Duration Precision Loss
**Multiple locations**: Converting `Duration` to integer seconds loses fractional parts.
**Fix**: Use `as_secs_f64()` where precision matters.

### 9. Metric Counter Overflow
**Issue**: Long-running processes can overflow metric counters.
**Fix**: Use gauge metrics that can be reset, or handle wraparound.

## Low Severity

### 10. Theoretical SQL Injection Risk
**Issue**: While the codebase uses prepared statements correctly, any future dynamic query construction would be risky.
**Fix**: Continue using `sqlx::query!` macros exclusively.

## Recommendations

1. **Enable overflow checks in release builds** for critical paths:
   ```toml
   [profile.release]
   overflow-checks = true
   ```

2. **Use saturating/checked arithmetic** for all counters:
   ```rust
   count.saturating_add(1)
   value.checked_add(1).unwrap_or(u32::MAX)
   ```

3. **Add property-based tests** for numeric edge cases:
   ```rust
   #[test]
   fn test_no_overflow(x: u32, y: u32) {
       let _ = x.checked_add(y); // Should handle gracefully
   }
   ```

4. **Validate all external inputs** before arithmetic operations.

5. **Use appropriate types**: Consider `u64` for counters that increment forever.

## Test Coverage

The test file `test/adversarial/numeric_overflow_bugs_test.rs` demonstrates each bug with failing test cases that should be fixed.