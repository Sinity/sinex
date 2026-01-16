# E2: Native Bindings Evaluation Decision

## Date
2026-01-16

## Decision
**Keep consolidated subprocess approach (Option C)** instead of implementing native libsystemd bindings.

## Context
Phase 2.2 Task E2 required evaluating native bindings (libsystemd-sys) versus the subprocess approach for journal watching.

## Research Findings

### libsystemd-sys Status
- **Crate:** `libsystemd-sys` at https://crates.io/crates/libsystemd-sys
- **Maintenance:** Uncertain - last update information not clearly visible
- **Downloads:** 3.5M+ (shows usage)
- **Dependencies:** 878 projects depend on it
- **Concerns:**
  - Maintenance status unclear
  - Last commit dates not visible in documentation
  - Open issues (20) and PRs (7) present but response time unknown

### Current Implementation Status
- **Before E1:** 2 separate `journalctl` processes (journal + systemd watchers)
- **After E1:** 1 unified `journalctl` process serving both purposes
- **Improvement:** 50% reduction in subprocess count
- **Benefits:** Simpler lifecycle, unified cursor tracking, single I/O stream

## Decision Rationale

1. **Risk vs. Reward**
   - Native bindings add C library dependency and FFI complexity
   - Uncertain maintenance status increases long-term risk
   - Marginal performance gain vs. increased maintenance burden

2. **Already Achieved Goals**
   - E1 consolidation reduced process overhead by 50%
   - Single subprocess is manageable with proper supervision
   - Cursor tracking and crash recovery work correctly

3. **Maintenance Simplicity**
   - Subprocess approach: well-understood, easy to debug
   - FFI approach: requires C library, harder to debug, potential ABI issues
   - Team familiarity: subprocess approach is standard practice

4. **Future Options**
   - Decision can be revisited if:
     - libsystemd-sys shows clear active maintenance
     - Performance profiling identifies journal processing as bottleneck
     - Rust-native journal libraries mature (e.g., pure-Rust alternatives)

## Alternatives Considered

### Option A: libsystemd-sys FFI bindings
- **Pros:** No subprocess, direct API access, potentially lower latency
- **Cons:** C library dependency, FFI complexity, uncertain maintenance
- **Status:** Rejected due to risk

### Option B: Pure Rust journal reader
- **Pros:** No dependencies, full control
- **Cons:** Very complex (binary journal format), high implementation cost
- **Status:** Rejected - complexity not justified

### Option C: Consolidated subprocess (SELECTED)
- **Pros:** Already implemented (E1), well-tested, simple, maintainable
- **Cons:** Subprocess overhead (but 50% reduced from before)
- **Status:** Selected as pragmatic choice

## Implementation
- Continue using `UnifiedJournalWatcher` from E1
- Monitor for future improvements in Rust systemd ecosystem
- Document subprocess supervision patterns for reliability

## Sources
- [libsystemd-sys crate](https://crates.io/crates/libsystemd-sys)
- [rust-systemd GitHub](https://github.com/codyps/rust-systemd)
- [libsystemd pure-Rust alternative](https://crates.io/crates/libsystemd)
