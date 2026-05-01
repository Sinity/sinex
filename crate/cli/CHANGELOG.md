# Changelog - sinex-cli

## Phase 3.3-HARDENING (2026-01-16)

### Phase A: Critical Reliability ✅

**Retry Logic & Error Handling:**
- Added exponential backoff retry for RPC calls (3 attempts, configurable)
- Smart error detection (retries 5xx, timeouts; skips 4xx auth errors)
- Enhanced error messages with troubleshooting guides
- Context-aware suggestions (e.g., "Use 'sinexctl node list'")

**Validation & Safety:**
- Comprehensive input validation module
- Interactive DLQ purge confirmation (inquire)
- Audit command 404 handling (no more crashes)

**Tests:** 21 tests (15 unit + 6 integration)

### Phase B: Consistency & Refactoring ✅

**Code Quality:**
- JSON utility helpers (get_str, get_i64, etc.)
- Reduced 15+ instances of code duplication
- Standardized output formatting module
- Validation for limits, offsets, URLs, time ranges

**Tests:** 40 tests (34 unit + 6 integration)

### Advanced Features ✅

**Structured Output:**
- JSON/YAML formatting for machine-readable output
- Plain terminal rendering without an unused syntax-highlighter dependency

**Interactive TUI:**
- Full terminal dashboard (ratatui + crossterm)
- 4 tabs: Dashboard, Replay, Events, DLQ
- Keyboard navigation (Tab/arrows, q, r)
- Foundation for live monitoring

**Tests:** 44 tests (38 unit + 6 integration)

### Summary

**Before:** Functional MVP with basic CRUD
**After:** Production-ready CLI with:
- ✅ Robust error handling & retries
- ✅ Structured JSON/YAML output
- ✅ Interactive TUI dashboard
- ✅ Comprehensive validation
- ✅ Clean, tested codebase

**Lines of Code:**
- +2,600 lines across 25 files
- 44 passing tests
- Zero clippy warnings
