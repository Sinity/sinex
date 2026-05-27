# Macro Usage Audit

**Status**: 2026-01-24

## Usage Summary

| Macro | Usage Count | Status | Notes |
|-------|-------------|--------|-------|
| `#[derive(EventPayload)]` | Active | ✅ In production | Implemented in this crate and consumed across payload types. |

## Recommendations

- Keep `EventPayload` derive minimal, tested, and documented.
- Keep this crate focused on proc-macro-only concerns.
- If additional macros are introduced, document them in `README.md` and add targeted tests in `tests/`.
