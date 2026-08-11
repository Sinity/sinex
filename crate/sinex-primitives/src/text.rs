//! Shared, UTF-8-safe string truncation primitives.
//!
//! Truncating a string for display is not one operation — this module
//! separates the three that actually occur in this codebase so callers stop
//! reimplementing (and occasionally getting wrong) whichever one they need:
//!
//! - [`truncate_chars`]: budget in *characters* (e.g. a fixed-width table
//!   column measured in codepoints, ellipsis appended).
//! - [`truncate_bytes`]: budget in *bytes* (e.g. a wire/log size cap), no
//!   ellipsis — the caller owns whatever truncation-visible convention it
//!   wants.
//! - [`truncate_cols`]: budget in terminal *display columns* (CJK/emoji-aware
//!   — a wide character occupies two columns), ellipsis appended.
//! - [`prefix_chars`]: a char-boundary-safe prefix of the first `n`
//!   characters, no ellipsis — for id/token previews.
//!
//! All four are safe on arbitrary UTF-8 input: none of them ever slices at a
//! raw byte offset that could land mid-codepoint.

use std::borrow::Cow;

/// Truncate to at most `max_chars` characters, appending `"..."` when
/// truncation occurs. The ellipsis itself counts against `max_chars`.
#[must_use]
pub fn truncate_chars(input: &str, max_chars: usize) -> Cow<'_, str> {
    if input.chars().count() <= max_chars {
        return Cow::Borrowed(input);
    }
    let keep = max_chars.saturating_sub(3);
    let end = input
        .char_indices()
        .nth(keep)
        .map_or(input.len(), |(index, _)| index);
    Cow::Owned(format!("{}...", &input[..end]))
}

/// Truncate to at most `max_bytes` bytes, clamping back to the nearest char
/// boundary at or before the limit. No ellipsis — for log/wire size caps
/// where the byte budget is a hard constraint, not a display convention.
#[must_use]
pub fn truncate_bytes(input: &str, max_bytes: usize) -> &str {
    if input.len() <= max_bytes {
        return input;
    }
    let mut end = max_bytes;
    while end > 0 && !input.is_char_boundary(end) {
        end -= 1;
    }
    &input[..end]
}

/// Truncate to at most `max_cols` terminal display columns (a CJK/wide
/// character occupies two columns), appending an ellipsis when truncation
/// occurs. This is the correct primitive for any fixed-width table cell or
/// terminal-rendered text — [`truncate_chars`] undercounts display width for
/// wide characters.
///
/// Delegates to [`console::truncate_str`], guarding the case where
/// `max_cols` is smaller than the ellipsis's own display width (that
/// combination underflows inside `console`'s implementation).
#[must_use]
pub fn truncate_cols(input: &str, max_cols: usize) -> Cow<'_, str> {
    const TAIL: &str = "...";
    let tail_width = console::measure_text_width(TAIL);
    if max_cols <= tail_width {
        // No room for any content plus the ellipsis — fall back to a bare
        // (unellipsized) column-budget cut so we still return something
        // useful rather than tripping console's internal underflow.
        return console::truncate_str(input, max_cols, "");
    }
    console::truncate_str(input, max_cols, TAIL)
}

/// A char-boundary-safe prefix of the first `n` characters, no ellipsis —
/// for id/token/hash previews (e.g. a log-friendly short form of an opaque
/// identifier). Replaces the `&x[..8.min(x.len())]` byte-index shape found
/// repeatedly across the workspace, which panics on non-ASCII input.
#[must_use]
pub fn prefix_chars(input: &str, n: usize) -> &str {
    let end = input
        .char_indices()
        .nth(n)
        .map_or(input.len(), |(index, _)| index);
    &input[..end]
}

#[cfg(test)]
mod tests {
    use super::{prefix_chars, truncate_bytes, truncate_chars, truncate_cols};

    #[test]
    fn truncate_chars_ascii_under_budget_is_unchanged() {
        assert_eq!(truncate_chars("hello", 10), "hello");
    }

    #[test]
    fn truncate_chars_ascii_over_budget_truncates_with_ellipsis() {
        assert_eq!(truncate_chars("hello world", 8), "hello...");
    }

    #[test]
    fn truncate_chars_multibyte_straddling_cut_point_does_not_panic() {
        // Each emoji is 4 bytes / 1 char; a byte-index cut at the naive
        // "max_chars" byte offset would land mid-codepoint.
        let s = "😀😀😀😀😀"; // 5 chars, 20 bytes
        let out = truncate_chars(s, 3);
        assert!(out.chars().count() <= 3);
        assert!(out.ends_with("..."));
        // Must be valid UTF-8 (guaranteed by type, but assert no panic path
        // was taken by checking char boundaries survived).
        assert!(std::str::from_utf8(out.as_bytes()).is_ok());
    }

    #[test]
    fn truncate_chars_exact_boundary_is_unchanged() {
        assert_eq!(truncate_chars("hello", 5), "hello");
    }

    #[test]
    fn truncate_bytes_never_splits_a_codepoint() {
        let s = "😀😀😀"; // 4 bytes each, 12 bytes total
        assert_eq!(truncate_bytes(s, 5), "😀");
        assert_eq!(truncate_bytes(s, 12), s);
        assert_eq!(truncate_bytes(s, 100), s);
        assert_eq!(truncate_bytes(s, 0), "");
        assert_eq!(truncate_bytes("ascii text", 4), "asci");
    }

    #[test]
    fn truncate_cols_ascii_under_budget_is_unchanged() {
        assert_eq!(truncate_cols("hello", 10), "hello");
    }

    #[test]
    fn truncate_cols_cjk_is_width_aware_not_char_count_aware() {
        // Each CJK character occupies 2 display columns. "你好世界" is 4
        // chars / 8 columns. A char-count-based truncate would keep all 4
        // chars under a "max_chars=4" budget; a column-aware one must not,
        // once the column budget is exceeded.
        let s = "你好世界"; // 4 chars, 8 display columns
        let out = truncate_cols(s, 6);
        assert!(console::measure_text_width(&out) <= 6);
        // Sanity: does not panic and produces valid UTF-8.
        assert!(std::str::from_utf8(out.as_bytes()).is_ok());
    }

    #[test]
    fn truncate_cols_budget_smaller_than_ellipsis_does_not_panic() {
        // Regression guard for the console::truncate_str underflow this
        // module works around: max_cols <= the ellipsis's own width.
        let out = truncate_cols("hello world", 2);
        assert!(console::measure_text_width(&out) <= 2);
    }

    #[test]
    fn truncate_cols_multibyte_straddling_cut_point_does_not_panic() {
        let s = "😀😀😀😀😀";
        let out = truncate_cols(s, 4);
        assert!(std::str::from_utf8(out.as_bytes()).is_ok());
    }

    #[test]
    fn prefix_chars_ascii() {
        assert_eq!(prefix_chars("hello world", 5), "hello");
        assert_eq!(prefix_chars("hi", 5), "hi");
    }

    #[test]
    fn prefix_chars_multibyte_does_not_panic() {
        // This is the exact shape that panicked in rate_limit.rs/rpc_server.rs:
        // &token[..8.min(token.len())] on a token containing multi-byte UTF-8
        // within the first 8 bytes.
        let token = "İstanbul-secret-token"; // 'İ' is 2 bytes
        let out = prefix_chars(token, 8);
        assert!(std::str::from_utf8(out.as_bytes()).is_ok());
        assert_eq!(out.chars().count(), 8);
    }

    #[test]
    fn prefix_chars_exact_boundary() {
        assert_eq!(prefix_chars("hello", 5), "hello");
        assert_eq!(prefix_chars("hello", 100), "hello");
    }
}
