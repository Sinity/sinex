use super::*;
use xtask::sandbox::prelude::sinex_test;

#[sinex_test]
async fn format_bytes_uses_binary_units() -> xtask::sandbox::TestResult<()> {
    assert_eq!(format_bytes(999), "999 B");
    assert_eq!(format_bytes(1536), "1.5 KiB");
    assert_eq!(format_bytes(10 * 1024), "10 KiB");
    Ok(())
}

#[sinex_test]
async fn format_duration_age_keeps_compact_age_shape() -> xtask::sandbox::TestResult<()> {
    assert_eq!(format_duration_age(time::Duration::seconds(62)), "1m2s ago");
    assert_eq!(
        format_duration_age(time::Duration::seconds(3660)),
        "1h1m ago"
    );
    Ok(())
}

#[sinex_test]
async fn format_duration_compact_secs_matches_report_shape() -> xtask::sandbox::TestResult<()> {
    assert_eq!(format_duration_compact_secs(47), "47s");
    assert_eq!(format_duration_compact_secs(120), "2m");
    assert_eq!(format_duration_compact_secs(198 * 60), "3h 18m");
    Ok(())
}

#[sinex_test]
async fn truncate_str_boundary_safe_never_splits_a_codepoint() -> xtask::sandbox::TestResult<()> {
    // Each emoji is 4 bytes; cutting at byte 5 must not panic and must land
    // on the boundary before the split codepoint (byte 4), not after it.
    let s = "😀😀😀"; // 12 bytes total
    assert_eq!(truncate_str_boundary_safe(s, 5), "😀");
    assert_eq!(truncate_str_boundary_safe(s, 12), s);
    assert_eq!(truncate_str_boundary_safe(s, 100), s);
    assert_eq!(truncate_str_boundary_safe(s, 0), "");
    assert_eq!(truncate_str_boundary_safe("ascii text", 4), "asci");
    Ok(())
}

#[sinex_test]
async fn truncate_str_boundary_safe_handles_every_utf8_codepoint_width()
-> xtask::sandbox::TestResult<()> {
    // The back-scan loop (`while !s.is_char_boundary(end) { end -= 1 }`) must
    // walk back the correct number of bytes for each UTF-8 width class, not
    // just the 4-byte case: 2-byte (Cyrillic, as in the real watch-summary
    // fixture), 3-byte (Japanese, as in the real tombstone-reason fixture),
    // and 4-byte (emoji) codepoints all straddle a naive byte cut differently.
    let two_byte = "привет"; // 'п' = 2 bytes; 12 bytes total, boundaries at even offsets only
    assert_eq!(truncate_str_boundary_safe(two_byte, 3), "п");
    assert_eq!(truncate_str_boundary_safe(two_byte, 1), "");

    let three_byte = "日本語"; // each char = 3 bytes; 9 bytes total
    assert_eq!(truncate_str_boundary_safe(three_byte, 4), "日");
    assert_eq!(truncate_str_boundary_safe(three_byte, 5), "日");
    assert_eq!(truncate_str_boundary_safe(three_byte, 6), "日本");
    assert_eq!(truncate_str_boundary_safe(three_byte, 2), "");

    let four_byte = "🎉🎊"; // each char = 4 bytes; 8 bytes total
    assert_eq!(truncate_str_boundary_safe(four_byte, 6), "🎉");
    assert_eq!(truncate_str_boundary_safe(four_byte, 3), "");

    // Mixed-width text (ASCII prefix + multi-byte suffix) is the realistic
    // tombstone-reason/watch-summary shape, not an isolated repeated glyph.
    let mixed = "log: 日本語 done"; // "log: " = 5 ASCII bytes, then 3-byte chars starting at byte 5
    assert_eq!(truncate_str_boundary_safe(mixed, 8), "log: 日");
    assert_eq!(truncate_str_boundary_safe(mixed, 7), "log: ");
    assert_eq!(truncate_str_boundary_safe(mixed, 6), "log: ");
    assert_eq!(truncate_str_boundary_safe(mixed, 5), "log: ");
    Ok(())
}
