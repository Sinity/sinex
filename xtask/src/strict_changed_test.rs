use super::*;

#[test]
fn failure_excerpt_keeps_tail_diagnostics_after_long_prefix() {
    let stdout = (0..140)
        .map(|i| format!("progress line {i}"))
        .collect::<Vec<_>>()
        .join("\n");
    let stderr = "error[E0425]: cannot find value `missing` in this scope\n  --> src/lib.rs:12:5";

    let excerpt = failure_output_excerpt(&stdout, stderr);

    assert!(
        !excerpt.contains("progress line 0"),
        "long progress prefix should be trimmed: {excerpt}"
    );
    assert!(
        excerpt.contains("error[E0425]"),
        "tail diagnostics should survive truncation: {excerpt}"
    );
    assert!(
        excerpt.contains("[... "),
        "trimmed excerpts should say earlier output was omitted: {excerpt}"
    );
}

#[test]
fn failure_excerpt_keeps_short_output_unchanged() {
    let excerpt = failure_output_excerpt("checking pkg", "error: failed");

    assert_eq!(excerpt, "checking pkg\nerror: failed");
}
