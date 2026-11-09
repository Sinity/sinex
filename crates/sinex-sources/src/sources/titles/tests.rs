use super::*;

#[test]
fn progress_glyphs_and_their_spaces_leave_the_text() {
    for (title, text) in [
        ("⣾ build", "build"),
        ("✳✶ two glyphs", "two glyphs"),
        ("◴ quarter", "quarter"),
        ("◐ Codex", "Codex"),
        ("· dot", "dot"),
        ("* asterisk alone", "asterisk alone"),
        ("*draft", "*draft"),
        ("⠋claude", "⠋claude"),
        ("plain", "plain"),
        ("⠋", ""),
    ] {
        assert_eq!(without_progress(title), text, "{title}");
    }
}
