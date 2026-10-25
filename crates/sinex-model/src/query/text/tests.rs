use super::*;
use serde_json::json;

#[devkit::test]
fn words_fold_case_and_normalize() {
    assert_eq!(words("Cargo TEST"), ["cargo", "test"]);
    // "é" decomposed and composed normalize to the same word.
    assert_eq!(words("cafe\u{301}"), words("café"));
    assert_eq!(words("Straße"), ["strasse"]);
}

#[devkit::test]
fn payload_text_takes_strings_not_keys() {
    let p = json!({"command": "Cargo test", "cwd": "/src/tide", "exit_code": 1, "tags": ["Fast"]});
    assert_eq!(payload_text(&p), "cargo test src tide fast");
}

#[devkit::test]
fn escapes() {
    assert_eq!(regex_escape("a.b*"), r"a\.b\*");
    assert_eq!(like_escape("50%_x\\"), r"50\%\_x\\");
}
