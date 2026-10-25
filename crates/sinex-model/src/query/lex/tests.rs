use super::*;

fn texts(q: &str) -> Vec<String> {
    tokens(q).unwrap().into_iter().map(|t| t.text).collect()
}

#[devkit::test]
fn splits_symbols_fields_and_quotes() {
    assert_eq!(
        texts(r#"commands where exit_code != 0 and command ~ "cargo test""#),
        [
            "commands",
            "where",
            "exit_code",
            "!=",
            "0",
            "and",
            "command",
            "~",
            "cargo test"
        ]
    );
    assert_eq!(texts("source:terminal.*"), ["source", ":", "terminal.*"]);
    assert_eq!(
        texts("kind = [a, b]"),
        ["kind", "=", "[", "a", ",", "b", "]"]
    );
    assert_eq!(texts("recall 14:05 ±20m"), ["recall", "14:05", "±20m"]);
    assert_eq!(texts("trace sinex:ev/0190"), ["trace", "sinex:ev/0190"]);
    assert_eq!(
        texts("when > 2026-03-02T14:05:00+01:00"),
        ["when", ">", "2026-03-02T14:05:00+01:00"]
    );
}

#[devkit::test]
fn quoted_words_are_never_keywords() {
    let t = tokens(r#""recall""#).unwrap();
    assert_eq!(t[0].kind, Kind::Quoted);
    assert!(!t[0].is_word("recall"));
}

#[devkit::test]
fn refuses_a_bad_quote() {
    assert!(tokens(r#"a "unterminated"#).is_err());
}
