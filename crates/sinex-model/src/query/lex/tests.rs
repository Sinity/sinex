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

#[devkit::test]
fn colons_are_symbols_between_names_even_with_spaces_or_capitals() {
    for text in [
        "query:by-directory",
        "QUERY:by-directory",
        "QUERY : by-directory",
        "query :by-directory",
        "query: by-directory",
    ] {
        let input = tokens(text).unwrap();
        assert!(input[0].is_word("query"));
        assert!(input[1].is_symbol(":"));
        assert_eq!(input[2].text, "by-directory");
    }
    assert_eq!(texts("recall 14:05 ±20m"), ["recall", "14:05", "±20m"]);
    assert_eq!(
        texts("show sinex:at/2026-09-14T08:01:00Z/shell.session"),
        ["show", "sinex:at/2026-09-14T08:01:00Z/shell.session"]
    );
}

#[devkit::test]
fn only_lowercase_names_split_field_colons() {
    for text in ["TODO:", "Word:value", "SOURCE:value"] {
        assert_eq!(texts(text), [text]);
    }
    assert_eq!(texts("source:value"), ["source", ":", "value"]);
    assert_eq!(texts("SINEX:ev/0190"), ["SINEX:ev/0190"]);
    assert_eq!(
        texts("QUERY:shared@operator"),
        ["QUERY", ":", "shared@operator"]
    );
}
