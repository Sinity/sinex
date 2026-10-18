use super::{Text, bind, token_text};

#[test]
fn like_patterns_escape_literal_wildcards_and_backslashes() {
    let text = Text {
        words: vec!["a%b_c\\d".into()],
        prefix: false,
        phrase: false,
    };
    assert_eq!(text.like_patterns(), ["%\u{1}a\\%b\\_c\\\\d\u{1}%"]);
    let prefix = Text {
        prefix: true,
        ..text
    };
    assert_eq!(prefix.like_patterns(), ["%\u{1}a\\%b\\_c\\\\d%"]);
    let phrase = Text {
        words: vec!["first".into(), "second".into()],
        prefix: false,
        phrase: true,
    };
    assert_eq!(phrase.like_patterns(), ["%\u{1}first\u{1}second\u{1}%"]);
}

#[test]
fn query_delimiters_are_tokenized_and_negative_text_never_supplies_rank() {
    let literal = "Straße\u{1}CAFE\u{301}\u{2}later";
    let query = format!(
        "commands where {} and not hidden and exit_code != 0",
        serde_json::to_string(literal).unwrap()
    );
    let bound = bind(&query).unwrap();
    assert!(bind(&format!("commands where \"{literal}\"")).is_err());
    let filter = bound.filter.unwrap();
    assert_eq!(filter.rank_text().unwrap(), "strasse café later");
    let encoded = token_text(&["Straße\u{1}CAFE\u{301}\u{2}later".into()]).unwrap();
    assert_eq!(encoded, "\u{1}strasse\u{1}café\u{1}later\u{1}\u{2}");
    assert!(
        bind("commands where not hidden")
            .unwrap()
            .filter
            .unwrap()
            .rank_text()
            .is_none()
    );
    assert!(
        bind("commands where project = tide")
            .unwrap()
            .filter
            .unwrap()
            .rank_text()
            .is_none()
    );
}
