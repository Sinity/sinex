use super::payload_strings;
use serde_json::json;
use sinex_model::retrieval::token_text;

#[test]
fn token_text_keeps_long_and_empty_strings_whole() {
    assert_eq!(token_text(&[String::new()]), Some(String::new()));
    let payload = json!({"command":format!("{}late neighbor", "repeat ".repeat(290)),"cwd":"/synthetic/late"});
    let strings = payload_strings(&payload);
    assert_eq!(strings[0], payload["command"]);
    assert!(strings.iter().any(|s| s == "/synthetic/late"));
    let tokens = token_text(&strings).unwrap();
    assert_eq!(
        tokens.split('\u{1}').filter(|s| *s == "repeat").count(),
        290
    );
    assert!(tokens.contains("\u{1}late\u{1}neighbor\u{1}"));
    let positional = (0..17000)
        .map(|n| format!("w{n}"))
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        token_text(&[positional])
            .unwrap()
            .contains("\u{1}w16999\u{1}")
    );
    assert!(
        token_text(&["a".repeat(1100)])
            .unwrap()
            .contains(&"a".repeat(1100))
    );
}

#[test]
fn token_text_separates_fields_and_drops_literal_delimiters() {
    assert_eq!(
        token_text(&["first".into(), "second".into()]).unwrap(),
        "\u{1}first\u{1}\u{2}\u{1}second\u{1}\u{2}"
    );
    assert_eq!(
        token_text(&["first\u{1}second\u{2}third".into()]).unwrap(),
        "\u{1}first\u{1}second\u{1}third\u{1}\u{2}"
    );
    assert_eq!(
        token_text(&["Straße CAFE\u{301}".into()]).unwrap(),
        "\u{1}strasse\u{1}café\u{1}\u{2}"
    );
}
