use super::{exact_vector, payload_strings};
use serde_json::json;

#[test]
fn unavailable_vector_is_distinct_from_exact_empty_and_preserves_text() {
    assert_eq!(exact_vector(&[String::new()]), Some(String::new()));
    let payload = json!({"command":"repeat ".repeat(257),"cwd":"/synthetic/late"});
    let strings = payload_strings(&payload);
    assert_eq!(strings[0], payload["command"]);
    assert!(strings.iter().any(|s| s == "/synthetic/late"));
    assert_eq!(exact_vector(&strings), None);
    assert!(exact_vector(&["repeat ".repeat(256)]).is_some());
}

#[test]
fn all_existing_exact_text_bounds_and_field_gaps_remain() {
    let positional = (0..16000)
        .map(|n| format!("w{n}"))
        .collect::<Vec<_>>()
        .join(" ");
    assert!(exact_vector(&[positional.clone()]).is_some());
    assert_eq!(exact_vector(&[format!("{positional} extra")]), None);
    assert!(exact_vector(&["a".repeat(900)]).is_some());
    assert_eq!(exact_vector(&["a".repeat(901)]), None);
    assert_eq!(exact_vector(&["é".repeat(451)]), None);
    let vector = exact_vector(&["first".into(), "second".into()]).unwrap();
    assert_eq!(vector, "'x6669727374':1 'x7365636f6e64':3");
}
