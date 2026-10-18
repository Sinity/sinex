use super::{bind, explicit_raw_events};
use crate::retrieval::{Expr, Field, Operator};

#[test]
fn quoted_operator_tokens_are_search_text() {
    for text in [
        r#"commands where cwd "exists""#,
        r#"commands where command "=" "git status""#,
    ] {
        assert!(bind(text).is_err(), "{text}");
    }
    for text in [
        r#"commands where command = "git status""#,
        r#"commands where cwd = "exists""#,
        r#"commands where command ~ "=""#,
    ] {
        assert!(bind(text).is_ok(), "{text}");
    }
}

#[test]
fn raw_event_queries_are_detected_only_from_unquoted_fields() {
    assert!(explicit_raw_events(
        r#"events where payload.exit_code = "bad""#
    ));
    assert!(explicit_raw_events(
        r#"events where source = terminal.atuin-history and payload.exit_code = "bad""#
    ));
    assert!(!explicit_raw_events(
        r#"events where "payload.exit_code = bad""#
    ));
    assert!(!explicit_raw_events(
        "events where command = payload.exit_code"
    ));
}

#[test]
fn full_query_parses_and_bad_fields_are_rejected() {
    let b = bind(
        "commands where (cargo or \"git push\") and project = tide and when >= 2026-09-14T00:00:00Z sort when desc limit 20",
    )
    .unwrap();
    assert_eq!(b.sort, Some(true));
    assert!(bind("commands where prject = x").is_err());
    assert!(bind("commands where exit_code = nope").is_err());
}

#[test]
fn when_values_are_normalized_to_utc() {
    let b = bind("commands where when >= 2026-09-14T02:00:00+02:00").unwrap();
    let Some(Expr::Compare { field, op, value }) = b.filter else {
        panic!("comparison");
    };
    assert_eq!((field, op), (Field::When, Operator::Ge));
    assert_eq!(value, "2026-09-14T00:00:00Z");
}

#[test]
fn when_outside_the_representable_range_is_an_error_not_a_panic() {
    for text in [
        "commands where when >= 0000-01-01T00:00:00+01:00",
        "commands where when >= 9999-12-31T23:30:00-01:00",
    ] {
        assert!(bind(text).is_err(), "{text}");
    }
}
