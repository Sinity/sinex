use super::*;
use serde_json::json;

#[test]
fn explicit_references_keep_order_and_exclude_query_text() {
    let rows: Rows =
        serde_json::from_value(json!({"references":["sinex:ev/second","sinex:ev/first"]})).unwrap();
    rows.validate().unwrap();
    assert_eq!(
        rows.references.as_deref(),
        Some(["sinex:ev/second".to_owned(), "sinex:ev/first".to_owned()].as_slice())
    );
    let both: Rows =
        serde_json::from_value(json!({"selection":"events", "references":["sinex:ev/first"]}))
            .unwrap();
    assert_eq!(both.validate().unwrap_err().kind, FailureKind::BadRequest);
}

#[test]
fn full_reads_do_not_silently_ignore_trace_paging_options() {
    let full: Rows = serde_json::from_value(
        json!({"selection":"events", "steps":[{"verb":"read", "cursor":"page"}]}),
    )
    .unwrap();
    assert_eq!(full.validate().unwrap_err().kind, FailureKind::BadRequest);
    let trace: Rows = serde_json::from_value(json!({"selection":"events", "steps":[{"verb":"read", "view":"trace", "cursor":"page", "page_size":2}]})).unwrap();
    trace.validate().unwrap();
}
