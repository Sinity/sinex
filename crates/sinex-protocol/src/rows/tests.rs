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

#[cfg(feature = "surface")]
#[test]
fn read_arguments_have_one_strict_typed_owner() {
    let words = ["--view", "trace", "--page-size", "2"].map(str::to_owned);
    let parsed = steps().parse_step("read", &words).unwrap();
    let Step::Read(read) = serde_json::from_value::<Step>(parsed).unwrap();
    assert_eq!(read.view, ReadView::Trace);
    assert_eq!(read.page_size, Some(2));
    assert_eq!(read.cursor, None);
    assert!(serde_json::from_value::<Step>(json!({"verb":"read", "veiw":"trace"})).is_err());
    assert!(steps().parse_step("read", &["unexpected".into()]).is_err());
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
