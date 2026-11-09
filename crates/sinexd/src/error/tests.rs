use super::*;

#[test]
fn a_file_error_keeps_its_kind_and_path() {
    let missing = Error::file(
        "/srv/inputs/history.db",
        std::io::Error::from(std::io::ErrorKind::NotFound),
    );
    assert!(missing.to_string().starts_with("/srv/inputs/history.db: "));
    assert_eq!(missing.failure().kind, FailureKind::NotFound);
    let forbidden = Error::from(std::io::Error::from(std::io::ErrorKind::PermissionDenied));
    assert_eq!(forbidden.failure().kind, FailureKind::Refused);
    let full = Error::from(std::io::Error::from(std::io::ErrorKind::StorageFull));
    assert_eq!(full.failure().kind, FailureKind::Io);
}

#[test]
fn a_query_error_is_placed_in_its_text_only_where_the_text_is_known() {
    let text = "events where nosuch = 1";
    let error = sinex_model::query::QueryError::at(13, "no field `nosuch`");
    let unplaced = Error::from(error.clone()).failure();
    assert_eq!(unplaced.kind, FailureKind::InvalidQuery);
    assert!(unplaced.caret().is_none());
    let placed = Error::from(sinex_store::Error::Query(error))
        .in_query(text)
        .failure();
    assert_eq!(placed.position.map(|span| span.offset), Some(13));
    assert!(placed.caret().unwrap().starts_with(text));
}

#[test]
fn a_note_keeps_the_kind_and_retryability() {
    let error = Error::from(sqlx::Error::PoolTimedOut).with_note("backup hold 7 remains open");
    let failure = error.failure();
    assert_eq!(failure.kind, FailureKind::Timeout);
    assert!(failure.retryable);
    assert!(failure.message.ends_with("; backup hold 7 remains open"));
}
