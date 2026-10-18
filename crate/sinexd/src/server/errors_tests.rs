use super::*;

#[test]
fn unpresentable_exclusion_outcome_is_partial_not_failed() {
    let value = json!({"action_id":"01800000-0000-7000-8000-000000000001","operation_id":"01800000-0000-7000-8000-000000000002","state":"completed","occurrence":{"equivalence_key":"Authorization: Bearer synthetic_sensitive_value"}});
    let error = exclusion_outcome(value).unwrap_err();
    assert_eq!(error.kind, ErrorKind::Partial);
    assert_eq!(error.data["outcome"], "retained_but_unpresentable");
    assert!(error.data["operation_id"].as_str().is_some());
}

#[test]
fn source_cancellation_reports_the_configured_statement_limit() {
    let error = read_error(sinex_store::Error::SourceStatementCancelled {
        configured_statement_timeout_ms: 2000,
    });
    assert_eq!(error.kind, ErrorKind::Budget);
    assert_eq!(error.data["reason"], "statement_cancelled");
    assert_eq!(error.data["operation"], "sources.list");
    assert_eq!(error.data["phase"], "primary_source_rows");
    assert_eq!(error.data["configured_statement_timeout_ms"], 2000);
    assert!(error.data.get("timeout_ms").is_none());
    assert!(error.data.get("elapsed_ms").is_none());
}

#[test]
fn other_cancellation_reports_no_limit() {
    let error = read_error(sinex_store::Error::Database(sqlx::Error::Database(
        Box::new(Cancelled),
    )));
    assert_eq!(error.kind, ErrorKind::Budget);
    assert_eq!(error.data, json!({"reason":"statement_cancelled"}));
    let deadline = deadline_error(37);
    assert_eq!(deadline.data["reason"], "execution_deadline");
    assert_eq!(deadline.data["timeout_ms"], 37);
}

#[derive(Debug)]
struct Cancelled;
impl std::fmt::Display for Cancelled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("cancelled")
    }
}
impl std::error::Error for Cancelled {}
impl sqlx::error::DatabaseError for Cancelled {
    fn message(&self) -> &str {
        "cancelled"
    }
    fn code(&self) -> Option<std::borrow::Cow<'_, str>> {
        Some("57014".into())
    }
    fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        self
    }
    fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
        self
    }
    fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
        self
    }
    fn kind(&self) -> sqlx::error::ErrorKind {
        sqlx::error::ErrorKind::Other
    }
}
