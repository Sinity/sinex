use super::*;

#[devkit::test]
fn shell_sessions_are_explicit_and_recall_last_filters_the_session() {
    let query = q("shell-sessions where cwds ^ /src/tide | sort by start desc");
    assert_eq!(query.source.alias().unwrap().kinds, &["shell.session"]);
    assert!(matches!(
        parse_query("recall last").source,
        Source::RecallLast { filter: None }
    ));
    let query = parse_query(r#"recall last where cwds ^ "/src/tide (notes)""#);
    assert!(matches!(
        query.source,
        Source::RecallLast { filter: Some(_) }
    ));
    assert!(
        read_as(&query, &jiff::tz::TimeZone::UTC)
            .contains("during the latest recorded shell session")
    );
    assert!(
        err("shell-sessions where cwds ^ relative")
            .message
            .contains("absolute")
    );
    assert!(
        err("shell-sessions where cwds > /src/tide")
            .message
            .contains("ordered")
    );
    assert!(
        err("shell-sessions where cwds ~ tide")
            .message
            .contains("path lists")
    );
    assert!(q("shell-sessions where cwds != /src/tide").filter.is_some());
    assert!(matches!(
        parse_query("recall mark").source,
        Source::RecallMark { number: 1, .. }
    ));
}
