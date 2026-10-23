use super::*;

#[test]
fn shell_sessions_are_explicit_and_recall_last_filters_the_anchor() {
    let query = q("shell-sessions where cwds ^ /src/tide sort start desc");
    assert_eq!(query.source.alias().unwrap().kinds, &["shell.session"]);
    assert!(matches!(
        q("recall last").source,
        Source::RecallLast { filter: None }
    ));
    let query = q(r#"recall last where cwds ^ "/src/tide (notes)""#);
    assert!(matches!(
        query.source,
        Source::RecallLast { filter: Some(_) }
    ));
    assert!(read_as(&query).contains("observed envelope"));
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
    assert!(err("recall mark").not_built);
    assert_eq!(
        q("sessions").source.alias().unwrap().kinds,
        &["activity.session.boundary"]
    );
}
