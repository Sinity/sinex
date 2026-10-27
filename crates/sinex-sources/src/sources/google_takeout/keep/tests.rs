use super::*;
use serde_json::json;

#[devkit::test]
fn notes_lists_and_trash() {
    let note = json!({"color": "DEFAULT", "isTrashed": true, "isPinned": false, "isArchived": false,
        "title": "Shopping", "createdTimestampUsec": 1_739_887_192_500_000i64,
        "userEditedTimestampUsec": 1_739_887_200_000_000i64,
        "listContent": [{"text": "milk", "isChecked": true}, {"text": "eggs", "isChecked": false}],
        "labels": [{"name": "home"}], "textContentHtml": "<p>x</p>"});
    let (out, ()) = KeepParser
        .parse(
            note.as_object().unwrap(),
            &(),
            &sinex_source_core::RecordContext::none(),
        )
        .unwrap();
    let n = &out[0];
    assert_eq!(n.items.len(), 2);
    assert!(n.items[0].checked);
    assert_eq!(n.labels, vec!["home"]);
    assert_eq!(n.trashed_at, n.edited_at);
    assert!(n.extra.contains_key("textContentHtml"));
    assert!(
        KeepParser
            .parse(
                json!({"title": "x"}).as_object().unwrap(),
                &(),
                &sinex_source_core::RecordContext::none()
            )
            .is_err()
    );
}
