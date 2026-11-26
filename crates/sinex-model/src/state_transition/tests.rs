use super::*;
use serde_json::json;
#[test]
fn subjects_keep_literal_fallback_and_json_scalar_meaning() {
    let focus = [
        SubjectField::Text {
            field: "window",
            prefix: "",
        },
        SubjectField::Text {
            field: "class",
            prefix: "class:",
        },
    ];
    assert_eq!(
        subject(&focus, &json!({"window":"", "class":"app"})),
        Some("class:app".into())
    );
    assert_eq!(
        subject(&focus, &json!({"window":"class:app", "class":"other"})),
        Some("class:app".into())
    );
    assert_eq!(
        subject(&focus, &json!({"window":false, "class":null})),
        None
    );
    let workspace = [
        SubjectField::Scalar {
            field: "id",
            prefix: "workspace:",
        },
        SubjectField::Scalar {
            field: "name",
            prefix: "workspace:",
        },
    ];
    for (payload, expected) in [
        (json!({"id":null,"name":""}), "workspace:"),
        (json!({"id":2}), "workspace:2"),
        (json!({"id":false}), "workspace:false"),
        (json!({"id":[1,{"aa":true}]}), "workspace:[1,{\"aa\":true}]"),
    ] {
        assert_eq!(subject(&workspace, &payload).as_deref(), Some(expected));
    }
}
#[test]
fn a_scope_is_the_first_string_scope_field_and_absent_without_one() {
    for (payload, expected) in [
        (json!({"scope":"","bucket":"b"}), Some("")),
        (json!({"scope":false,"bucket":"b"}), Some("b")),
        (json!({"scope":null,"bucket":3}), None),
        (json!({"window":"w"}), None),
    ] {
        assert_eq!(scope(&payload).as_deref(), expected);
    }
}
