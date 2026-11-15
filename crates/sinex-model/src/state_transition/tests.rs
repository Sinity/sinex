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
fn scopes_use_strings_then_bucket_then_the_captured_record() {
    for (payload, expected) in [
        (json!({"scope":"","bucket":"b"}), ""),
        (json!({"scope":false,"bucket":"b"}), "b"),
        (json!({"scope":null,"bucket":3}), "captured"),
    ] {
        assert_eq!(scope(&payload, || "captured".into()), expected);
    }
}
