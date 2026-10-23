use super::*;

#[test]
fn tools_come_from_the_command_table() {
    let tools = tools();
    let names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
    assert!(names.contains(&"sinex_query"));
    assert!(names.contains(&"sinex_recall"));
    for name in ["sinex_stage", "sinex_same", "sinex_distinct", "sinex_mark"] {
        assert!(!names.contains(&name), "{name} is not implemented");
    }
    assert!(!names.contains(&"sinex_follow"), "follow streams");
    assert!(
        !names.contains(&"sinex_excise"),
        "excision is the operator's"
    );
    let query = tools.iter().find(|t| t.name == "sinex_query").unwrap();
    assert_eq!(query.input_schema["required"], json!(["query"]));
}
