use super::*;

#[devkit::test]
async fn status_lists_exclusions_and_reasons_without_source_runs(db: devkit::pg::TestDatabase) {
    let config = serde_json::from_value(json!({"exclude": {
        "desktop.clipboard": {"reason":"synthetic operator choice"},
        "session.media": {"reason":"synthetic media choice"},
        "desktop.hyprland": null
    }}))
    .unwrap();
    let world = world_with_config(db, config).await;
    let (status, body) = call(&world, Some(READ), json!({"method":"status"})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let result = &body["result"];
    assert_eq!(
        result["exclusions"]["desktop.clipboard"]["reason"],
        "synthetic operator choice"
    );
    assert_eq!(result["exclusions"].as_object().unwrap().len(), 2);
    assert_eq!(result["health"]["exclusions"], result["exclusions"]);
    assert!(
        result["discovery"]
            .as_array()
            .unwrap()
            .iter()
            .any(|source| source["source"] == "desktop.clipboard"
                && source["availability"] == "excluded"
                && source["reason"] == "synthetic operator choice")
    );
}
