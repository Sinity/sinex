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
    assert!(result.get("health").is_none());
    assert!(result["discovery"].as_array().unwrap().iter().any(|input| {
        input["source"] == "desktop.hyprland" && input["availability"] == "producer_reported"
    }));
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

/// law: sx.cap.exclusion-reasoned
/// law: sx.cap.unimplemented-census
#[devkit::test]
async fn sources_discover_dry_run_lists_states_reasons_and_the_census(
    db: devkit::pg::TestDatabase,
) {
    let config = serde_json::from_value(json!({"exclude": {
        "desktop.clipboard": {"reason":"synthetic operator choice"}
    }}))
    .unwrap();
    let world = world_with_config(db, config).await;
    let (status, body) = call(&world, Some(READ), json!({"method":"sources-discover"})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let inputs = body["result"]["inputs"].as_array().unwrap();
    assert!(inputs.iter().any(|input| input["source"] == "desktop.clipboard"
        && input["availability"] == "excluded"
        && input["reason"] == "synthetic operator choice"));
    assert!(inputs.iter().any(
        |input| input["source"] == "forecast.request" && input["availability"] == "on_demand"
    ));
    assert!(body["result"]["unimplemented"].is_array());
}
