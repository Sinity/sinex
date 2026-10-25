use super::*;

async fn issue(w: &World, p: &str) -> Value {
    let (_,body)=call(w,Some(OPERATOR),json!({"method":"forecast-issue","target":"synthetic completion","probability":p,"criteria":"synthetic command observed","horizon":"2099-01-01T00:00:00Z","cutoff":"2026-01-01T00:00:00Z"})).await;
    assert_eq!(body["outcome"], "ok", "{body}");
    body["result"].clone()
}
async fn score(w: &World, token: &str) -> Value {
    let (_, body) = call(w, Some(token), json!({"method":"forecast-report"})).await;
    assert_eq!(body["outcome"], "ok", "{body}");
    body["result"].clone()
}
#[devkit::test]
async fn forecast_public_workflow_withdraws_hidden_and_excised_inputs(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    let empty = score(&w, OPERATOR).await;
    assert_eq!(empty["report"]["status"], "not_estimable");
    assert!(empty["report"]["brier"].is_null());
    let issued = issue(&w, "0.8").await;
    let issued_ref = issued["citation"].as_str().unwrap();
    let (_, shown) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"show","reference":issued_ref}),
    )
    .await;
    assert_eq!(shown["result"]["event"]["payload"]["by"], "operator");
    assert!(shown["result"]["event"]["material"].is_string());
    assert_eq!(score(&w, READ).await["report"]["issued"], 0);
    let evidence = command("synthetic forecast evidence", 0);
    let evidence_id = evidence.id.0.to_string();
    admit_events(&w, vec![evidence]).await;
    let (_,unknown)=call(&w,Some(OPERATOR),json!({"method":"forecast-resolve","forecast":issued_ref,"outcome":"unknown","evidence":[evidence_id]})).await;
    assert_eq!(unknown["outcome"], "ok", "{unknown}");
    assert!(score(&w, OPERATOR).await["report"]["brier"].is_null());
    let (_,resolved)=call(&w,Some(OPERATOR),json!({"method":"forecast-resolve","forecast":issued_ref,"outcome":"true","evidence":[evidence_id]})).await;
    assert_eq!(resolved["outcome"], "ok", "{resolved}");
    let report = score(&w, OPERATOR).await;
    assert!((report["report"]["brier"].as_f64().unwrap() - 0.04).abs() < 1e-12);
    assert_eq!(report["report"]["calibration"][8]["outcome_frequency"], 1.0);
    let (_,duplicate)=call(&w,Some(OPERATOR),json!({"method":"forecast-resolve","forecast":issued_ref,"outcome":"false","evidence":[evidence_id]})).await;
    assert_eq!(duplicate["outcome"], "refused");
    let resolution = resolved["result"]["citation"].as_str().unwrap();
    let (_, hidden) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"hide","reference":resolution}),
    )
    .await;
    assert_eq!(hidden["outcome"], "ok", "{hidden}");
    assert!(score(&w, OPERATOR).await["report"]["brier"].is_null());
    let (_, restored) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"unhide","reference":resolution}),
    )
    .await;
    assert_eq!(restored["outcome"], "ok", "{restored}");
    let (_, hidden_evidence) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"hide","reference":evidence_id}),
    )
    .await;
    assert_eq!(hidden_evidence["outcome"], "ok", "{hidden_evidence}");
    assert!(score(&w, OPERATOR).await["report"]["brier"].is_null());
    let (_, excised) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"excise","reference":issued_ref}),
    )
    .await;
    assert_eq!(excised["outcome"], "ok", "{excised}");
    assert_eq!(score(&w, OPERATOR).await["report"]["issued"], 0);
}
#[devkit::test]
async fn forecast_public_commands_refuse_invalid_inputs_and_ungranted_authors(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    for p in ["NaN", "inf", "-0.1", "1.01"] {
        let (_,body)=call(&w,Some(OPERATOR),json!({"method":"forecast-issue","target":"synthetic","probability":p,"criteria":"evidence","horizon":"2099-01-01T00:00:00Z","cutoff":"2026-01-01T00:00:00Z"})).await;
        assert_eq!(body["outcome"], "refused", "{body}");
    }
    let (_,body)=call(&w,Some(AGENT),json!({"method":"forecast-issue","target":"synthetic","probability":"0.5","criteria":"evidence","horizon":"2099-01-01T00:00:00Z","cutoff":"2026-01-01T00:00:00Z"})).await;
    assert_eq!(body["outcome"], "refused", "{body}");
}

#[devkit::test]
async fn forecast_new_authoring_continues_after_an_unrelated_forecast_is_excised(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    let first = issue(&w, "0.2").await;
    let (_, body) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"excise","reference":first["citation"]}),
    )
    .await;
    assert_eq!(body["outcome"], "ok", "{body}");
    let second = issue(&w, "0.7").await;
    assert_ne!(first["forecast"], second["forecast"]);
    assert_eq!(score(&w, OPERATOR).await["report"]["issued"], 1);
}

#[devkit::test]
async fn forecast_commands_preserve_temporal_and_evidence_boundaries(db: devkit::pg::TestDatabase) {
    let w = world(db).await;
    let evidence = command("synthetic cutoff evidence", 0);
    let evidence_id = evidence.id.0.to_string();
    admit_events(&w, vec![evidence]).await;
    for change in [
        json!({"target":""}),
        json!({"criteria":""}),
        json!({"horizon":"2000-01-01T00:00:00Z"}),
        json!({"cutoff":"2099-01-01T00:00:00Z"}),
        json!({"information":[evidence_id]}),
    ] {
        let mut request = json!({"method":"forecast-issue","target":"synthetic","probability":"0.5","criteria":"recorded evidence","horizon":"2099-01-01T00:00:00Z","cutoff":"2026-01-01T00:00:00Z"});
        request
            .as_object_mut()
            .unwrap()
            .extend(change.as_object().unwrap().clone());
        let (_, body) = call(&w, Some(OPERATOR), request).await;
        assert_eq!(body["outcome"], "refused", "{body}");
    }
    let issued = issue(&w, "0.8").await;
    for change in [
        json!({"outcome":"maybe"}),
        json!({"at":"2000-01-01T00:00:00Z"}),
        json!({"at":"2099-01-01T00:00:00Z"}),
        json!({"evidence":[]}),
        json!({"evidence":["00000000-0000-7000-8000-000000000000"]}),
    ] {
        let mut request = json!({"method":"forecast-resolve","forecast":issued["citation"],"outcome":"true","evidence":[evidence_id]});
        request
            .as_object_mut()
            .unwrap()
            .extend(change.as_object().unwrap().clone());
        let (_, body) = call(&w, Some(OPERATOR), request).await;
        assert_eq!(body["outcome"], "refused", "{body}");
    }
    assert_eq!(score(&w, OPERATOR).await["report"]["unresolved"], 1);
}

#[devkit::test]
async fn forecast_report_refuses_conflicting_visible_resolution_records(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    let issued = issue(&w, "0.8").await;
    let evidence = command("synthetic evidence for conflicting resolutions", 0);
    let evidence_id = evidence.id.0.to_string();
    admit_events(&w, vec![evidence]).await;
    let mut events = Vec::new();
    for outcome in [true, false] {
        let payload = sinex_model::event_types::forecast::Resolved {
            forecast: issued["forecast"].as_str().unwrap().into(),
            outcome: Some(outcome),
            outcome_at: None,
            evidence: vec![evidence_id.clone()],
            by: "agent".into(),
            at: Timestamp::now(),
        };
        events.push(
            Event::build(
                &payload,
                "forecast.request",
                Origin::Record {
                    location: RecordLocation {
                        material: MaterialId::new(),
                        anchor: 0,
                    },
                    ordinal: 0,
                },
                ReadingOrder(1),
                *blake3::hash(format!("{outcome}").as_bytes()).as_bytes(),
                Class::Guarded,
            )
            .unwrap(),
        );
    }
    admit_events(&w, events).await;
    let (_, body) = call(&w, Some(OPERATOR), json!({"method":"forecast-report"})).await;
    assert_eq!(body["outcome"], "refused", "{body}");
    assert!(
        body["reason"]
            .as_str()
            .unwrap()
            .contains("multiple visible known resolutions")
    );
}
