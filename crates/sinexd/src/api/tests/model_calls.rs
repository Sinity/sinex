use super::*;
use sinex_model::event_types::model_call::{Charge, RecordedResponse};
use sinex_model::model_calls::{
    Budget, InputHash, InputIdentity, InputSelection, InputTarget, Instructions, OutputHash,
    Policy, Prices, Request as InputRequest, Route,
};

fn policy() -> Policy {
    let now = Timestamp::now();
    Policy {
        name: "public-model-fixture".into(),
        version: "1".into(),
        budget: Budget {
            scope: "public-fixture-account".into(),
            currency: "TEST".into(),
            unit: "fixture-unit".into(),
            period_start: Timestamp::from_unix_ns(now.unix_ns() - 3_600_000_000_000).unwrap(),
            period_end: Timestamp::from_unix_ns(now.unix_ns() + 3_600_000_000_000).unwrap(),
            limit_units: 50,
        },
        routes: vec![Route {
            name: "explicit-fixture-route".into(),
            operation: "fixture-text".into(),
            provider: "recorded-fixture".into(),
            provider_account: "synthetic-account".into(),
            model: "neutral".into(),
            revision: "1".into(),
            maximum_output_units: 6,
            instructions: Instructions {
                version: "1".into(),
                prompt: "fixture prompt".into(),
                prompt_hash: InputHash::of(b"fixture prompt").0,
                rubric: "fixture rubric".into(),
                rubric_hash: InputHash::of(b"fixture rubric").0,
                settings_version: "1".into(),
                settings: json!({"fixture":true}),
                settings_hash: sinex_model::json_identity::hash(&json!({"fixture":true})),
            },
            prices: Prices {
                call: 2,
                input_byte: 1,
                output_unit: 1,
            },
        }],
    }
}

#[devkit::test]
async fn disabled_model_call_does_not_acquire_content_or_require_a_working_database(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    w.reads.close().await;
    let (status, body) = call(
        &w,
        Some(AGENT),
        json!({"method":"model-call","operation":"not-configured"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["result"]["model_call"]["refusal"]["reason"],
        "provider_disabled"
    );
}

#[devkit::test]
async fn public_recorded_response_reuses_accounting_and_policy_authoring_requires_the_operator(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    let input = command("neutral model input", 0);
    admit_events(&w, vec![input.clone()]).await;
    let policy = policy();
    let definition = serde_json::to_string(&policy).unwrap();
    let (status, _) = call(
        &w,
        Some(AGENT),
        json!({"method":"model-policy","definition":definition}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(
        sqlx::query_scalar!("SELECT count(*) FROM derivation.model_policies")
            .fetch_one(&w.reads)
            .await
            .unwrap(),
        Some(0)
    );
    let (status, configured) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"model-policy","definition":definition}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{configured}");
    assert_eq!(
        configured["result"]["model_call"]["outcome"], "authorized",
        "{configured}"
    );
    let reference = configured["result"]["policy"].as_str().unwrap();
    let route = &policy.routes[0];
    let request = InputRequest {
        operation: route.operation.clone(),
        provider: route.provider.clone(),
        provider_account: route.provider_account.clone(),
        model: route.model.clone(),
        revision: route.revision.clone(),
        instructions: route.instructions.clone(),
        maximum_output_units: route.maximum_output_units,
        inputs: vec![InputTarget {
            event: input.id,
            identity: InputIdentity::for_event(
                &input,
                InputSelection::Text {
                    pointer: "/command".into(),
                },
                None,
            )
            .unwrap(),
        }],
    };
    let output = json!({"text":"neutral supplied text"});
    let response = RecordedResponse {
        request_id: request.identity(),
        request,
        output_hash: OutputHash::for_value(&output),
        output,
        actual: Charge {
            currency: "TEST".into(),
            unit: "fixture-unit".into(),
            units: 7,
        },
        provider_receipt: Some("neutral-provider-receipt".into()),
        received_at: Timestamp::now(),
    };
    let request = json!({"method":"model-record","policy":reference,"definition":serde_json::to_string(&response).unwrap()});
    let (status, first) = call(&w, Some(OPERATOR), request.clone()).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(
        first["result"]["model_call"]["outcome"], "accepted",
        "{first}"
    );
    let (status, again) = call(&w, Some(OPERATOR), request.clone()).await;
    assert_eq!(status, StatusCode::OK, "{again}");
    assert_eq!(
        again["result"]["model_call"]["decision"]["reused"], true,
        "{again}"
    );
    assert_eq!(first["result"]["response"], again["result"]["response"]);
    assert_eq!(first["result"]["request"], again["result"]["request"]);
    let stored=sqlx::query!("SELECT count(*) AS \"calls!\",sum(actual_units)::text AS actual FROM derivation.model_calls").fetch_one(&w.reads).await.unwrap();
    assert_eq!((stored.calls, stored.actual.as_deref()), (1, Some("7")));
    let mut changed_charge = response.clone();
    changed_charge.actual.units = 8;
    let (status,contradiction)=call(&w,Some(OPERATOR),json!({"method":"model-record","policy":reference,"definition":serde_json::to_string(&changed_charge).unwrap()})).await;
    assert_eq!(status, StatusCode::OK, "{contradiction}");
    assert_eq!(
        contradiction["result"]["model_call"]["refusal"]["reason"], "contradictory_response",
        "{contradiction}"
    );
    let (_, hidden) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"hide","reference":format!("sinex:ev/{}",input.id)}),
    )
    .await;
    assert_eq!(hidden["outcome"], "ok", "{hidden}");
    let requests_before = sqlx::query_scalar!(
        "SELECT count(*) FROM core.visible_events WHERE event_type='model.request'"
    )
    .fetch_one(&w.reads)
    .await
    .unwrap();
    let (status, archived) = call(&w, Some(OPERATOR), request).await;
    assert_eq!(status, StatusCode::OK, "{archived}");
    assert_eq!(
        archived["result"]["model_call"]["outcome"], "accepted",
        "{archived}"
    );
    assert_eq!(
        archived["result"]["model_call"]["decision"]["reused"], true,
        "{archived}"
    );
    assert_eq!(archived["result"]["binding"], "unavailable", "{archived}");
    assert_eq!(archived["result"]["response"], first["result"]["response"]);
    assert_eq!(
        sqlx::query_scalar!(
            "SELECT count(*) FROM core.visible_events WHERE event_type='model.request'"
        )
        .fetch_one(&w.reads)
        .await
        .unwrap(),
        requests_before
    );
    let stored=sqlx::query!("SELECT count(*) AS \"calls!\",sum(actual_units)::text AS actual FROM derivation.model_calls").fetch_one(&w.reads).await.unwrap();
    assert_eq!((stored.calls, stored.actual.as_deref()), (1, Some("7")));
}

#[devkit::test]
async fn receipt_only_default_deny_precedes_definition_parsing_and_content_access(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    w.reads.close().await;
    let (status, body) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"model-receipt","policy":"not-a-reference","definition":"not-json"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["result"]["model_call"]["refusal"]["reason"],
        "receipt_accounting_disabled"
    );
}

#[devkit::test]
async fn public_actual_unbound_receipt_later_binds_once_without_creating_a_fake_request(
    db: devkit::pg::TestDatabase,
) {
    let w = world_with_config(
        db,
        crate::binding_runner::Config {
            model_accounting: crate::binding_runner::ModelAccountingConfig {
                actual_receipts: true,
            },
            ..Default::default()
        },
    )
    .await;
    let configured = policy();
    let (status, policy_reply) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"model-policy","definition":serde_json::to_string(&configured).unwrap()}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{policy_reply}");
    assert_eq!(
        policy_reply["result"]["model_call"]["outcome"], "authorized",
        "{policy_reply}"
    );
    let reference = policy_reply["result"]["policy"].as_str().unwrap();
    let route = &configured.routes[0];
    let receipt = sinex_model::model_calls::ReceiptOnly {
        provider: route.provider.clone(),
        provider_account: route.provider_account.clone(),
        provider_receipt: "public-native-actual-receipt".into(),
        policy_name: configured.name.clone(),
        policy_version: configured.version.clone(),
        operation: route.operation.clone(),
        basis: sinex_model::model_calls::BudgetBasis::from(&configured.budget),
        actual: Charge {
            currency: "TEST".into(),
            unit: "fixture-unit".into(),
            units: 7,
        },
    };
    let original_definition = serde_json::to_string_pretty(&receipt).unwrap();
    let request =
        json!({"method":"model-receipt","policy":reference,"definition":original_definition});
    let (status, _) = call(&w, Some(AGENT), request.clone()).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, first) = call(&w, Some(OPERATOR), request.clone()).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(
        first["result"]["model_call"]["outcome"], "actual_accepted",
        "{first}"
    );
    assert_eq!(
        first["result"]["model_call"]["decision"]["state"],
        "actual_unbound"
    );
    assert!(first["result"]["model_call"]["decision"]["bound_request"].is_null());
    for field in [
        "basis",
        "actual",
        "policy_name",
        "policy_version",
        "provider_receipt",
    ] {
        assert!(
            first["result"]["model_call"]["decision"]
                .get(field)
                .is_none(),
            "private receipt fields require the projected source view"
        );
    }
    let receipt_id = first["result"]["receipt"]
        .as_str()
        .unwrap()
        .trim_start_matches("sinex:ev/")
        .parse::<uuid::Uuid>()
        .unwrap();
    let blob = sqlx::query_scalar!(
        "SELECT m.blob FROM raw.materials m JOIN core.events e ON e.material_id=m.id WHERE e.id=$1",
        receipt_id
    )
    .fetch_one(&w.reads)
    .await
    .unwrap();
    let captured = w.blobs.read(&blob).unwrap();
    assert!(
        captured
            .windows(original_definition.len())
            .any(|bytes| bytes == original_definition.as_bytes()),
        "the API must retain supplied JSON whitespace and bytes"
    );
    assert_eq!(
        sqlx::query_scalar!(
            "SELECT count(*) FROM core.visible_events WHERE event_type='model.request'"
        )
        .fetch_one(&w.reads)
        .await
        .unwrap(),
        Some(0)
    );
    let (status, again) = call(&w, Some(OPERATOR), request).await;
    assert_eq!(status, StatusCode::OK, "{again}");
    assert_eq!(again["result"]["model_call"]["decision"]["reused"], true);
    assert_eq!(again["result"]["receipt"], first["result"]["receipt"]);
    let input = command("neutral actual receipt target", 0);
    admit_events(&w, vec![input.clone()]).await;
    let request = InputRequest {
        operation: route.operation.clone(),
        provider: route.provider.clone(),
        provider_account: route.provider_account.clone(),
        model: route.model.clone(),
        revision: route.revision.clone(),
        instructions: route.instructions.clone(),
        maximum_output_units: route.maximum_output_units,
        inputs: vec![InputTarget {
            event: input.id,
            identity: InputIdentity::for_event(
                &input,
                InputSelection::Text {
                    pointer: "/command".into(),
                },
                None,
            )
            .unwrap(),
        }],
    };
    let output = json!({"text":"neutral bound effect"});
    let response = RecordedResponse {
        request_id: request.identity(),
        request,
        output_hash: OutputHash::for_value(&output),
        output,
        actual: receipt.actual,
        provider_receipt: Some(receipt.provider_receipt),
        received_at: Timestamp::now(),
    };
    let(status,bound)=call(&w,Some(OPERATOR),json!({"method":"model-record","policy":reference,"definition":serde_json::to_string(&response).unwrap()})).await;
    assert_eq!(status, StatusCode::OK, "{bound}");
    assert_eq!(
        bound["result"]["model_call"]["outcome"], "accepted",
        "{bound}"
    );
    assert_eq!(bound["result"]["model_call"]["decision"]["reused"], true);
    let row=sqlx::query!("SELECT count(*) AS \"calls!\",sum(actual_units)::text AS actual FROM derivation.model_calls").fetch_one(&w.reads).await.unwrap();
    assert_eq!((row.calls, row.actual.as_deref()), (1, Some("7")));
    assert_eq!(
        sqlx::query_scalar!(
            "SELECT count(*) FROM core.visible_events WHERE event_type='model.request'"
        )
        .fetch_one(&w.reads)
        .await
        .unwrap(),
        Some(1)
    );
}

#[devkit::test]
async fn public_incurred_response_records_actual_with_an_absent_overflowing_quote(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    let input = command("neutral overflow quote input", 0);
    admit_events(&w, vec![input.clone()]).await;
    let mut configured = policy();
    configured.routes[0].prices.input_byte = u64::MAX;
    let (status, configured_reply) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"model-policy","definition":serde_json::to_string(&configured).unwrap()}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{configured_reply}");
    assert_eq!(
        configured_reply["result"]["model_call"]["outcome"], "authorized",
        "{configured_reply}"
    );
    let reference = configured_reply["result"]["policy"].as_str().unwrap();
    let route = &configured.routes[0];
    let request = InputRequest {
        operation: route.operation.clone(),
        provider: route.provider.clone(),
        provider_account: route.provider_account.clone(),
        model: route.model.clone(),
        revision: route.revision.clone(),
        instructions: route.instructions.clone(),
        maximum_output_units: route.maximum_output_units,
        inputs: vec![InputTarget {
            event: input.id,
            identity: InputIdentity::for_event(
                &input,
                InputSelection::Text {
                    pointer: "/command".into(),
                },
                None,
            )
            .unwrap(),
        }],
    };
    let output = json!({"text":"known output with unrepresentable quote"});
    let response = RecordedResponse {
        request_id: request.identity(),
        request,
        output_hash: OutputHash::for_value(&output),
        output,
        actual: Charge {
            currency: "TEST".into(),
            unit: "fixture-unit".into(),
            units: 7,
        },
        provider_receipt: Some("public-overflow-quote-receipt".into()),
        received_at: Timestamp::now(),
    };
    let request = json!({"method":"model-record","policy":reference,"definition":serde_json::to_string(&response).unwrap()});
    for (reused, status_expected) in [(false, StatusCode::OK), (true, StatusCode::OK)] {
        let (status, body) = call(&w, Some(OPERATOR), request.clone()).await;
        assert_eq!(status, status_expected, "{body}");
        let decision = &body["result"]["model_call"]["decision"];
        assert_eq!(
            body["result"]["model_call"]["outcome"], "accepted",
            "{body}"
        );
        assert!(decision["reserved_units"].is_null());
        assert_eq!(decision["actual_units"], 7);
        assert_eq!(decision["reused"], reused);
    }
    let row=sqlx::query!("SELECT count(*) AS \"calls!\",sum(actual_units)::text AS actual FROM derivation.model_calls").fetch_one(&w.reads).await.unwrap();
    assert_eq!((row.calls, row.actual.as_deref()), (1, Some("7")));
}
