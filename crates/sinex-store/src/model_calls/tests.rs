use super::*;
use crate::{AdmissionContext, NoDerivations};
use serde_json::json;
use sinex_model::event_types::model_call::Charge;
use sinex_model::event_types::session::MediaSegment;
use sinex_model::model_calls::{
    Budget, InputIdentity, InputSelection, InputTarget, Instructions, Policy, Prices, Request,
    Route,
};
use sinex_model::{
    Batch, BatchKind, Class, MaterialId, Origin, ReadingOrder, RecordLocation, RecordOutcome, RunId,
};

fn at(seconds: i64) -> Timestamp {
    Timestamp::from_unix_secs(seconds).unwrap()
}
fn captured<P: EventPayload>(payload: &P) -> Event {
    Event::build(
        payload,
        "model.fixture",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(payload.ts_orig().at.map_or(0, |at| at.unix_ns() as i64)),
        *blake3::hash(&serde_json::to_vec(payload).unwrap()).as_bytes(),
        Class::Guarded,
    )
    .unwrap()
}
fn input(text: &str) -> Event {
    captured(&MediaSegment {
        kind: "screen".into(),
        started_at: at(100),
        ended_at: at(101),
        mime: "image/png".into(),
        bytes: 0,
        target: None,
        width: Some(100),
        height: Some(80),
        accessibility_text: Some(text.into()),
    })
}
fn policy(version: &str, limit: u64, configured_at: i64) -> ModelPolicy {
    ModelPolicy {
        policy_name: "fixture-policy".into(),
        configured_by: "operator".into(),
        configured_at: at(configured_at),
        policy: Policy {
            name: "fixture-policy".into(),
            version: version.into(),
            budget: Budget {
                scope: "fixture-account".into(),
                currency: "TEST".into(),
                unit: "fixture-unit".into(),
                period_start: at(0),
                period_end: at(1000),
                limit_units: limit,
            },
            routes: vec![Route {
                name: "fixture-route".into(),
                operation: "fixture-text".into(),
                provider: "recorded-fixture".into(),
                provider_account: "synthetic-account".into(),
                model: "neutral".into(),
                revision: "1".into(),
                instructions: Instructions {
                    version: "1".into(),
                    prompt: "fixture prompt".into(),
                    prompt_hash: sinex_model::model_calls::InputHash::of(b"fixture prompt").0,
                    rubric: "fixture rubric".into(),
                    rubric_hash: sinex_model::model_calls::InputHash::of(b"fixture rubric").0,
                    settings_version: "1".into(),
                    settings: json!({"fixture":true}),
                    settings_hash: json_identity::hash(&json!({"fixture":true})),
                },
                maximum_output_units: 6,
                prices: Prices {
                    call: 5,
                    input_byte: 1,
                    output_unit: 1,
                },
            }],
        },
    }
}
fn requested(policy_event: EventId, target: &Event) -> ModelRequest {
    let route = policy("1", 35, 10).policy.routes.remove(0);
    let request = Request {
        operation: route.operation,
        provider: route.provider,
        provider_account: route.provider_account,
        model: route.model,
        revision: route.revision,
        instructions: route.instructions,
        maximum_output_units: route.maximum_output_units,
        inputs: vec![InputTarget {
            event: target.id,
            identity: InputIdentity::for_event(
                target,
                InputSelection::Text {
                    pointer: "/accessibility_text".into(),
                },
                None,
            )
            .unwrap(),
        }],
    };
    ModelRequest {
        request_id: request.identity(),
        policy: policy_event,
        request,
        recorded_response: None,
        requested_at: at(100),
        requested_by: "agent".into(),
    }
}
fn batch(events: Vec<Event>) -> Batch {
    Batch {
        run: RunId::new(),
        kind: BatchKind::Api {
            actor: "fixture".into(),
        },
        records: vec![RecordOutcome::Events {
            location: None,
            events,
        }],
        archive: vec![],
    }
}
async fn admit(conn: &mut AdmissionContext, events: Vec<Event>) {
    let mut tx = conn.begin().await.unwrap();
    tx.admit(&batch(events), &NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
}
async fn act(conn: &mut AdmissionContext, action: Action) -> Outcome {
    act_with(conn, action, Capabilities::default()).await
}
async fn act_with(
    conn: &mut AdmissionContext,
    action: Action,
    capabilities: Capabilities,
) -> Outcome {
    let mut tx = conn.begin().await.unwrap();
    let result = tx
        .model_call("operator", &action, capabilities)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    result
}
async fn fixture(db: &devkit::pg::TestDatabase) -> (AdmissionContext, Event, ModelRequest, Event) {
    let mut conn = AdmissionContext::connect(&db.url(None)).await.unwrap();
    let target = input("first fixture input");
    let policy = captured(&policy("1", 35, 10));
    admit(&mut conn, vec![target.clone(), policy.clone()]).await;
    let request = requested(policy.id, &target);
    let event = captured(&request);
    admit(&mut conn, vec![event.clone()]).await;
    (conn, policy, request, event)
}

#[devkit::test]
async fn policy_label_is_inert_until_an_internal_operator_authorization_commits(
    db: devkit::pg::TestDatabase,
) {
    let (mut conn, policy, _, request) = fixture(&db).await;
    assert_eq!(
        act(
            &mut conn,
            Action::Reserve {
                request: request.id,
                at: at(200)
            }
        )
        .await,
        refused(Refusal::PolicyUnauthorized)
    );
    {
        let mut tx = conn.begin().await.unwrap();
        let result = tx
            .model_call(
                "agent",
                &Action::AuthorizePolicy {
                    policy: policy.id,
                    at: at(200),
                },
                Capabilities::default(),
            )
            .await
            .unwrap();
        assert_eq!(result, refused(Refusal::PolicyUnauthorized));
        tx.commit().await.unwrap();
    }
    {
        let mut tx = conn.begin().await.unwrap();
        tx.model_call(
            "operator",
            &Action::AuthorizePolicy {
                policy: policy.id,
                at: at(200),
            },
            Capabilities::default(),
        )
        .await
        .unwrap();
        tx.rollback().await.unwrap();
    }
    assert_eq!(
        act(
            &mut conn,
            Action::Reserve {
                request: request.id,
                at: at(200)
            }
        )
        .await,
        refused(Refusal::PolicyUnauthorized)
    );
    assert_eq!(
        act(
            &mut conn,
            Action::AuthorizePolicy {
                policy: policy.id,
                at: at(200)
            }
        )
        .await,
        Outcome::Authorized {
            policy: policy.id,
            reused: false
        }
    );
    assert_eq!(
        act(
            &mut conn,
            Action::AuthorizePolicy {
                policy: policy.id,
                at: at(201)
            }
        )
        .await,
        Outcome::Authorized {
            policy: policy.id,
            reused: true
        }
    );
    let Outcome::Accepted { decision } = act(
        &mut conn,
        Action::Reserve {
            request: request.id,
            at: at(200),
        },
    )
    .await
    else {
        panic!("authorized configured budget must reserve")
    };
    assert_eq!(decision.reserved_units, Some(30));
}

#[devkit::test]
async fn crash_boundaries_never_duplicate_or_release_a_model_reservation(
    db: devkit::pg::TestDatabase,
) {
    let (mut conn, policy, request, event) = fixture(&db).await;
    act(
        &mut conn,
        Action::AuthorizePolicy {
            policy: policy.id,
            at: at(200),
        },
    )
    .await;
    {
        let mut tx = conn.begin().await.unwrap();
        tx.model_call(
            "operator",
            &Action::Reserve {
                request: event.id,
                at: at(200),
            },
            Capabilities::default(),
        )
        .await
        .unwrap();
        tx.rollback().await.unwrap();
    }
    assert!(
        decision(&mut conn, &request.request_id)
            .await
            .unwrap()
            .is_none()
    );
    act(
        &mut conn,
        Action::Reserve {
            request: event.id,
            at: at(200),
        },
    )
    .await;
    conn.close().await.unwrap();
    let mut conn = AdmissionContext::connect(&db.url(None)).await.unwrap();
    let Outcome::Accepted { decision } = act(
        &mut conn,
        Action::Reserve {
            request: event.id,
            at: at(201),
        },
    )
    .await
    else {
        panic!("durable reserve missing")
    };
    assert!(decision.reused);
    assert_eq!(decision.reserved_units, Some(30));
    act(
        &mut conn,
        Action::Uncertain {
            request: request.request_id.clone(),
            reason: "fixture interruption after durable intent".into(),
            at: at(202),
        },
    )
    .await;
    conn.close().await.unwrap();
    let mut conn = AdmissionContext::connect(&db.url(None)).await.unwrap();
    let Outcome::Accepted { decision } = act(
        &mut conn,
        Action::Reserve {
            request: event.id,
            at: at(203),
        },
    )
    .await
    else {
        panic!("uncertain reserve missing")
    };
    assert!(decision.reused);
    assert_eq!(decision.state, State::Uncertain);
    let count = sqlx::query_scalar!("SELECT count(*) FROM derivation.model_calls")
        .fetch_one(&mut *conn)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(count, 1);
}

#[devkit::test]
async fn raising_a_policy_budget_cannot_forget_spend_or_make_an_old_policy_current(
    db: devkit::pg::TestDatabase,
) {
    let (mut conn, first, _, request) = fixture(&db).await;
    act(
        &mut conn,
        Action::AuthorizePolicy {
            policy: first.id,
            at: at(200),
        },
    )
    .await;
    act(
        &mut conn,
        Action::Reserve {
            request: request.id,
            at: at(200),
        },
    )
    .await;
    let second = captured(&policy("2", 50, 20));
    admit(&mut conn, vec![second.clone()]).await;
    act(
        &mut conn,
        Action::AuthorizePolicy {
            policy: second.id,
            at: at(201),
        },
    )
    .await;
    assert_eq!(
        act(
            &mut conn,
            Action::Reserve {
                request: request.id,
                at: at(201)
            }
        )
        .await,
        refused(Refusal::PolicyUnavailable)
    );
    let target = input("second fixture input");
    admit(&mut conn, vec![target.clone()]).await;
    let request = captured(&requested(second.id, &target));
    admit(&mut conn, vec![request.clone()]).await;
    let Outcome::Refused {
        refusal:
            Refusal::InsufficientConfiguredBudget {
                committed_units,
                required_units,
                limit_units,
                ..
            },
    } = act(
        &mut conn,
        Action::Reserve {
            request: request.id,
            at: at(202),
        },
    )
    .await
    else {
        panic!("new policy must retain previous reservation")
    };
    assert_eq!(
        (committed_units.as_str(), required_units, limit_units),
        ("30", 31, 50)
    );
}

#[devkit::test]
async fn recorded_cost_overruns_are_truth_and_reconciliation_never_charges_twice(
    db: devkit::pg::TestDatabase,
) {
    let (mut conn, policy, request, event) = fixture(&db).await;
    act(
        &mut conn,
        Action::AuthorizePolicy {
            policy: policy.id,
            at: at(200),
        },
    )
    .await;
    act(
        &mut conn,
        Action::Reserve {
            request: event.id,
            at: at(200),
        },
    )
    .await;
    let output = json!({"text":"neutral recorded response"});
    let response = RecordedResponse {
        request_id: request.request_id.clone(),
        request: request.request.clone(),
        output_hash: OutputHash::for_value(&output),
        output,
        actual: Charge {
            currency: "TEST".into(),
            unit: "fixture-unit".into(),
            units: 55,
        },
        provider_receipt: Some("fixture-provider-key".into()),
        received_at: at(201),
    };
    let response_event = captured(&response);
    admit(&mut conn, vec![response_event.clone()]).await;
    let Outcome::Accepted { decision } = act(
        &mut conn,
        Action::Reconcile {
            request: event.id,
            response: response_event.id,
            at: at(201),
        },
    )
    .await
    else {
        panic!("incurred cost overrun must be recorded")
    };
    assert_eq!(decision.actual_units, Some(55));
    assert!(decision.overrun);
    let Outcome::Accepted { decision } = act(
        &mut conn,
        Action::Reconcile {
            request: event.id,
            response: response_event.id,
            at: at(202),
        },
    )
    .await
    else {
        panic!("same receipt must reuse")
    };
    assert!(decision.reused);
    let mut contradiction = response.clone();
    contradiction.output = json!({"text":"contradictory response"});
    contradiction.output_hash = OutputHash::for_value(&contradiction.output);
    let conflict = captured(&contradiction);
    admit(&mut conn, vec![conflict.clone()]).await;
    assert_eq!(
        act(
            &mut conn,
            Action::Reconcile {
                request: event.id,
                response: conflict.id,
                at: at(203)
            }
        )
        .await,
        refused(Refusal::ContradictoryResponse {
            request: request.request_id.clone()
        })
    );
    let persisted = super::decision(&mut conn, &request.request_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(persisted.actual_units, Some(55));
    assert_eq!(persisted.response, Some(response_event.id));
    let target = input("another fixture input");
    admit(&mut conn, vec![target.clone()]).await;
    let next_request = requested(policy.id, &target);
    let next = captured(&next_request);
    admit(&mut conn, vec![next.clone()]).await;
    assert!(matches!(
        act(
            &mut conn,
            Action::Reserve {
                request: next.id,
                at: at(204)
            }
        )
        .await,
        Outcome::Refused {
            refusal: Refusal::InsufficientConfiguredBudget { .. }
        }
    ));
    let mut alias = response.clone();
    alias.request = next_request.request;
    alias.request_id = next_request.request_id.clone();
    let alias_event = captured(&alias);
    admit(&mut conn, vec![alias_event.clone()]).await;
    assert_eq!(
        act(
            &mut conn,
            Action::Reconcile {
                request: next.id,
                response: alias_event.id,
                at: at(205)
            }
        )
        .await,
        refused(Refusal::ContradictoryReceipt {
            receipt: response
                .receipt_identity()
                .expect("fixture has native receipt identity")
        })
    );
    assert_eq!(
        sqlx::query_scalar!("SELECT count(*) FROM derivation.model_calls")
            .fetch_one(&mut *conn)
            .await
            .unwrap(),
        Some(1)
    );
    assert!(
        super::decision(&mut conn, &next_request.request_id)
            .await
            .unwrap()
            .is_none(),
        "the aliased receipt cannot create a second request charge"
    );
    assert_eq!(
        super::decision(&mut conn, &request.request_id)
            .await
            .unwrap(),
        Some(persisted),
        "receipt conflict preserves the original amount, effect and frozen account"
    );
}

#[devkit::test]
async fn a_claimed_input_hash_cannot_bind_a_different_stored_reading(db: devkit::pg::TestDatabase) {
    let (mut conn, policy, _, _) = fixture(&db).await;
    let target = input("new actual input");
    admit(&mut conn, vec![target.clone()]).await;
    let mut request = requested(policy.id, &target);
    request.request.inputs[0].identity.input_hash =
        sinex_model::model_calls::InputHash::of(b"claimed other input");
    request.request_id = request.request.identity();
    let mut tx = conn.begin().await.unwrap();
    let result = tx
        .admit(&batch(vec![captured(&request)]), &NoDerivations)
        .await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("selected content differs")
    );
    tx.rollback().await.unwrap();
    let mut request = requested(policy.id, &target);
    request.request.inputs[0].identity.selected_bytes = 0;
    request.request_id = request.request.identity();
    let mut tx = conn.begin().await.unwrap();
    assert!(
        tx.admit(&batch(vec![captured(&request)]), &NoDerivations)
            .await
            .unwrap_err()
            .to_string()
            .contains("selected content differs")
    );
    tx.rollback().await.unwrap();
}

#[devkit::test]
async fn a_retained_request_reconciles_incurred_cost_after_targets_and_policy_are_archived(
    db: devkit::pg::TestDatabase,
) {
    let (mut conn, policy, request, event) = fixture(&db).await;
    act(
        &mut conn,
        Action::AuthorizePolicy {
            policy: policy.id,
            at: at(200),
        },
    )
    .await;
    let target = request.request.inputs[0].event;
    let mut archive = batch(vec![]);
    archive.archive = vec![target, policy.id]
        .into_iter()
        .map(|target| sinex_model::ArchiveRequest::Hide {
            target,
            operation: sinex_model::OperationId::new(),
        })
        .collect();
    let mut tx = conn.begin().await.unwrap();
    tx.admit(&archive, &NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
    let output = json!({"text":"recorded after interrupted execution"});
    let response = RecordedResponse {
        request_id: request.request_id.clone(),
        request: request.request,
        output_hash: OutputHash::for_value(&output),
        output,
        actual: Charge {
            currency: "TEST".into(),
            unit: "fixture-unit".into(),
            units: 42,
        },
        provider_receipt: Some("after-interruption-fixture".into()),
        received_at: at(1200),
    };
    let response = captured(&response);
    admit(&mut conn, vec![response.clone()]).await;
    assert_eq!(
        act(
            &mut conn,
            Action::FindRequest {
                policy: policy.id,
                response: response.id
            }
        )
        .await,
        Outcome::RequestLookup {
            request: Some(event.id),
            policy_current: false
        }
    );
    let Outcome::Accepted { decision } = act(
        &mut conn,
        Action::Reconcile {
            request: event.id,
            response: response.id,
            at: at(1200),
        },
    )
    .await
    else {
        panic!("known expenditure must survive target and policy lifecycle")
    };
    assert_eq!(decision.actual_units, Some(42));
    assert!(decision.overrun);
    let Outcome::Accepted { decision } = act(
        &mut conn,
        Action::Reconcile {
            request: event.id,
            response: response.id,
            at: at(1201),
        },
    )
    .await
    else {
        panic!("repeated historical accounting must reuse")
    };
    assert!(decision.reused);
    assert_eq!(
        sqlx::query_scalar!("SELECT count(*) FROM derivation.model_calls")
            .fetch_one(&mut *conn)
            .await
            .unwrap(),
        Some(1)
    );
    assert_eq!(
        recording_class(&mut conn, &[target]).await.unwrap(),
        Some((Class::Guarded, false))
    );
}

#[devkit::test]
async fn equivalent_replay_binds_new_exact_parents_without_purchasing_an_effect_again(
    db: devkit::pg::TestDatabase,
) {
    let (mut conn, policy, request, original) = fixture(&db).await;
    act(
        &mut conn,
        Action::AuthorizePolicy {
            policy: policy.id,
            at: at(200),
        },
    )
    .await;
    let output = json!({"text":"retained effect"});
    let response = RecordedResponse {
        request_id: request.request_id.clone(),
        request: request.request.clone(),
        output_hash: OutputHash::for_value(&output),
        output,
        actual: Charge {
            currency: "TEST".into(),
            unit: "fixture-unit".into(),
            units: 7,
        },
        provider_receipt: Some("reused-effect-fixture".into()),
        received_at: at(201),
    };
    let response = captured(&response);
    admit(&mut conn, vec![response.clone()]).await;
    act(
        &mut conn,
        Action::Reconcile {
            request: original.id,
            response: response.id,
            at: at(201),
        },
    )
    .await;
    let old_target = request.request.inputs[0].event;
    let mut exact =
        crate::derivations::ExactInputs::new(vec![old_target], 1, crate::PAGE_BYTES).unwrap();
    let mut reread = exact
        .next(&mut conn)
        .await
        .unwrap()
        .unwrap()
        .remove(0)
        .event;
    let material = match &reread.origin {
        Origin::Record { location, .. } => location.material,
        _ => panic!("fixture is a stored record"),
    };
    reread.id = EventId::new();
    let mut replay = batch(vec![reread.clone()]);
    replay.archive.push(sinex_model::ArchiveRequest::Replay {
        scope: sinex_model::ReplayScope {
            materials: vec![material],
            ..Default::default()
        },
        operation: sinex_model::OperationId::new(),
    });
    let mut tx = conn.begin().await.unwrap();
    tx.admit(&replay, &NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
    let mut binding = request;
    binding.request.inputs[0].event = reread.id;
    binding.recorded_response = Some(response.id);
    binding.requested_at = at(202);
    assert_eq!(binding.request_id, binding.request.identity());
    let bound = captured(&binding);
    admit(&mut conn, vec![bound.clone()]).await;
    let Outcome::Accepted { decision } = act(
        &mut conn,
        Action::Reserve {
            request: bound.id,
            at: at(202),
        },
    )
    .await
    else {
        panic!("equivalent current targets must reuse retained effect")
    };
    assert!(decision.reused);
    assert_eq!(decision.state, State::Recorded);
    assert_eq!(decision.actual_units, Some(7));
    assert_eq!(binding.request.inputs[0].event, reread.id);
    assert_ne!(old_target, reread.id);
    assert_eq!(
        sqlx::query_scalar!("SELECT count(*) FROM derivation.model_calls")
            .fetch_one(&mut *conn)
            .await
            .unwrap(),
        Some(1)
    );
}

fn receipt(policy: EventId, amount: u64) -> ModelReceipt {
    let configured = policy_fixture();
    ModelReceipt {
        policy,
        receipt: sinex_model::model_calls::ReceiptOnly {
            provider: configured.routes[0].provider.clone(),
            provider_account: configured.routes[0].provider_account.clone(),
            provider_receipt: "actual-native-fixture".into(),
            policy_name: configured.name.clone(),
            policy_version: configured.version.clone(),
            operation: configured.routes[0].operation.clone(),
            basis: sinex_model::model_calls::BudgetBasis::from(&configured.budget),
            actual: Charge {
                currency: "TEST".into(),
                unit: "fixture-unit".into(),
                units: amount,
            },
        },
        recorded_at: at(1200),
        recorded_by: "operator".into(),
    }
}
fn policy_fixture() -> Policy {
    policy("1", 35, 10).policy
}
fn receipt_capability() -> Capabilities {
    Capabilities {
        actual_receipts: true,
    }
}

#[devkit::test]
async fn submicrosecond_receipt_period_survives_repeat_restart_and_later_binding(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = AdmissionContext::connect(&db.url(None)).await.unwrap();
    let mut configured = policy("1", 35, 10);
    configured.policy.budget.period_start = Timestamp::from_unix_ns(137).unwrap();
    configured.policy.budget.period_end =
        Timestamp::from_unix_ns(at(1000).unix_ns() + 911).unwrap();
    let basis = sinex_model::model_calls::BudgetBasis::from(&configured.policy.budget);
    let policy = captured(&configured);
    let target = input("fractional period");
    admit(&mut conn, vec![policy.clone(), target.clone()]).await;
    act(
        &mut conn,
        Action::AuthorizePolicy {
            policy: policy.id,
            at: at(200),
        },
    )
    .await;
    let request = requested(policy.id, &target);
    let event = captured(&request);
    admit(&mut conn, vec![event.clone()]).await;
    let Outcome::Accepted { .. } = act(
        &mut conn,
        Action::Reserve {
            request: event.id,
            at: at(200),
        },
    )
    .await
    else {
        panic!("configured hold")
    };
    let mut payload = receipt(policy.id, 7);
    payload.receipt.basis = basis.clone();
    let receipt = captured(&payload);
    admit(&mut conn, vec![receipt.clone()]).await;
    for reused in [false, true] {
        let outcome = act_with(
            &mut conn,
            Action::RecordActual {
                receipt: receipt.id,
                at: at(1200),
            },
            receipt_capability(),
        )
        .await;
        let Outcome::ActualAccepted { decision } = outcome else {
            panic!("exact submicrosecond receipt must remain valid: {outcome:?}")
        };
        assert_eq!(decision.reused, reused);
    }
    let row=sqlx::query!("SELECT entry_id,period_start_ns::text AS \"start!\",period_end_ns::text AS \"end!\" FROM derivation.model_calls WHERE receipt_hash IS NOT NULL").fetch_one(&mut *conn).await.unwrap();
    assert_eq!(row.start, "137");
    assert_eq!(row.end, basis.period_end.unix_ns().to_string());
    conn.close().await.unwrap();
    let mut conn = AdmissionContext::connect(&db.url(None)).await.unwrap();
    let output = json!({"text":"neutral submicrosecond fixture"});
    let response = captured(&RecordedResponse {
        request_id: request.request_id,
        request: request.request,
        output_hash: OutputHash::for_value(&output),
        output,
        actual: payload.receipt.actual,
        provider_receipt: Some(payload.receipt.provider_receipt),
        received_at: at(1201),
    });
    admit(&mut conn, vec![response.clone()]).await;
    let outcome = act(
        &mut conn,
        Action::Reconcile {
            request: event.id,
            response: response.id,
            at: at(1201),
        },
    )
    .await;
    let Outcome::Accepted { decision } = outcome else {
        panic!("exact original period must bind: {outcome:?}")
    };
    assert!(decision.reused);
    assert_eq!(decision.actual_units, Some(7));
    let after=sqlx::query!("SELECT entry_id,period_start_ns::text AS \"start!\",period_end_ns::text AS \"end!\",coalesce(actual_units,reserved_units)::text AS \"amount!\" FROM derivation.model_calls").fetch_one(&mut *conn).await.unwrap();
    assert_eq!(after.entry_id, row.entry_id);
    assert_eq!(after.start, row.start);
    assert_eq!(after.end, row.end);
    assert_eq!(after.amount, "7");
    assert_eq!(
        sqlx::query_scalar!("SELECT count(*) FROM derivation.model_calls")
            .fetch_one(&mut *conn)
            .await
            .unwrap(),
        Some(1)
    );
}

#[devkit::test]
async fn actual_unbound_requires_both_deployment_capability_and_authorized_operator_policy(
    db: devkit::pg::TestDatabase,
) {
    let (mut conn, policy, _, _) = fixture(&db).await;
    let record = captured(&receipt(policy.id, 7));
    admit(&mut conn, vec![record.clone()]).await;
    assert_eq!(
        act(
            &mut conn,
            Action::RecordActual {
                receipt: EventId::new(),
                at: at(1200)
            }
        )
        .await,
        refused(Refusal::ReceiptAccountingDisabled)
    );
    let mut tx = conn.begin().await.unwrap();
    assert_eq!(
        tx.model_call(
            "agent",
            &Action::RecordActual {
                receipt: record.id,
                at: at(1200)
            },
            receipt_capability()
        )
        .await
        .unwrap(),
        refused(Refusal::PolicyUnauthorized)
    );
    tx.commit().await.unwrap();
    assert_eq!(
        act_with(
            &mut conn,
            Action::RecordActual {
                receipt: record.id,
                at: at(1200)
            },
            receipt_capability()
        )
        .await,
        refused(Refusal::PolicyUnauthorized)
    );
    assert_eq!(
        sqlx::query_scalar!("SELECT count(*) FROM derivation.model_calls")
            .fetch_one(&mut *conn)
            .await
            .unwrap(),
        Some(0)
    );
    act(
        &mut conn,
        Action::AuthorizePolicy {
            policy: policy.id,
            at: at(200),
        },
    )
    .await;
    let Outcome::ActualAccepted { decision } = act_with(
        &mut conn,
        Action::RecordActual {
            receipt: record.id,
            at: at(1200),
        },
        receipt_capability(),
    )
    .await
    else {
        panic!("authorized actual receipt must be retained")
    };
    assert_eq!(decision.state, State::ActualUnbound);
    assert_eq!(decision.bound_request, None);
    let row=sqlx::query!("SELECT request_hash,reserved_units::text AS reserved,actual_units::text AS actual,period_start_ns::text AS \"period_start!\",period_end_ns::text AS \"period_end!\",receipt_event FROM derivation.model_calls").fetch_one(&mut *conn).await.unwrap();
    assert_eq!(row.actual.as_deref(), Some("7"));
    assert_eq!(
        (
            accounting::timestamp(&row.period_start).unwrap(),
            accounting::timestamp(&row.period_end).unwrap()
        ),
        (at(0), at(1000)),
        "observation clock is not an incurred period"
    );
    assert!(row.request_hash.is_none());
    assert!(row.reserved.is_none());
    assert_eq!(row.receipt_event, Some(record.id.0));
}

#[devkit::test]
async fn later_binding_merges_one_pending_hold_and_unbound_actual_atomically_across_restart(
    db: devkit::pg::TestDatabase,
) {
    let (mut conn, policy, request, event) = fixture(&db).await;
    act(
        &mut conn,
        Action::AuthorizePolicy {
            policy: policy.id,
            at: at(200),
        },
    )
    .await;
    act(
        &mut conn,
        Action::Reserve {
            request: event.id,
            at: at(200),
        },
    )
    .await;
    let receipt_payload = receipt(policy.id, 7);
    let record = captured(&receipt_payload);
    admit(&mut conn, vec![record.clone()]).await;
    act_with(
        &mut conn,
        Action::RecordActual {
            receipt: record.id,
            at: at(1200),
        },
        receipt_capability(),
    )
    .await;
    let native = receipt_payload.receipt.identity();
    let entry = sqlx::query_scalar!(
        "SELECT entry_id FROM derivation.model_calls WHERE receipt_hash=decode($1,'hex')",
        native
    )
    .fetch_one(&mut *conn)
    .await
    .unwrap();
    let output = json!({"text":"neutral retained output"});
    let response = captured(&RecordedResponse {
        request_id: request.request_id.clone(),
        request: request.request.clone(),
        output_hash: OutputHash::for_value(&output),
        output,
        actual: receipt_payload.receipt.actual,
        provider_receipt: Some(receipt_payload.receipt.provider_receipt),
        received_at: at(1201),
    });
    admit(&mut conn, vec![response.clone()]).await;
    {
        let mut tx = conn.begin().await.unwrap();
        let result = tx
            .model_call(
                "operator",
                &Action::Reconcile {
                    request: event.id,
                    response: response.id,
                    at: at(1201),
                },
                Capabilities::default(),
            )
            .await
            .unwrap();
        assert!(matches!(result, Outcome::Accepted { .. }));
        assert_eq!(
            sqlx::query_scalar!("SELECT count(*) FROM derivation.model_calls")
                .fetch_one(&mut *tx)
                .await
                .unwrap(),
            Some(1)
        );
        tx.rollback().await.unwrap();
    }
    assert_eq!(
        sqlx::query_scalar!("SELECT count(*) FROM derivation.model_calls")
            .fetch_one(&mut *conn)
            .await
            .unwrap(),
        Some(2)
    );
    conn.close().await.unwrap();
    let mut conn = AdmissionContext::connect(&db.url(None)).await.unwrap();
    let Outcome::Accepted { decision } = act(
        &mut conn,
        Action::Reconcile {
            request: event.id,
            response: response.id,
            at: at(1300),
        },
    )
    .await
    else {
        panic!("binding retained actual must merge")
    };
    assert!(decision.reused);
    assert_eq!(decision.actual_units, Some(7));
    let row=sqlx::query!("SELECT entry_id,request_hash,period_start_ns::text AS \"period_start!\",period_end_ns::text AS \"period_end!\",receipt_event,coalesce(actual_units,reserved_units)::text AS amount FROM derivation.model_calls").fetch_one(&mut *conn).await.unwrap();
    assert_eq!(row.entry_id, entry);
    assert!(row.request_hash.is_some());
    assert_eq!(row.receipt_event, Some(record.id.0));
    assert_eq!(
        (
            accounting::timestamp(&row.period_start).unwrap(),
            accounting::timestamp(&row.period_end).unwrap()
        ),
        (at(0), at(1000))
    );
    assert_eq!(row.amount.as_deref(), Some("7"));
    let Outcome::ActualAccepted { decision } = act_with(
        &mut conn,
        Action::RecordActual {
            receipt: record.id,
            at: at(1400),
        },
        receipt_capability(),
    )
    .await
    else {
        panic!("same receipt must reuse bound charge")
    };
    assert!(decision.reused);
    assert_eq!(decision.state, State::Recorded);
    assert_eq!(decision.bound_request, Some(request.request_id));
    assert_eq!(
        sqlx::query_scalar!("SELECT count(*) FROM derivation.model_calls")
            .fetch_one(&mut *conn)
            .await
            .unwrap(),
        Some(1)
    );
    assert_eq!(
        sqlx::query_scalar!("SELECT count(*) FROM core.events WHERE id=$1", event.id.0)
            .fetch_one(&mut *conn)
            .await
            .unwrap(),
        Some(1),
        "merging accounting retains source request facts"
    );
}

#[devkit::test]
async fn contradictory_receipts_never_move_frozen_period_or_replace_known_amount(
    db: devkit::pg::TestDatabase,
) {
    let (mut conn, first, _, _) = fixture(&db).await;
    act(
        &mut conn,
        Action::AuthorizePolicy {
            policy: first.id,
            at: at(200),
        },
    )
    .await;
    let original = receipt(first.id, 7);
    let record = captured(&original);
    admit(&mut conn, vec![record.clone()]).await;
    act_with(
        &mut conn,
        Action::RecordActual {
            receipt: record.id,
            at: at(1200),
        },
        receipt_capability(),
    )
    .await;
    let mut changed = original.clone();
    changed.receipt.actual.units = 8;
    let changed = captured(&changed);
    admit(&mut conn, vec![changed.clone()]).await;
    assert_eq!(
        act_with(
            &mut conn,
            Action::RecordActual {
                receipt: changed.id,
                at: at(1201)
            },
            receipt_capability()
        )
        .await,
        refused(Refusal::ContradictoryReceipt {
            receipt: original.receipt.identity()
        })
    );
    let mut revised = policy("2", 50, 20);
    revised.policy.budget.period_start = at(1000);
    revised.policy.budget.period_end = at(2000);
    let revised = captured(&revised);
    admit(&mut conn, vec![revised.clone()]).await;
    act(
        &mut conn,
        Action::AuthorizePolicy {
            policy: revised.id,
            at: at(1201),
        },
    )
    .await;
    let mut changed = original.clone();
    changed.policy = revised.id;
    changed.receipt.policy_version = "2".into();
    changed.receipt.basis.period_start = at(1000);
    changed.receipt.basis.period_end = at(2000);
    let changed = captured(&changed);
    admit(&mut conn, vec![changed.clone()]).await;
    assert_eq!(
        act_with(
            &mut conn,
            Action::RecordActual {
                receipt: changed.id,
                at: at(1202)
            },
            receipt_capability()
        )
        .await,
        refused(Refusal::ContradictoryReceipt {
            receipt: original.receipt.identity()
        })
    );
    let row=sqlx::query!("SELECT actual_units::text AS actual,period_start_ns::text AS \"period_start!\",period_end_ns::text AS \"period_end!\",policy_version FROM derivation.model_calls").fetch_one(&mut *conn).await.unwrap();
    assert_eq!(row.actual.as_deref(), Some("7"));
    assert_eq!(row.policy_version, "1");
    assert_eq!(
        (
            accounting::timestamp(&row.period_start).unwrap(),
            accounting::timestamp(&row.period_end).unwrap()
        ),
        (at(0), at(1000))
    );
}

#[devkit::test]
async fn actual_unbound_overrun_is_recorded_and_blocks_future_reservations(
    db: devkit::pg::TestDatabase,
) {
    let (mut conn, policy, _, event) = fixture(&db).await;
    act(
        &mut conn,
        Action::AuthorizePolicy {
            policy: policy.id,
            at: at(200),
        },
    )
    .await;
    let record = captured(&receipt(policy.id, 55));
    admit(&mut conn, vec![record.clone()]).await;
    let Outcome::ActualAccepted { decision } = act_with(
        &mut conn,
        Action::RecordActual {
            receipt: record.id,
            at: at(1200),
        },
        receipt_capability(),
    )
    .await
    else {
        panic!("known actual overrun must be recorded")
    };
    assert!(decision.overrun);
    assert_eq!(
        sqlx::query_scalar!("SELECT actual_units::text FROM derivation.model_calls")
            .fetch_one(&mut *conn)
            .await
            .unwrap(),
        Some("55".into())
    );
    let result = act(
        &mut conn,
        Action::Reserve {
            request: event.id,
            at: at(200),
        },
    )
    .await;
    assert!(matches!(
        result,
        Outcome::Refused {
            refusal: Refusal::InsufficientConfiguredBudget { .. }
        }
    ));
    assert_eq!(
        sqlx::query_scalar!("SELECT count(*) FROM derivation.model_calls")
            .fetch_one(&mut *conn)
            .await
            .unwrap(),
        Some(1)
    );
}

async fn overflow_fixture(
    db: &devkit::pg::TestDatabase,
) -> (AdmissionContext, Event, ModelRequest, Event) {
    let mut conn = AdmissionContext::connect(&db.url(None)).await.unwrap();
    let target = input("overflow quote input");
    let mut configured = policy("1", 35, 10);
    configured.policy.routes[0].prices.input_byte = u64::MAX;
    let configured = captured(&configured);
    admit(&mut conn, vec![target.clone(), configured.clone()]).await;
    act(
        &mut conn,
        Action::AuthorizePolicy {
            policy: configured.id,
            at: at(200),
        },
    )
    .await;
    let request = requested(configured.id, &target);
    let event = captured(&request);
    admit(&mut conn, vec![event.clone()]).await;
    (conn, configured, request, event)
}
#[devkit::test]
async fn unknown_quote_refuses_new_reservation_but_never_discards_incurred_actual(
    db: devkit::pg::TestDatabase,
) {
    let (mut conn, _, request, event) = overflow_fixture(&db).await;
    let result = act(
        &mut conn,
        Action::Reserve {
            request: event.id,
            at: at(200),
        },
    )
    .await;
    assert!(
        matches!(result,Outcome::Refused{refusal:Refusal::Invalid{detail}} if detail.contains("estimate exceeds"))
    );
    assert_eq!(
        sqlx::query_scalar!("SELECT count(*) FROM derivation.model_calls")
            .fetch_one(&mut *conn)
            .await
            .unwrap(),
        Some(0)
    );
    let output = json!({"text":"known incurred output"});
    let response = captured(&RecordedResponse {
        request_id: request.request_id.clone(),
        request: request.request,
        output_hash: OutputHash::for_value(&output),
        output,
        actual: Charge {
            currency: "TEST".into(),
            unit: "fixture-unit".into(),
            units: 7,
        },
        provider_receipt: Some("unrepresentable-quote-fixture".into()),
        received_at: at(1200),
    });
    admit(&mut conn, vec![response.clone()]).await;
    for (observed, reused) in [(1200, false), (1300, true)] {
        let Outcome::Accepted { decision } = act(
            &mut conn,
            Action::Reconcile {
                request: event.id,
                response: response.id,
                at: at(observed),
            },
        )
        .await
        else {
            panic!("exact incurred actual is not an estimate")
        };
        assert_eq!(decision.reserved_units, None);
        assert_eq!(decision.actual_units, Some(7));
        assert_eq!(decision.state, State::Recorded);
        assert_eq!(decision.reused, reused);
    }
    let row=sqlx::query!("SELECT reserved_units::text AS reserved,actual_units::text AS actual,period_start_ns::text AS \"period_start!\",period_end_ns::text AS \"period_end!\",reason FROM derivation.model_calls").fetch_one(&mut *conn).await.unwrap();
    assert_eq!(row.reserved, None);
    assert_eq!(row.actual.as_deref(), Some("7"));
    assert!(row.reason.as_deref().unwrap().contains("estimate exceeds"));
    assert_eq!(
        (
            accounting::timestamp(&row.period_start).unwrap(),
            accounting::timestamp(&row.period_end).unwrap()
        ),
        (at(0), at(1000))
    );
}
#[devkit::test]
async fn an_unbound_actual_can_bind_with_an_unrepresentable_quote_without_moving_charge(
    db: devkit::pg::TestDatabase,
) {
    let (mut conn, policy, request, event) = overflow_fixture(&db).await;
    let actual = receipt(policy.id, 7);
    let source = captured(&actual);
    admit(&mut conn, vec![source.clone()]).await;
    act_with(
        &mut conn,
        Action::RecordActual {
            receipt: source.id,
            at: at(1200),
        },
        receipt_capability(),
    )
    .await;
    let native = actual.receipt.identity();
    let entry = sqlx::query_scalar!(
        "SELECT entry_id FROM derivation.model_calls WHERE receipt_hash=decode($1,'hex')",
        native
    )
    .fetch_one(&mut *conn)
    .await
    .unwrap();
    let output = json!({"text":"known unbound output"});
    let response = captured(&RecordedResponse {
        request_id: request.request_id,
        request: request.request,
        output_hash: OutputHash::for_value(&output),
        output,
        actual: actual.receipt.actual,
        provider_receipt: Some(actual.receipt.provider_receipt),
        received_at: at(1201),
    });
    admit(&mut conn, vec![response.clone()]).await;
    let Outcome::Accepted { decision } = act(
        &mut conn,
        Action::Reconcile {
            request: event.id,
            response: response.id,
            at: at(1201),
        },
    )
    .await
    else {
        panic!("known actual must bind independently of an unavailable estimate")
    };
    assert_eq!(decision.reserved_units, None);
    assert_eq!(decision.actual_units, Some(7));
    assert!(decision.reused);
    let row=sqlx::query!("SELECT entry_id,period_start_ns::text AS \"period_start!\",period_end_ns::text AS \"period_end!\",actual_units::text AS actual FROM derivation.model_calls").fetch_one(&mut *conn).await.unwrap();
    assert_eq!(row.entry_id, entry);
    assert_eq!(row.actual.as_deref(), Some("7"));
    assert_eq!(
        (
            accounting::timestamp(&row.period_start).unwrap(),
            accounting::timestamp(&row.period_end).unwrap()
        ),
        (at(0), at(1000))
    );
    assert_eq!(
        sqlx::query_scalar!("SELECT count(*) FROM derivation.model_calls")
            .fetch_one(&mut *conn)
            .await
            .unwrap(),
        Some(1)
    );
}
