use super::*;
use ::signals::notifications::*;
use ::signals::*;
use sinex_model::Timestamp;

fn completion() -> EmergencyCompletion {
    let trigger=emergency_trigger(sinex_model::EventId::new());
    let id = trigger.delivery_id();
    let at = 1_800_000_000_000;
    let delivery = Delivery {
        id: id.clone(),
        trigger,
        matched: at - 1,
        at,
        basis: vec![],
        channel: Channel::Interrupt,
        downgrade: None,
        change: None,
        value: None,
        scope: None,
    };
    let notice=emergency_notice(&id);
    let mut state = NotificationState::default();
    state.begin(notice.clone(), Route::Dbus);
    let attempt = state.attempt(&id).unwrap().clone();
    EmergencyCompletion {
        inputs: vec![],
        delivery_id: id.clone(),
        host: "fixture".into(),
        context_token: "opaque-owner-issued-proof".into(),
        intent: FiredIntent {
            delivery,
            author: Author::Shipped,
            quiet_hours: None,
            rate_limit: None,
            current: true,
        },
        notice: notice.clone(),
        receipts: vec![NotificationReceipt {
            delivery_id: id,
            route: Route::Dbus,
            outcome: DeliveryOutcome::Unknown,
            notification_id: None,
            event: None,
            via: None,
        }],
        attempt,
        invoked_at: Timestamp::from_unix_secs(at / 1000).unwrap(),
        completed_at: Timestamp::from_unix_secs(at / 1000).unwrap(),
    }
}
#[devkit::test]
fn emergency_completion_retains_actual_unknown_and_refuses_authored_spoofs() {
    let original = completion();
    assert!(validate_emergency(&original, "polylogue").is_ok());
    let encoded = serde_json::to_vec(&original).unwrap();
    let decoded: EmergencyCompletion = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded, original);
    assert_eq!(decoded.receipts[0].outcome, DeliveryOutcome::Unknown);
    for mutate in [
        |value: &mut EmergencyCompletion| value.intent.author = Author::Operator,
        |value: &mut EmergencyCompletion| {
            value.intent.delivery.trigger.rule.id = "authored:shipped:sinex-blocked".into()
        },
        |value: &mut EmergencyCompletion| value.intent.delivery.trigger.rule.revision += 1,
        |value: &mut EmergencyCompletion| value.notice.data_loss = true,
        |value: &mut EmergencyCompletion| value.receipts[0].outcome = DeliveryOutcome::Delivered,
        |value: &mut EmergencyCompletion| {
            value.attempt.notice.body = "different frozen body".into()
        },
        |value: &mut EmergencyCompletion| value.receipts.clear(),
        |value: &mut EmergencyCompletion| {
            value.completed_at = Timestamp::from_unix_secs(1).unwrap()
        },
    ] {
        let mut value = original.clone();
        mutate(&mut value);
        assert!(validate_emergency(&value, "polylogue").is_err());
    }
    assert!(validate_emergency(&original, "other-family").is_err());
}
#[devkit::test]
fn emergency_transfer_preserves_desktop_retry_and_refuses_command_authority() {
    let mut value = completion();
    value.attempt.dbus = Some(DeliveryOutcome::NotSent("no_notification_server".into()));
    value.attempt.retried = true;
    value.receipts[0].outcome = value.attempt.dbus.clone().unwrap();
    assert!(validate_emergency(&value, "polylogue").is_ok());
    let encoded = serde_json::to_value(&value).unwrap();
    assert_eq!(encoded["attempt"]["retried"], true);
    value.attempt.command = Some(DeliveryOutcome::Delivered);
    let mut command = value.receipts[0].clone();
    command.route = Route::Command;
    command.outcome = DeliveryOutcome::Delivered;
    value.receipts.push(command);
    assert!(validate_emergency(&value, "polylogue").is_err(),"offline desktop authority cannot attest a command effect");
    value.receipts.pop();
    assert!(
        validate_emergency(&value, "polylogue").is_err(),
        "a delivered command route cannot disappear during transfer"
    );
}

#[devkit::test]
fn emergency_keeps_invocation_separate_from_slow_receipt_observation() {
    let mut value=completion();
    value.completed_at=Timestamp::from_unix_secs(1_800_000_120).unwrap();
    assert!(validate_emergency(&value,"polylogue").is_ok());
    value.invoked_at=Timestamp::from_unix_secs(1_800_000_121).unwrap();
    assert!(validate_emergency(&value,"polylogue").is_err());
    let mut encoded=serde_json::to_value(completion()).unwrap();
    encoded.as_object_mut().unwrap().remove("invoked_at");
    assert!(serde_json::from_value::<EmergencyCompletion>(encoded).is_err(),"a receipt clock cannot stand in for missing invocation observation");
}


fn attested_rule(intent:&::signals::FiredIntent)->::signals::ActiveRule {
    serde_json::from_value(serde_json::json!({"version":intent.delivery.trigger.rule,"rule":{"name":"synthetic compiled display","query":"events","fires":"on_new","for":0,"min_change":1,"deliver":"notice","target":null,"command":null,"interrupt":true,"quiet_hours":null,"rate_limit":null,"scope":null,"enabled":true},"author":intent.author,"via":"compiled","live":true})).unwrap()
}
#[devkit::test]
fn elevated_forwarded_authors_require_exact_configured_attestation() {
    let mut intent=completion().intent;intent.author=::signals::Author::Operator;
    intent.delivery.trigger.rule.id="authored:synthetic-operator".into();intent.delivery.id=intent.delivery.trigger.delivery_id();
    let active=attested_rule(&intent);
    assert!(authorize_author(&intent,Some(&active),"polylogue",None).is_err());
    assert!(authorize_author(&intent,Some(&active),"polylogue",Some(&AuthorAttestation::default())).is_err());
    let operator=AuthorAttestation{operator:true,shipped:vec![]};
    assert!(authorize_author(&intent,None,"polylogue",Some(&operator)).is_err());
    authorize_author(&intent,Some(&active),"polylogue",Some(&operator)).unwrap();
    assert!(authorize_author(&intent,Some(&active),"another-family",Some(&operator)).is_err());
    let mut changed=active.clone();changed.author=::signals::Author::Agent("same-name".into());
    assert!(authorize_author(&intent,Some(&changed),"polylogue",Some(&operator)).is_err());
    intent.author=::signals::Author::Shipped;
    intent.delivery.trigger.rule.id="shipped:synthetic-compiled".into();intent.delivery.id=intent.delivery.trigger.delivery_id();
    let active=attested_rule(&intent);
    let shipped=AuthorAttestation{operator:false,shipped:vec![ShippedRule{id:active.version.id.clone(),revision:active.version.revision,fingerprint:rule_fingerprint(&active)}]};
    authorize_author(&intent,Some(&active),"polylogue",Some(&shipped)).unwrap();
    let mut changed=active.clone();changed.rule.query="events where text = edited".into();
    assert!(authorize_author(&intent,Some(&changed),"polylogue",Some(&shipped)).is_err(),"same display and ID do not attest a different compiled definition");
    let mut authored=active;authored.version.id="authored:same-display".into();intent.delivery.trigger.rule=authored.version.clone();
    assert!(authorize_author(&intent,Some(&authored),"polylogue",Some(&shipped)).is_err());
}

#[devkit::test]
fn unprivileged_claim_cannot_preempt_a_compiled_signal_identity() {
    let mut intent=completion().intent;intent.author=::signals::Author::Agent("same-display".into());
    assert!(authorize_author(&intent,None,"polylogue",None).unwrap_err().contains("namespace"));
    intent.author=::signals::Author::Operator;
    assert!(authorize_author(&intent,Some(&attested_rule(&intent)),"polylogue",Some(&AuthorAttestation{operator:true,shipped:vec![]})).is_err());
}

#[devkit::test]
fn compiled_emergency_refuses_dynamic_private_notice_fields() {
    for field in ["summary","body","reference","command_hint","occurrence","icon"] {
        let mut value=completion();
        let mut notice=serde_json::to_value(&value.notice).unwrap();notice[field]=serde_json::json!("synthetic private detail");
        value.notice=serde_json::from_value(notice).unwrap();
        assert!(validate_emergency(&value,"polylogue").is_err(),"field {field} is not the compiled notice");
    }
}

#[devkit::test]
fn compiled_emergency_exports_no_copied_query_or_policy_fields() {
    let mut value=completion();value.intent.delivery.scope=Some("synthetic private scope".into());assert!(validate_emergency(&value,"polylogue").is_err());
    let mut value=completion();value.intent.quiet_hours=Some(::signals::QuietHours{from:0,to:60});assert!(validate_emergency(&value,"polylogue").is_err());
}

#[devkit::test]
fn emergency_trigger_rejects_dynamic_paths_and_non_v7_episodes() {
    for episode in ["synthetic/private/path","00000000-0000-4000-8000-000000000001"] {
        let mut value=completion();value.intent.delivery.trigger.occurrence.episode=episode.into();
        let id=value.intent.delivery.trigger.delivery_id();value.delivery_id=id.clone();value.intent.delivery.id=id.clone();
        value.notice=emergency_notice(&id);
        let mut state=NotificationState::default();state.begin(value.notice.clone(),Route::Dbus);value.attempt=state.attempt(&id).unwrap().clone();
        for receipt in &mut value.receipts {receipt.delivery_id=id.clone();}
        assert!(validate_emergency(&value,"polylogue").unwrap_err().contains("opaque compiled outage episode"));
    }
}
