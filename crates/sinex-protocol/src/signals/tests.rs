use super::*;
use ::signals::notifications::*;
use ::signals::*;
use sinex_model::Timestamp;

fn completion() -> EmergencyCompletion {
    let rule = RuleVersion {
        product: "polylogue".into(),
        id: EMERGENCY_RULE_ID.into(),
        revision: EMERGENCY_RULE_REVISION,
    };
    let trigger = Trigger {
        rule,
        occurrence: Occurrence {
            id: "owner-unreachable".into(),
            episode: "observed-interval-1".into(),
        },
    };
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
    let notice = Notice {
        delivery_id: id.clone(),
        product: "polylogue".into(),
        icon: "dialog-warning".into(),
        rule: EMERGENCY_RULE_ID.into(),
        occurrence: "owner-unreachable".into(),
        summary: "Synthetic owner unavailable".into(),
        body: "Synthetic isolated connection failure".into(),
        reference: "sinex:ev/00000000-0000-7000-8000-000000000001".into(),
        command_hint: "polylogue status".into(),
        interrupt: true,
        current: true,
        data_loss: false,
        budget_passive_count: None,
    };
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
fn emergency_transfer_preserves_both_route_receipts_and_used_retry() {
    let mut value = completion();
    value.attempt.dbus = Some(DeliveryOutcome::NotSent("no_notification_server".into()));
    value.attempt.command = Some(DeliveryOutcome::Delivered);
    value.attempt.retried = true;
    value.receipts[0].outcome = value.attempt.dbus.clone().unwrap();
    let mut command = value.receipts[0].clone();
    command.route = Route::Command;
    command.outcome = DeliveryOutcome::Delivered;
    value.receipts.push(command);
    assert!(validate_emergency(&value, "polylogue").is_ok());
    let encoded = serde_json::to_value(&value).unwrap();
    assert_eq!(encoded["attempt"]["retried"], true);
    value.receipts.pop();
    assert!(
        validate_emergency(&value, "polylogue").is_err(),
        "a delivered command route cannot disappear during transfer"
    );
}


fn attested_rule(intent:&::signals::FiredIntent)->::signals::ActiveRule {
    serde_json::from_value(serde_json::json!({"version":intent.delivery.trigger.rule,"rule":{"name":"synthetic compiled display","query":"events","fires":"on_new","for":0,"min_change":1,"deliver":"notice","target":null,"command":null,"interrupt":true,"quiet_hours":null,"rate_limit":null,"scope":null,"enabled":true},"author":intent.author,"via":"compiled","live":true})).unwrap()
}
#[devkit::test]
fn elevated_forwarded_authors_require_exact_configured_attestation() {
    let mut intent=completion().intent;intent.author=::signals::Author::Operator;
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
    let active=attested_rule(&intent);
    let shipped=AuthorAttestation{operator:false,shipped:vec![ShippedRule{id:active.version.id.clone(),revision:active.version.revision,fingerprint:rule_fingerprint(&active)}]};
    authorize_author(&intent,Some(&active),"polylogue",Some(&shipped)).unwrap();
    let mut changed=active.clone();changed.rule.query="events where text = edited".into();
    assert!(authorize_author(&intent,Some(&changed),"polylogue",Some(&shipped)).is_err(),"same display and ID do not attest a different compiled definition");
    let mut authored=active;authored.version.id="authored:same-display".into();intent.delivery.trigger.rule=authored.version.clone();
    assert!(authorize_author(&intent,Some(&authored),"polylogue",Some(&shipped)).is_err());
}
