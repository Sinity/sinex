use super::*;
use serde_json::json;

fn config(desktop: bool) -> Config {
    serde_json::from_value(json!({
        "interrupt_budget_per_hour": 4,
        "quiet_hours": null,
        "notifications": if desktop { json!({"away_command": null, "bus_address": null}) } else { json!(null) },
        "values": {"recorded": {"kind": "file", "path": "/synthetic/value.json"}},
        "actions": {"controlled": {"program": "/synthetic/controlled", "arguments": []}},
        "outlet_timeout_ms": 1000
    }))
    .unwrap()
}

fn rule(id: &str, deliver: &str) -> sinex_store::signals::Rule {
    sinex_store::signals::Rule {
        active: ::signals::ActiveRule {
            version: ::signals::RuleVersion {
                product: PRODUCT.into(),
                id: id.into(),
                revision: 1,
            },
            rule: serde_json::from_value(json!({
                "name": id,
                "query": "events",
                "deliver": deliver,
                "target": (deliver == "value").then_some("recorded"),
                "command": (deliver == "action").then_some("controlled"),
            }))
            .unwrap(),
            author: ::signals::Author::Operator,
            via: "api".into(),
            live: false,
            shipped: Default::default(),
        },
        event: EventId::new(),
        class: Class::Guarded,
    }
}

fn delivery(
    product: &str,
    rule: &str,
    occurrence: &str,
    channel: ::signals::Channel,
    decision: ::signals::Decision,
    current: bool,
) -> ::signals::Delivery {
    let trigger = ::signals::Trigger {
        rule: ::signals::RuleVersion {
            product: product.into(),
            id: rule.into(),
            revision: 1,
        },
        occurrence: ::signals::Occurrence {
            id: occurrence.into(),
            period: "native".into(),
        },
    };
    ::signals::Delivery {
        fired: ::signals::Fired {
            id: trigger.delivery_id(),
            rule_name: rule.into(),
            matched: 1,
            at: 1,
            basis: vec![::signals::Row {
                occurrence: trigger.occurrence.clone(),
                reference: "sinex:ev/0190a1b2-0000-7000-8000-000000000001".into(),
                observed: 1,
                start: 1,
                end: None,
                origin: ::signals::Origin::Source,
                evidence: json!({"text": "synthetic"}),
            }],
            trigger,
            channel,
            change: None,
            value: None,
            scope: None,
            to: vec![],
            author: ::signals::Author::Operator,
            quiet_hours: None,
            rate_limit: None,
            current,
            shipped: Default::default(),
        },
        at: 1,
        decision,
        held: vec![],
        attention: Default::default(),
    }
}

fn state() -> Checkpoint {
    let day = query_language::time::DaySetting::new(
        jiff::tz::TimeZone::get("UTC").unwrap(),
        jiff::civil::Time::constant(4, 0, 0, 0),
    )
    .unwrap();
    Checkpoint {
        settings_identity: "synthetic".into(),
        evaluator: ::signals::Evaluator::new(0, vec![], &::signals::DayCalendar::new(day)).unwrap(),
        host: ::signals::HostPolicy::new(config(true).host_settings(), Default::default()),
        notifications: Default::default(),
        rules: Default::default(),
        rows: Default::default(),
        inputs: Default::default(),
        open: Default::default(),
        outbox: Default::default(),
        forwarded: Default::default(),
        reports_seq: 0,
        observed_seq: 0,
    }
}

fn parents(event: &sinex_model::Event) -> Vec<EventId> {
    match &event.origin {
        sinex_model::Origin::Derived { parents } => parents.clone(),
        origin => panic!("a delivery is derived: {origin:?}"),
    }
}

fn mark() -> sinex_store::ReadMark {
    sinex_store::ReadMark::default()
}

fn handoffs(deliveries: Vec<::signals::Delivery>) -> ::signals::Handoffs {
    ::signals::Handoffs {
        deliveries,
        ..Default::default()
    }
}

/// law: signal-delivery-cites-rule-and-inputs
/// A delivery is derived from the rule that fired and from every reading
/// its matched row was computed from, including a group's selected readings.
#[devkit::test]
fn a_delivery_cites_its_rule_and_the_readings_its_row_was_computed_from() {
    let mut state = state();
    let own = rule("grouped", "notice");
    let rule_event = own.event;
    state.rules.insert("grouped".into(), own);
    let selected = vec![EventId::new(), EventId::new()];
    // The mark the row was first read under, older than the handoff's.
    let read = sinex_store::ReadMark {
        changes: 3,
        tombstoned: 0,
        policy_epoch: Some(1),
    };
    state.inputs.insert(
        "grouped".into(),
        [(
            "group-a".to_string(),
            sinex_store::signals::Inputs {
                parents: selected.clone(),
                read,
            },
        )]
        .into(),
    );
    let handed = delivery(
        PRODUCT,
        "grouped",
        "group-a",
        ::signals::Channel::Notice,
        ::signals::Decision::Passive {
            reason: ::signals::PassiveReason::RuleIsPassive,
        },
        false,
    );
    let (events, records) = hand_off(
        &config(true),
        &mut state,
        handoffs(vec![handed.clone()]),
        mark(),
        Timestamp::from_unix_secs(1_800_000_000).unwrap(),
    )
    .unwrap();
    assert!(records.is_empty());
    let mut expected = vec![rule_event, selected[0], selected[1]];
    expected.sort_unstable();
    assert_eq!(parents(&events[0].0), expected);
    assert_eq!(events[0].0.payload["delivery_id"], handed.fired.id);
    assert_eq!(events[0].0.class, Class::Guarded);
    assert_eq!(
        events[0].1, read,
        "admitted under the mark its row was read with"
    );
    // A passive notice owes no outlet, and the row left the answer.
    assert!(state.outbox.is_empty());
    assert!(state.inputs.is_empty());
}

/// law: signal-forwarded-delivery-cites-report
/// A forwarded firing is derived from the report reading that carried it;
/// a delivery naming neither a rule here nor a forwarded report is refused.
#[devkit::test]
fn a_forwarded_delivery_cites_its_report_and_an_unknown_one_is_refused() {
    let mut state = state();
    let handed = delivery(
        "polylogue",
        "authored:watch",
        "session-a",
        ::signals::Channel::Notice,
        ::signals::Decision::Passive {
            reason: ::signals::PassiveReason::RuleIsPassive,
        },
        false,
    );
    let at = Timestamp::from_unix_secs(1_800_000_000).unwrap();
    assert!(
        hand_off(
            &config(true),
            &mut state,
            handoffs(vec![handed.clone()]),
            mark(),
            at
        )
        .is_err()
    );
    let report = EventId::new();
    state.forwarded.insert(
        handed.fired.id.clone(),
        Forwarded {
            report,
            class: Class::Open,
        },
    );
    let (events, _) = hand_off(
        &config(true),
        &mut state,
        handoffs(vec![handed]),
        mark(),
        at,
    )
    .unwrap();
    assert_eq!(parents(&events[0].0), vec![report]);
    assert_eq!(events[0].0.class, Class::Open);
    assert!(state.forwarded.is_empty());
}

/// law: signal-outlet-per-decision
/// An interrupting decision owes a desktop notice when notifications are
/// configured; a run decision owes its rule's configured value or action; a
/// forwarded run decision is recorded not sent, since its outlet belongs to
/// its own product.
#[devkit::test]
fn each_decision_owes_the_outlet_it_names() {
    let mut state = state();
    state.rules.insert("shown".into(), rule("shown", "notice"));
    state
        .rules
        .insert("written".into(), rule("written", "value"));
    state.rules.insert("run".into(), rule("run", "action"));
    let shown = delivery(
        PRODUCT,
        "shown",
        "a",
        ::signals::Channel::Interrupt,
        ::signals::Decision::Show { send_away: false },
        false,
    );
    let written = delivery(
        PRODUCT,
        "written",
        "a",
        ::signals::Channel::Value,
        ::signals::Decision::Run,
        false,
    );
    let run = delivery(
        PRODUCT,
        "run",
        "a",
        ::signals::Channel::Action,
        ::signals::Decision::Run,
        false,
    );
    let forwarded = delivery(
        "polylogue",
        "authored:value",
        "a",
        ::signals::Channel::Value,
        ::signals::Decision::Run,
        false,
    );
    state.forwarded.insert(
        forwarded.fired.id.clone(),
        Forwarded {
            report: EventId::new(),
            class: Class::Open,
        },
    );
    let at = Timestamp::from_unix_secs(1_800_000_000).unwrap();
    let (_, records) = hand_off(
        &config(true),
        &mut state,
        handoffs(vec![
            shown.clone(),
            written.clone(),
            run.clone(),
            forwarded.clone(),
        ]),
        mark(),
        at,
    )
    .unwrap();
    let Some(Pending::Notice { notice }) = state.outbox.get(shown.id()) else {
        panic!("an interrupting decision owes a notice: {:?}", state.outbox);
    };
    assert_eq!(notice.delivery_id, shown.fired.id);
    assert!(notice.outlets.desktop && !notice.outlets.command);
    assert_eq!(
        state.outbox.get(written.id()),
        Some(&Pending::Value {
            target: "recorded".into(),
            delivery: Box::new(written.clone()),
        })
    );
    assert_eq!(
        state.outbox.get(run.id()),
        Some(&Pending::Action {
            command: "controlled".into(),
            reference: "sinex:ev/0190a1b2-0000-7000-8000-000000000001".into(),
        })
    );
    assert_eq!(state.outbox.len(), 3);
    assert_eq!(
        records,
        vec![outcome(
            forwarded.id(),
            ::signals::Outcome::Attempt {
                result: ::signals::DeliveryOutcome::NotSent(
                    "outlet_not_configured_on_this_host".into()
                ),
            },
            at,
        )]
    );
    // Without desktop notifications, an interrupting decision owes nothing.
    let mut quiet = self::state();
    quiet.rules.insert("shown".into(), rule("shown", "notice"));
    hand_off(
        &config(false),
        &mut quiet,
        handoffs(vec![shown]),
        mark(),
        at,
    )
    .unwrap();
    assert!(quiet.outbox.is_empty());
}

/// law: signal-cleared-closes-or-never-shows
/// A cleared current-state delivery that was shown is closed; one whose
/// notice was not begun yet is never shown on its own: its owed notice is
/// replaced by a clear.
#[devkit::test]
fn a_cleared_delivery_is_closed_once_shown_and_never_shown_otherwise() {
    let mut state = state();
    state.rules.insert("open".into(), rule("open", "notice"));
    let shown = delivery(
        PRODUCT,
        "open",
        "a",
        ::signals::Channel::Interrupt,
        ::signals::Decision::Show { send_away: false },
        true,
    );
    let unsent = delivery(
        PRODUCT,
        "open",
        "b",
        ::signals::Channel::Interrupt,
        ::signals::Decision::Show { send_away: false },
        true,
    );
    let at = Timestamp::from_unix_secs(1_800_000_000).unwrap();
    hand_off(
        &config(true),
        &mut state,
        handoffs(vec![shown.clone(), unsent.clone()]),
        mark(),
        at,
    )
    .unwrap();
    assert_eq!(state.open.len(), 2);
    let Some(Pending::Notice { notice }) = state.outbox.get(shown.id()).cloned() else {
        panic!("shown delivery owes a notice");
    };
    // The desktop deliverer began the first notice; settling drops it.
    assert!(!state.notifications.begin(notice).is_empty());
    settle(&mut state);
    assert!(!state.outbox.contains_key(shown.id()));
    let cleared = |id: &str| ::signals::OutcomeRecord {
        delivery: id.into(),
        at: 2,
        outcome: ::signals::Outcome::Cleared,
    };
    let (_, records) = hand_off(
        &config(true),
        &mut state,
        ::signals::Handoffs {
            outcomes: vec![cleared(shown.id()), cleared(unsent.id())],
            ..Default::default()
        },
        mark(),
        at,
    )
    .unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(state.outbox.get(shown.id()), Some(&Pending::Clear));
    // The unsent notice is never shown: its notice is replaced by a clear,
    // which closes it if it is already on the way to the deliverer.
    assert_eq!(state.outbox.get(unsent.id()), Some(&Pending::Clear));
    assert!(state.open.is_empty());
}
