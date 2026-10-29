use super::*;
use sinex_protocol::{
    CoverageStatus, OfflineEmergency, OfflineEmergencyRoute, PublicContext, PublicInterruptBudget,
};

fn at(seconds: i64) -> Timestamp {
    Timestamp::from_unix_secs(seconds).unwrap()
}

fn fixture() -> (sinex_protocol::signals::EmergencyCompletion, PublicContext) {
    let issued = 1_800_000_000; // 08:00 UTC.
    let invoked = issued + 3599;
    let trigger = sinex_protocol::signals::emergency_trigger(sinex_model::EventId::new());
    let id = trigger.delivery_id();
    let notice = sinex_protocol::signals::emergency_notice(&id);
    let mut state = ::signals::notifications::NotificationState::default();
    let receipts = state.begin(notice.clone(), ::signals::notifications::Route::Dbus);
    let attempt = state.release(&id).unwrap();
    let completion = sinex_protocol::signals::EmergencyCompletion {
        inputs: vec![],
        delivery_id: id.clone(),
        host: "fixture".into(),
        context_token: "verified-owner-token".into(),
        intent: ::signals::FiredIntent {
            delivery: ::signals::Delivery {
                id,
                trigger,
                matched: invoked * 1000,
                at: invoked * 1000,
                basis: vec![],
                channel: ::signals::Channel::Interrupt,
                downgrade: None,
                change: None,
                value: None,
                scope: None,
            },
            author: ::signals::Author::Shipped,
            quiet_hours: None,
            rate_limit: None,
            current: true,
        },
        notice,
        receipts,
        attempt,
        invoked_at: at(invoked),
        completed_at: at(issued + 3720),
    };
    let context = PublicContext::Available {
        context_token: "verified-owner-token".into(),
        settings_identity: "settings-a".into(),
        owner_revision: 1,
        day: serde_json::from_value(json!({"zone":"UTC","start":"04:00:00"})).unwrap(),
        quiet_hours: Some(::signals::QuietHours {
            from: 9 * 60,
            to: 10 * 60,
        }),
        interrupt_budget: PublicInterruptBudget {
            per_hour: 1,
            used: 1,
            remaining: 0,
            window_ms: 3_600_000,
        },
        observed_at: at(issued),
        coverage: CoverageStatus::NotRequested,
        offline_emergency: Some(OfflineEmergency {
            producer: "primary".into(),
            host: "fixture".into(),
            settings_identity: "settings-a".into(),
            rule: sinex_protocol::signals::emergency_rule(),
            route: OfflineEmergencyRoute::Desktop,
            issued_at: at(issued),
            expires_at: at(issued + 3600),
        }),
    };
    (completion, context)
}

#[devkit::test]
fn recovery_checks_invocation_authority_before_slow_receipt_not_recovery_clock() {
    let (completion, context) = fixture();
    sinex_protocol::signals::validate_emergency(&completion, "polylogue").unwrap();
    assert_eq!(
        emergency_policy(
            &completion,
            "primary",
            "fixture",
            &context,
            at(1_800_010_000)
        )
        .unwrap(),
        "settings-a"
    );
    let mut expired = completion.clone();
    expired.invoked_at = at(1_800_003_600);
    assert!(
        emergency_policy(&expired, "primary", "fixture", &context, at(1_800_010_000))
            .unwrap_err()
            .contains("authority")
    );
    let mut quiet = context.clone();
    if let PublicContext::Available {
        offline_emergency: Some(capability),
        ..
    } = &mut quiet
    {
        capability.expires_at = at(1_800_007_200);
    }
    assert!(
        emergency_policy(&expired, "primary", "fixture", &quiet, at(1_800_010_000))
            .unwrap_err()
            .contains("quiet")
    );
}

#[devkit::test]
fn recovery_requires_signed_exact_offline_capability_and_ordered_observations() {
    let (completion, context) = fixture();
    for (producer, host) in [("foreign", "fixture"), ("primary", "foreign")] {
        assert!(
            emergency_policy(
                &completion,
                producer,
                host,
                &context,
                completion.completed_at
            )
            .is_err()
        );
    }
    let mut missing = context.clone();
    if let PublicContext::Available {
        offline_emergency, ..
    } = &mut missing
    {
        *offline_emergency = None;
    }
    assert!(
        emergency_policy(
            &completion,
            "primary",
            "fixture",
            &missing,
            completion.completed_at
        )
        .unwrap_err()
        .contains("lacks signed")
    );
    for mutate in [
        |capability: &mut OfflineEmergency| capability.rule.fingerprint.push('0'),
        |capability: &mut OfflineEmergency| capability.settings_identity = "other-settings".into(),
        |capability: &mut OfflineEmergency| capability.issued_at = at(1_800_003_600),
    ] {
        let mut changed = context.clone();
        if let PublicContext::Available {
            offline_emergency: Some(capability),
            ..
        } = &mut changed
        {
            mutate(capability);
        }
        assert!(
            emergency_policy(
                &completion,
                "primary",
                "fixture",
                &changed,
                completion.completed_at
            )
            .is_err()
        );
    }
    let mut before = completion.clone();
    before.completed_at = at(1_800_000_000);
    assert!(
        emergency_policy(
            &before,
            "primary",
            "fixture",
            &context,
            completion.completed_at
        )
        .is_err()
    );
    assert!(
        emergency_policy(
            &completion,
            "primary",
            "fixture",
            &context,
            at(1_800_003_600)
        )
        .is_err(),
        "future receipt observation is refused"
    );
}
