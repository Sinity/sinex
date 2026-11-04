use super::*;
use sqlx::Connection;

fn calendar(at: Timestamp) -> ::signals::DayCalendar {
    let context = sinex_model::query::Context::new(
        jiff::Timestamp::from_nanosecond(at.unix_ns()).unwrap(),
        jiff::tz::TimeZone::get("UTC").unwrap(),
        jiff::civil::Time::constant(4, 0, 0, 0),
    );
    ::signals::DayCalendar::new(context.clock().unwrap().day)
}

fn checkpoint(at: Timestamp) -> Checkpoint {
    let clock = i64::try_from(at.unix_ns() / 1_000_000).unwrap();
    let calendar = calendar(at);
    Checkpoint {
        settings_identity: "configured-day-policy".into(),
        evaluator: ::signals::Evaluator::new(clock, vec![], &calendar).unwrap(),
        host: ::signals::HostPolicy::new(
            ::signals::HostSettings {
                product: "sinex".into(),
                quiet_hours: Some(::signals::QuietHours {
                    from: 1320,
                    to: 480,
                }),
                away_command: false,
                deep_work_deferral: ::signals::DeepWorkDeferral::default(),
            },
            // One charged handoff: only agents' and producers' interrupting
            // handoffs enter the host budget.
            ::signals::InterruptBudget {
                per_hour: 2,
                handoffs: vec![clock],
            },
        ),
        notifications: Default::default(),
        rules: BTreeMap::new(),
        rows: BTreeMap::new(),
        inputs: BTreeMap::new(),
        open: BTreeMap::new(),
        outbox: BTreeMap::new(),
        forwarded: BTreeMap::new(),
        reports_seq: 0,
        observed_seq: 0,
    }
}

#[devkit::test]
async fn public_policy_is_available_only_for_one_committed_config_and_exposes_counts(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let at = Timestamp::from_unix_secs(1_800_000_000).unwrap();
    assert!(matches!(
        public_host_policy(&mut conn, "fixture", false, "configured-day-policy", at)
            .await
            .unwrap(),
        HostPolicyProjection::Unavailable { .. }
    ));
    assert!(matches!(
        public_host_policy(&mut conn, "fixture", true, "configured-day-policy", at)
            .await
            .unwrap(),
        HostPolicyProjection::Pending { .. }
    ));
    let state = checkpoint(at);
    let mut tx = conn.begin().await.unwrap();
    assert_eq!(save(&mut tx, "fixture", None, &state).await.unwrap(), 1);
    tx.commit().await.unwrap();
    let projection = public_host_policy(&mut conn, "fixture", true, "configured-day-policy", at)
        .await
        .unwrap();
    assert_eq!(
        projection,
        HostPolicyProjection::Available {
            checkpoint_revision: 1,
            settings_identity: "configured-day-policy".into(),
            quiet_hours: Some(::signals::QuietHours {
                from: 1320,
                to: 480
            }),
            interrupt_budget: InterruptBudgetProjection {
                per_hour: 2,
                used: 1,
                remaining: 1,
                window_ms: ::signals::HOUR
            },
            observed_at: at
        }
    );
    let public = serde_json::to_value(projection).unwrap();
    for private in [
        "evaluator",
        "host",
        "handoffs",
        "consumed",
        "pending",
        "notifications",
        "rules",
        "rows",
        "open",
        "outbox",
        "forwarded",
    ] {
        assert!(public.get(private).is_none(), "{private}");
    }
    let pending = serde_json::to_value(
        public_host_policy(&mut conn, "fixture", true, "new-config", at)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        pending,
        serde_json::json!({"state":"pending","reason":"host_policy_settings_changed"})
    );
    sqlx::query("SET ROLE sinex_reader")
        .execute(&mut conn)
        .await
        .unwrap();
    let denied = sqlx::query("SELECT state FROM core.signal_checkpoints")
        .fetch_all(&mut conn)
        .await
        .unwrap_err();
    assert_eq!(
        denied
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("42501")
    );
}

#[devkit::test]
async fn stale_checkpoint_never_overwrites_consumption_or_new_settings(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let mut state = checkpoint(Timestamp::from_unix_secs(1_800_000_000).unwrap());
    let mut tx = conn.begin().await.unwrap();
    save(&mut tx, "fixture", None, &state).await.unwrap();
    tx.commit().await.unwrap();
    state.settings_identity = "new-config".into();
    let mut tx = conn.begin().await.unwrap();
    assert_eq!(save(&mut tx, "fixture", Some(1), &state).await.unwrap(), 2);
    tx.commit().await.unwrap();
    let mut stale = state.clone();
    stale.settings_identity = "stale".into();
    stale.host.budget.handoffs.clear();
    let mut tx = conn.begin().await.unwrap();
    assert!(save(&mut tx, "fixture", Some(1), &stale).await.is_err());
    tx.rollback().await.unwrap();
    let saved = load(&mut conn, "fixture").await.unwrap().unwrap();
    assert_eq!(saved.revision, 2);
    assert_eq!(
        serde_json::to_value(saved.checkpoint).unwrap(),
        serde_json::to_value(state).unwrap()
    );
}
