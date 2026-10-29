use super::*;
use signal_delivery::CancellationToken;
use sqlx::{Connection, PgConnection};
use std::time::Duration;

async fn wait_for_policy_waiter(holder: &mut PgConnection, name: &str) {
    let limit = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        sqlx::query("SELECT pg_stat_clear_snapshot()")
            .execute(&mut *holder)
            .await
            .unwrap();
        let blocked: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock')",
        ).bind(name).fetch_one(&mut *holder).await.unwrap();
        if blocked {
            return;
        }
        assert!(
            tokio::time::Instant::now() < limit,
            "first policy lock did not reach the controlled row lock"
        );
        tokio::task::yield_now().await;
    }
}

async fn reader_pool(url: &str, name: &str) -> sqlx::PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(
            url.parse::<sqlx::postgres::PgConnectOptions>()
                .unwrap()
                .application_name(name),
        )
        .await
        .unwrap()
}

#[devkit::test]
async fn first_policy_lock_retries_the_same_epoch_with_the_original_attempt_deadline(
    db: devkit::pg::TestDatabase,
) {
    let mut writer = PgConnection::connect(&db.url(None)).await.unwrap();
    let known = sinex_store::privacy::projection::epoch(&mut writer)
        .await
        .unwrap();
    assert!(known.ready());
    let mut holder = writer.begin().await.unwrap();
    sqlx::query("UPDATE privacy.policy_state SET active_epoch=active_epoch WHERE id")
        .execute(&mut *holder)
        .await
        .unwrap();
    let pool = reader_pool(&db.url(None), "same_epoch_policy_attempt").await;
    let attempt = AttemptContext::new(Duration::from_secs(10), None, CancellationToken::new());
    let deadline = attempt.deadline();
    let task = tokio::spawn(async move {
        let mut diagnostic = Diagnostic::new(&[]);
        let (tx, epoch, _) = open(&pool, &attempt, known.active, &mut diagnostic, None)
            .await
            .unwrap();
        assert_eq!(
            attempt.deadline(),
            deadline,
            "retry cannot renew the owner budget"
        );
        tx.rollback().await.unwrap();
        pool.close().await;
        epoch
    });
    wait_for_policy_waiter(&mut holder, "same_epoch_policy_attempt").await;
    holder.commit().await.unwrap();
    assert_eq!(task.await.unwrap(), known);
}

#[devkit::test]
async fn first_policy_lock_retry_cannot_remint_authority_after_a_real_epoch_change(
    db: devkit::pg::TestDatabase,
) {
    let mut writer = PgConnection::connect(&db.url(None)).await.unwrap();
    let known = sinex_store::privacy::projection::epoch(&mut writer)
        .await
        .unwrap();
    let mut holder = writer.begin().await.unwrap();
    sqlx::query("UPDATE privacy.policy_state SET desired_epoch=desired_epoch+1,active_epoch=active_epoch+1 WHERE id")
        .execute(&mut *holder).await.unwrap();
    let pool = reader_pool(&db.url(None), "changed_epoch_policy_attempt").await;
    let attempt = AttemptContext::new(Duration::from_secs(10), None, CancellationToken::new());
    let task = tokio::spawn(async move {
        let mut diagnostic = Diagnostic::new(&[]);
        let result = open(&pool, &attempt, known.active, &mut diagnostic, None).await;
        assert!(
            matches!(result, Err(PolicyFailure::Unavailable)),
            "the original known epoch remains authoritative"
        );
        pool.close().await;
    });
    wait_for_policy_waiter(&mut holder, "changed_epoch_policy_attempt").await;
    holder.commit().await.unwrap();
    task.await.unwrap();
}

#[devkit::test]
async fn cancelled_policy_lock_acknowledges_rollback_before_returning_no_authority(
    db: devkit::pg::TestDatabase,
) {
    let mut writer = PgConnection::connect(&db.url(None)).await.unwrap();
    let known = sinex_store::privacy::projection::epoch(&mut writer)
        .await
        .unwrap();
    let mut holder = writer.begin().await.unwrap();
    sqlx::query("SELECT id FROM privacy.policy_state WHERE id FOR UPDATE")
        .execute(&mut *holder)
        .await
        .unwrap();
    let pool = reader_pool(&db.url(None), "cancelled_policy_attempt").await;
    let cancellation = CancellationToken::new();
    let attempt = AttemptContext::new(Duration::from_secs(10), None, cancellation.clone());
    let returned_pool = pool.clone();
    let task = tokio::spawn(async move {
        let mut diagnostic = Diagnostic::new(&[]);
        let result = open(&pool, &attempt, known.active, &mut diagnostic, None).await;
        assert!(matches!(
            result,
            Err(PolicyFailure::Interrupted(
                signal_delivery::Interrupted::Cancelled
            ))
        ));
    });
    wait_for_policy_waiter(&mut holder, "cancelled_policy_attempt").await;
    cancellation.cancel();
    tokio::task::yield_now().await;
    assert!(
        !task.is_finished(),
        "an in-flight server query must finish rollback cleanup first"
    );
    holder.commit().await.unwrap();
    task.await.unwrap();
    // A pool of one can be reused only after the cancelled transaction has
    // acknowledged its rollback; there is no policy transaction left open.
    let idle: bool = sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='cancelled_policy_attempt' AND xact_start IS NOT NULL AND pid<>pg_backend_pid())")
        .fetch_one(&returned_pool).await.unwrap();
    assert!(idle);
    let value: i32 = sqlx::query_scalar("SELECT 1")
        .fetch_one(&returned_pool)
        .await
        .unwrap();
    assert_eq!(value, 1);
    returned_pool.close().await;
}

fn forwarded() -> Forwarded {
    let trigger = ::signals::Trigger {
        rule: ::signals::RuleVersion {
            product: "synthetic-peer".into(),
            id: "authored:controlled".into(),
            revision: 1,
        },
        occurrence: ::signals::Occurrence {
            id: "controlled-native".into(),
            episode: "one".into(),
        },
    };
    let id = trigger.delivery_id();
    let delivery = ::signals::Delivery {
        id: id.clone(),
        trigger,
        matched: 0,
        at: 0,
        basis: vec![],
        channel: ::signals::Channel::Interrupt,
        downgrade: None,
        change: None,
        value: None,
        scope: None,
    };
    Forwarded {
        kind: ForwardedKind::Intent,
        available: true,
        producer: "synthetic-peer".into(),
        event: sinex_model::EventId::new(),
        intent: ::signals::FiredIntent {
            delivery,
            author: ::signals::Author::Agent("fixture".into()),
            quiet_hours: None,
            rate_limit: None,
            current: true,
        },
        notice: ::signals::notifications::Notice {
            delivery_id: id,
            product: "synthetic-peer".into(),
            icon: "fixture".into(),
            rule: "authored:controlled".into(),
            occurrence: "controlled-native".into(),
            summary: "Synthetic".into(),
            body: "Synthetic".into(),
            reference: "status".into(),
            command_hint: "synthetic status".into(),
            interrupt: true,
            current: true,
            data_loss: false,
            budget_passive_count: None,
        },
    }
}

async fn reserved_checkpoint(conn: &mut PgConnection) -> (u64, serde_json::Value) {
    let at = Timestamp::now();
    let context = sinex_model::query::Context::new(
        jiff::Timestamp::from_nanosecond(at.unix_ns()).unwrap(),
        jiff::tz::TimeZone::get("UTC").unwrap(),
        jiff::civil::Time::constant(4, 0, 0, 0),
    );
    let calendar = ::signals::DayCalendar::new(context.clock().unwrap().day);
    let now = millis(at).unwrap();
    let state = Checkpoint {
        settings_identity: "controlled-policy".into(),
        evaluator: ::signals::Evaluator::new(
            now,
            vec![],
            ::signals::Policy { private: vec![] },
            &calendar,
        )
        .unwrap(),
        host_policy: ::signals::HostPolicy::new(
            ::signals::InterruptBudget {
                per_hour: 1,
                handoffs: vec![now],
            },
            None,
        ),
        notifications: Default::default(),
        forwarded: Default::default(),
        outbox: Default::default(),
        rule_events: Default::default(),
        rules: Default::default(),
        match_basis: Default::default(),
        current_inputs: Default::default(),
        rows: Default::default(),
        attempts: Default::default(),
        observed_seq: 0,
        passive_count: 0,
    };
    let mut tx = conn.begin().await.unwrap();
    let revision = sinex_store::signals::save(&mut tx, "fixture", None, &state)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let budget = serde_json::to_value(&state.host_policy.budget).unwrap();
    (revision, budget)
}

#[devkit::test]
async fn first_lock_retry_refreshes_the_peer_answer_without_repeating_budget_reservation(
    db: devkit::pg::TestDatabase,
) {
    use futures::StreamExt;
    for (case, second) in [
        ("ended", Condition::Ended),
        ("incomplete", Condition::Incomplete),
    ] {
        let nats = devkit::nats::test_nats().unwrap();
        let client = async_nats::connect(nats.url()).await.unwrap();
        let subjects = Subjects::new(format!("controlled{}", uuid::Uuid::now_v7().simple()));
        let mut requests = client
            .subscribe(subjects.signal_condition("synthetic-peer"))
            .await
            .unwrap();
        client.flush().await.unwrap();
        let responder = client.clone();
        let answers = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = answers.clone();
        let peer = tokio::spawn(async move {
            while let Some(message) = requests.next().await {
                let index = count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let reply = SignalConditionReply {
                    condition: if index == 0 {
                        Condition::Matching
                    } else {
                        second
                    },
                    reason: None,
                };
                sinex_transport::replies::publish_json(
                    &responder,
                    message.reply.unwrap().to_string(),
                    reply,
                    Duration::from_secs(2),
                )
                .await
                .unwrap();
            }
        });
        let mut writer = PgConnection::connect(&db.url(None)).await.unwrap();
        // Each independent case retains its own actual spent reservation.
        if case == "incomplete" {
            sqlx::query("DELETE FROM core.signal_checkpoints WHERE host='fixture'")
                .execute(&mut writer)
                .await
                .unwrap();
        }
        let baseline = reserved_checkpoint(&mut writer).await;
        let known = sinex_store::privacy::projection::epoch(&mut writer)
            .await
            .unwrap();
        let mut holder = writer.begin().await.unwrap();
        sqlx::query("UPDATE privacy.policy_state SET active_epoch=active_epoch WHERE id")
            .execute(&mut *holder)
            .await
            .unwrap();
        let name = format!("peer_retry_{case}");
        let pool = reader_pool(&db.url(None), &name).await;
        let attempt = AttemptContext::new(Duration::from_secs(10), None, CancellationToken::new());
        let task = tokio::spawn(async move {
            let mut diagnostic = Diagnostic::new(&[]);
            let peers = PeerConditions {
                client: &client,
                subjects: &subjects,
                evaluator_ms: 0,
                forwarded: vec![forwarded()],
            };
            let (tx, _, mut conditions) =
                open(&pool, &attempt, known.active, &mut diagnostic, Some(&peers))
                    .await
                    .unwrap();
            tx.rollback().await.unwrap();
            pool.close().await;
            peer_matches(conditions.pop_first().unwrap().1.1)
        });
        wait_for_policy_waiter(&mut holder, &name).await;
        holder.commit().await.unwrap();
        let result = task.await.unwrap();
        match second {
            Condition::Ended => assert_eq!(
                result,
                Ok(false),
                "the configured peer's Ended answer is a definitive denial"
            ),
            Condition::Incomplete => assert_eq!(
                result,
                Err(PolicyFailure::Unavailable),
                "an incomplete answer is operationally unavailable"
            ),
            Condition::Matching => unreachable!(),
        }
        assert_eq!(
            answers.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "each fresh first-lock admission reads the peer again"
        );
        let saved = sinex_store::signals::load(&mut writer, "fixture")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (
                saved.revision,
                serde_json::to_value(saved.checkpoint.host_policy.budget).unwrap()
            ),
            baseline
        );
        peer.abort();
        let _ = peer.await;
    }
}
