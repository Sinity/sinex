//! A step whose commit is refused: for a changed basis it is withdrawn, for
//! anything else it is taken again.
use super::*;

/// A daemon whose only committers are the deliverer and the requests a test
/// makes, with a quiet host policy.
async fn quiet_daemon(
    database: &str,
    root: &Path,
    nats: &devkit::nats::TestNats,
    allowed: &[&str],
) -> (daemon::Process, std::path::PathBuf, PgConnection) {
    let store = root.join("store");
    let socket = root.join("api.sock");
    std::fs::create_dir(root.join("tokens")).unwrap();
    std::fs::write(root.join("tokens/operator.token"), TOKEN).unwrap();
    daemon::sources(&root.join("sources.json"), allowed);
    let mut config: Value =
        serde_json::from_slice(&std::fs::read(root.join("sources.json")).unwrap()).unwrap();
    config["signals"] = json!({"interrupt_budget_per_hour":4,"quiet_hours":null,"notifications":null,"values":{},"actions":{},"outlet_timeout_ms":1000});
    std::fs::write(
        root.join("sources.json"),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();
    let mut process = start(database, &store, root, nats);
    ready(&mut process, &socket).await;
    let mut conn = PgConnection::connect(database).await.unwrap();
    wait_scalar(&mut conn, "SELECT count(*) FROM core.signal_checkpoints", 1).await;
    (process, socket, conn)
}

const WITHDRAWN: &str = "SELECT count(*) FROM core.events WHERE event_type='signal.outcome' AND payload->'outcome'->'result'->>'reason'='basis_changed'";

/// law: signal-basis-changed-withdraws
/// Replaying the mark a pending firing cites archives that reading and makes a
/// new one of the same record. The firing is withdrawn rather than delivered
/// citing the archived reading, and fires again citing the new one.
#[devkit::test]
async fn a_firing_whose_reading_was_replayed_is_withdrawn_then_cites_the_new_reading() {
    let db = devkit::pg::test_database().unwrap();
    let database = db.url(None);
    let nats = devkit::nats::test_nats().unwrap();
    let root = tempfile::tempdir().unwrap();
    let (mut process, socket, mut conn) = quiet_daemon(
        &database,
        root.path(),
        &nats,
        &["sinex.signals", "query.request"],
    )
    .await;
    let mut held = rule("held", "notice");
    held["for"] = json!(5000);
    ok(
        &socket,
        json!({"method":"signal","action":"add","name":"held","rule":held.to_string()}),
    );
    wait_scalar(&mut conn, ADOPTED, 1).await;
    ok(
        &socket,
        json!({"method":"mark","text":"synthetic replayed occurrence"}),
    );
    wait_scalar(
        &mut conn,
        "SELECT count(*) FROM core.signal_checkpoints WHERE state->'inputs' <> '{}'::jsonb",
        1,
    )
    .await;
    ok(
        &socket,
        json!({"method":"replay","event-type":["operator.mark"]}),
    );
    let replaced: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM audit.archived a JOIN core.events e ON e.id = a.event_id WHERE e.event_type = 'operator.mark' AND a.reason = 'replay'",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(replaced, 1, "the replay archives the cited reading");

    wait_scalar(&mut conn, WITHDRAWN, 1).await;
    wait_scalar(
        &mut conn,
        "SELECT count(*) FROM core.events WHERE event_type='signal.delivery'",
        1,
    )
    .await;
    let citing_archived: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM core.events d WHERE d.event_type='signal.delivery' AND EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = ANY(d.parents))",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(
        citing_archived, 0,
        "the delivery cites the replayed reading"
    );
    let citing_mark: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM core.events d WHERE d.event_type='signal.delivery' AND d.parents && ARRAY(SELECT id FROM core.events WHERE event_type='operator.mark')",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(citing_mark, 1);
    let withdrawn: i64 = sqlx::query_scalar(WITHDRAWN)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(withdrawn, 1, "withdrawn once, never retried");
    process.running();
    process.stop().await;
}

/// A step refused for a conflict that is not about its basis (here its
/// checkpoint changed under it) is taken again from the committed state and
/// delivers once.
#[devkit::test]
async fn a_step_refused_for_a_changed_checkpoint_is_taken_again() {
    let db = devkit::pg::test_database().unwrap();
    let database = db.url(None);
    let nats = devkit::nats::test_nats().unwrap();
    let root = tempfile::tempdir().unwrap();
    let (mut process, socket, mut conn) =
        quiet_daemon(&database, root.path(), &nats, &["sinex.signals"]).await;
    ok(
        &socket,
        json!({"method":"signal","action":"add","name":"plain","rule":rule("plain","notice").to_string()}),
    );
    wait_scalar(&mut conn, ADOPTED, 1).await;
    // The next checkpoint save finds no row to update, as when another step
    // committed first. A sequence counts saves outside the refused
    // transaction, so exactly one is refused.
    for statement in [
        "CREATE SEQUENCE test_checkpoint_saves",
        "CREATE FUNCTION test_refuse_one_save() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER AS $$ BEGIN IF nextval('test_checkpoint_saves') = 1 THEN RETURN NULL; END IF; RETURN NEW; END $$",
        "CREATE TRIGGER test_refuse_one_save BEFORE UPDATE ON core.signal_checkpoints FOR EACH ROW EXECUTE FUNCTION test_refuse_one_save()",
    ] {
        sqlx::query(statement).execute(&mut conn).await.unwrap();
    }
    ok(
        &socket,
        json!({"method":"mark","text":"synthetic contested occurrence"}),
    );
    wait_scalar(
        &mut conn,
        "SELECT count(*) FROM core.events WHERE event_type='signal.delivery'",
        1,
    )
    .await;
    let saves: i64 = sqlx::query_scalar("SELECT last_value FROM test_checkpoint_saves")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert!(saves >= 2, "one save was refused and a later one committed");
    let delivered: i64 =
        sqlx::query_scalar("SELECT count(*) FROM core.events WHERE event_type='signal.delivery'")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(delivered, 1, "the retried step delivers once");
    let withdrawn: i64 = sqlx::query_scalar(WITHDRAWN)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(withdrawn, 0, "a conflict is not a changed basis");
    process.running();
    process.stop().await;
}
