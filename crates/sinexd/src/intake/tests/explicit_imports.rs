//! Explicit imports through backfill, intake, admission and stored-byte replay.
use serde_json::Value;

#[devkit::test]
async fn explicit_imports_replay_without_source_files_and_keep_guarded_records(
    db: devkit::pg::TestDatabase,
) {
    let dir = tempfile::tempdir().unwrap();
    let journal = dir.path().join("example.journal");
    let history = dir.path().join("example.history");
    let transaction =
        "2030-01-01 Example\n  Assets:Example\n  Expenses:Example  1.00000000000000000001 TEST\n\n";
    std::fs::write(
        &journal,
        format!("{transaction}{transaction}include other.journal\n"),
    )
    .unwrap();
    std::fs::write(&history, "pwd\npwd\n#123\npartial").unwrap();
    let store = dir.path().join("store");
    let options = || crate::backfill::Options {
        database: db.url(None),
        store: store.clone(),
        reads: vec![
            format!("hledger={}", journal.display()),
            format!("text-history={}", history.display()),
        ],
        zone: Some("Europe/Warsaw".into()),
        options: vec![],
        host: "example-host".into(),
        json: true,
        identity_file: None,
    };
    crate::backfill::run(options()).await.unwrap();
    crate::backfill::run(options()).await.unwrap();
    std::fs::remove_file(&journal).unwrap();
    std::fs::remove_file(&history).unwrap();
    let mut ctx = crate::reader_context::ReaderContext::open(&db.url(None), &store)
        .await
        .unwrap();
    async fn live(conn: &mut sqlx::PgConnection) -> Vec<(String, String, Value)> {
        sqlx::query_as("SELECT e.event_type, e.class::text, e.payload FROM (SELECT event_id FROM core.live_locations UNION ALL SELECT event_id FROM core.live_equivalence) l JOIN core.events e ON e.id=l.event_id ORDER BY e.event_type,e.payload::text")
            .fetch_all(conn).await.unwrap()
    }
    let before = live(&mut ctx.read_conn).await;
    assert_eq!(before.len(), 6);
    assert!(before.iter().all(|(_, class, _)| class == "guarded"));
    let transactions = before
        .iter()
        .filter(|(kind, _, _)| kind == "transaction.posted")
        .collect::<Vec<_>>();
    assert_eq!(transactions.len(), 2);
    assert_eq!(
        transactions[0].2["postings"][1]["amount"],
        "1.00000000000000000001"
    );
    let report = crate::replay::run(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        crate::replay::Options {
            scope: crate::replay::Scope {
                event_types: Some(vec![
                    "transaction.posted".into(),
                    "journal.directive".into(),
                    "command.imported".into(),
                ]),
                ..Default::default()
            },
            compare: false,
            resume: None,
            actor: "example-test".into(),
            unlock: None,
        },
    )
    .await
    .unwrap();
    assert!(report.events_read >= 5);
    assert_eq!(live(&mut ctx.read_conn).await, before);
    let unknown: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events WHERE event_type IN ('transaction.posted','command.imported') AND ts_orig IS NOT NULL")
        .fetch_one(&mut ctx.read_conn).await.unwrap();
    assert_eq!(unknown, 0);
    ctx.close().await.unwrap();
}

#[devkit::test]
async fn included_journal_changes_are_read_when_the_parent_is_unchanged_and_replay_from_stored_bytes(
    db: devkit::pg::TestDatabase,
) {
    let dir = tempfile::tempdir().unwrap();
    let parent = dir.path().join("main.journal");
    let child = dir.path().join("child.journal");
    std::fs::write(
        &parent,
        "Y2032\ninclude child.journal\ninclude child.journal\n",
    )
    .unwrap();
    std::fs::write(
        &child,
        "02/29 Child\n  (Assets:Example)  1.234,000 TEST @ 2 OTHER == 3 TEST\n",
    )
    .unwrap();
    let store = dir.path().join("store");
    let options = || crate::backfill::Options {
        database: db.url(None),
        store: store.clone(),
        reads: vec![format!("hledger={}", parent.display())],
        zone: Some("Europe/Warsaw".into()),
        options: vec![],
        host: "example-host".into(),
        json: true,
        identity_file: None,
    };
    crate::backfill::run(options()).await.unwrap();
    let mut ctx = crate::reader_context::ReaderContext::open(&db.url(None), &store)
        .await
        .unwrap();
    let transactions: Vec<Value> = sqlx::query_scalar("SELECT payload FROM core.events WHERE event_type='transaction.posted' ORDER BY payload::text").fetch_all(&mut ctx.read_conn).await.unwrap();
    assert_eq!(transactions.len(), 2);
    assert!(
        transactions
            .iter()
            .all(|p| p["resolved_date"] == "2032-02-29")
    );
    assert_ne!(
        transactions[0]["included_from"],
        transactions[1]["included_from"]
    );
    ctx.close().await.unwrap();
    std::fs::write(
        &child,
        "03/01 Child changed\n  [Assets:Example]  = 9 TEST\n",
    )
    .unwrap();
    crate::backfill::run(options()).await.unwrap();
    std::fs::write(&parent, "Y2036\ninclude child.journal\ninclude child.journal\n").unwrap();
    crate::backfill::run(options()).await.unwrap();
    std::fs::remove_file(&child).unwrap();
    std::fs::remove_file(&parent).unwrap();
    let mut ctx = crate::reader_context::ReaderContext::open(&db.url(None), &store)
        .await
        .unwrap();
    let changed: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events WHERE event_type='transaction.posted' AND payload->>'resolved_date'='2032-03-01'").fetch_one(&mut ctx.read_conn).await.unwrap();
    assert_eq!(changed, 2);
    let inherited: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events WHERE event_type='transaction.posted' AND payload->>'resolved_date'='2036-03-01'")
        .fetch_one(&mut ctx.read_conn).await.unwrap();
    assert_eq!(inherited, 2);
    let report = crate::replay::run(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        crate::replay::Options {
            scope: crate::replay::Scope {
                event_types: Some(vec![
                    "transaction.posted".into(),
                    "journal.directive".into(),
                ]),
                ..Default::default()
            },
            compare: false,
            resume: None,
            actor: "example-test".into(),
            unlock: None,
        },
    )
    .await
    .unwrap();
    assert!(report.events_read >= 7);
    let changed: i64 = sqlx::query_scalar("SELECT count(*) FROM (SELECT event_id FROM core.live_equivalence) l JOIN core.events e ON e.id=l.event_id WHERE e.event_type='transaction.posted' AND e.payload->>'resolved_date'='2036-03-01'").fetch_one(&mut ctx.read_conn).await.unwrap();
    assert_eq!(changed, 2);
    ctx.close().await.unwrap();
}
