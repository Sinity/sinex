//! Commit order never skips: a transaction that takes commit positions holds
//! the counter until it ends, so a later one cannot commit a higher position
//! first and a commit-order reader never passes a position still to come.
use super::*;

#[devkit::test]
async fn a_second_transaction_waits_for_the_first_to_commit(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let mut first = PgConnection::connect(&url).await.unwrap();
    let mut second = PgConnection::connect(&url).await.unwrap();
    let mut observer = PgConnection::connect(&url).await.unwrap();

    let key = crate::privacy::hash_key(&mut first).await.unwrap();
    let mut early_cache = crate::privacy::Cache::new(key);
    let mut late_cache = crate::privacy::Cache::new(key);
    let mut held = first.begin().await.unwrap();
    let early = admit(
        &mut held,
        &mut early_cache,
        &batch(vec![keyed("s", 1, 1, 1)]),
        &NoDerivations,
        None,
    )
    .await
    .unwrap();
    let late = tokio::spawn(async move {
        let mut tx = second.begin().await.unwrap();
        let out = admit(
            &mut tx,
            &mut late_cache,
            &batch(vec![keyed("s", 2, 1, 1)]),
            &NoDerivations,
            None,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        out
    });

    // The second transaction waits on the counter row the first holds.
    let mut waited = false;
    for _ in 0..500 {
        let waiting: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity
             WHERE datname = current_database() AND wait_event_type = 'Lock')",
        )
        .fetch_one(&mut observer)
        .await
        .unwrap();
        if waiting {
            waited = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(waited, "the second transaction did not wait for the first");
    assert!(!late.is_finished());
    let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM core.commit_log")
        .fetch_one(&mut observer)
        .await
        .unwrap();
    assert_eq!(visible, 0, "nothing committed yet, so nothing to read");

    held.commit().await.unwrap();
    let late = late.await.unwrap();
    let (early_seq, late_seq) = (early.commit_seq.unwrap(), late.commit_seq.unwrap());
    assert!(late_seq.0 > early_seq.1, "{early_seq:?} then {late_seq:?}");
    let seqs: Vec<i64> =
        sqlx::query_scalar("SELECT commit_seq FROM core.commit_log ORDER BY commit_seq")
            .fetch_all(&mut observer)
            .await
            .unwrap();
    assert_eq!(
        seqs,
        (1..=2).collect::<Vec<i64>>(),
        "dense, in commit order"
    );
}
