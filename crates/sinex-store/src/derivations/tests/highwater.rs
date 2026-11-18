use super::*;
use sqlx::Connection;

#[devkit::test]
async fn stored_reading_orders_keep_each_producer_separate(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    sqlx::query("INSERT INTO derivation.positions(derivation,commit_seq,reading_order) VALUES('synthetic.first',100,4398046511104),('synthetic.second',200,71),('synthetic.unrelated',300,9223372036854775790)").execute(&mut conn).await.unwrap();
    let names = [
        "synthetic.first",
        "synthetic.second",
        "synthetic.empty",
        "synthetic.first",
    ]
    .map(str::to_owned);
    let expected = BTreeMap::from([
        ("synthetic.first".into(), 1_i64 << 42),
        ("synthetic.second".into(), 71),
        ("synthetic.empty".into(), 0),
    ]);
    assert_eq!(reading_orders(&mut conn, &names).await.unwrap(), expected);
    assert!(reading_orders(&mut conn, &[]).await.unwrap().is_empty());
    let mut transaction = conn.begin().await.unwrap();
    reset_position(&mut transaction, "synthetic.first")
        .await
        .unwrap();
    assert_eq!(
        position(&mut transaction, "synthetic.first").await.unwrap(),
        0
    );
    assert_eq!(
        reading_orders(&mut transaction, &names).await.unwrap(),
        expected
    );
    transaction.rollback().await.unwrap();
    assert_eq!(position(&mut conn, "synthetic.first").await.unwrap(), 100);
    assert_eq!(reading_orders(&mut conn, &names).await.unwrap(), expected);
}
