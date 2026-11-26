use super::*;
use sqlx::Executor;

/// The registry while a selection also named the build that made it.
pub(super) async fn derivations_with_selected_build(conn: &mut PgConnection) {
    super::output_ranges::outputs_before_ranges(&mut *conn).await;
    conn.execute("ALTER TABLE registry.derivations ADD COLUMN build text NOT NULL DEFAULT ''")
        .await
        .unwrap();
}

/// The step keeps every registered derivation and its selected version; only
/// the build goes.
#[devkit::test]
async fn selections_keep_their_versions_without_the_build(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let fresh = catalog(&mut conn).await.unwrap();
    derivations_with_selected_build(&mut conn).await;
    conn.execute(
        "INSERT INTO registry.derivations(name,version,kind,inputs,outputs,build) VALUES
           ('synthetic.keyed',6,'keyed','{synthetic.input}','{synthetic.output}','an-earlier-build'),
           ('synthetic.per-event',2,'per_event','{synthetic.input}','{synthetic.text}','');
         DELETE FROM sinex_schema.applied;
         INSERT INTO sinex_schema.applied(version) VALUES(50)",
    )
    .await
    .unwrap();
    assert!(matches!(
        apply(&mut conn).await.unwrap(),
        Applied::Upgraded {
            from: 50,
            to: VERSION
        }
    ));
    assert_eq!(catalog(&mut conn).await.unwrap(), fresh);
    let rows: Vec<(String, i64, String, Vec<String>, Vec<String>)> = sqlx::query_as(
        "SELECT name,version,kind,inputs,outputs FROM registry.derivations ORDER BY name",
    )
    .fetch_all(&mut conn)
    .await
    .unwrap();
    assert_eq!(
        rows,
        [
            (
                "synthetic.keyed".to_owned(),
                6,
                "keyed".to_owned(),
                vec!["synthetic.input".to_owned()],
                vec!["synthetic.output".to_owned()]
            ),
            (
                "synthetic.per-event".to_owned(),
                2,
                "per_event".to_owned(),
                vec!["synthetic.input".to_owned()],
                vec!["synthetic.text".to_owned()]
            ),
        ]
    );
}
