use sea_query::{Alias, Expr, PostgresQueryBuilder, Query};
use sinex_core::db::seaquery_helpers::SeaQueryUlidExt;
use sinex_core::Ulid;
use sinex_test_utils::sinex_test;

#[sinex_test]
fn seaquery_eq_ulid_generates_expression() -> color_eyre::eyre::Result<()> {
    let ulid = Ulid::new();
    let query = Query::select()
        .from(Alias::new("events"))
        .and_where(Expr::col(Alias::new("event_id")).eq_ulid(ulid))
        .to_owned();

    let (sql, _) = query.build(PostgresQueryBuilder);
    assert!(sql.contains("WHERE"));
    Ok(())
}

#[sinex_test]
fn seaquery_in_ulids_converts_collection() -> color_eyre::eyre::Result<()> {
    let ulids = vec![Ulid::new(), Ulid::new(), Ulid::new()];
    let query = Query::select()
        .from(Alias::new("events"))
        .and_where(Expr::col(Alias::new("event_id")).in_ulids(ulids))
        .to_owned();

    let (sql, _) = query.build(PostgresQueryBuilder);
    assert!(sql.contains("IN"));
    Ok(())
}
