use super::*;
use sqlx::{Connection, Executor};

#[devkit::test]
async fn preparation_is_scoped_and_unhardened_start_or_upgrade_is_refused(db: devkit::pg::TestDatabase) {
    let mut conn=PgConnection::connect(&db.url(None)).await.unwrap();
    require(&mut conn).await.unwrap();
    let owner:String=sqlx::query_scalar("SELECT session_user::text").fetch_one(&mut conn).await.unwrap();
    let writer:bool=sqlx::query_scalar("SELECT has_function_privilege('sinex_writer','pg_catalog.set_config(text,text,boolean)','EXECUTE')").fetch_one(&mut conn).await.unwrap();
    assert!(writer);
    conn.execute("GRANT EXECUTE ON FUNCTION pg_catalog.set_config(text,text,boolean) TO PUBLIC").await.unwrap();
    assert!(matches!(crate::require_current(&mut conn).await,Err(Error::UnsafeReadBoundary)));
    assert!(matches!(crate::apply(&mut conn).await,Err(Error::UnsafeReadBoundary)));
    crate::prepare(&mut conn,&owner).await.unwrap();
    crate::require_current(&mut conn).await.unwrap();
    sqlx::query("SELECT pg_catalog.set_config('sinex.grants','guarded',true)").execute(&mut conn).await.unwrap();
    let mut tx=conn.begin().await.unwrap();
    sqlx::query("SET LOCAL ROLE sinex_reader").execute(&mut *tx).await.unwrap();
    let error=sqlx::query("SELECT pg_catalog.set_config('role','none',true)").execute(&mut *tx).await.unwrap_err();
    assert_eq!(error.as_database_error().unwrap().code().as_deref(),Some("42501"));
    tx.rollback().await.unwrap();
}

#[devkit::test]
async fn applying_current_revokes_private_table_and_column_grants(db:devkit::pg::TestDatabase) {
    let mut conn=PgConnection::connect(&db.url(None)).await.unwrap();
    conn.execute("GRANT SELECT(hash_key) ON privacy.settings TO sinex_reader,PUBLIC").await.unwrap();
    conn.execute("GRANT SELECT ON privacy.data_keys,core.admission_stage,raw.bindings,derivation.comparisons TO sinex_reader").await.unwrap();
    crate::apply(&mut conn).await.unwrap();
    for (table,column) in [("privacy.settings","hash_key"),("privacy.data_keys","wrapped_key"),("core.admission_stage","payload"),("raw.bindings","settings"),("raw.reader_memory","memory"),("derivation.comparisons","examples"),("core.query_cursor_key","secret"),("core.owner_protocol_key","secret"),("privacy.original_events","baseline_class"),("privacy.event_outputs","payload"),("core.signal_checkpoints","state")] {
        let permitted:bool=sqlx::query_scalar("SELECT has_column_privilege('sinex_reader',$1,$2,'SELECT')").bind(table).bind(column).fetch_one(&mut conn).await.unwrap();
        assert!(!permitted,"private SQL column is readable: {table}.{column}");
    }
    let mut tx=conn.begin().await.unwrap();
    sqlx::query("SET LOCAL ROLE sinex_reader").execute(&mut *tx).await.unwrap();
    let _:i64=sqlx::query_scalar("SELECT count(*) FROM core.visible_events").fetch_one(&mut *tx).await.unwrap();
    let _:i64=sqlx::query_scalar("SELECT count(*) FROM event_types.command_imported").fetch_one(&mut *tx).await.unwrap();
    tx.rollback().await.unwrap();
}

#[devkit::test]
async fn inherited_private_select_refuses_startup_instead_of_widening_reader(db:devkit::pg::TestDatabase) {
    let mut conn=PgConnection::connect(&db.url(None)).await.unwrap();
    let role=format!("private_reader_{}",sinex_model::EventId::new().0.simple());
    conn.execute(format!("CREATE ROLE {role} NOLOGIN").as_str()).await.unwrap();
    conn.execute(format!("GRANT SELECT(hash_key) ON privacy.settings TO {role}").as_str()).await.unwrap();
    conn.execute(format!("GRANT {role} TO sinex_reader").as_str()).await.unwrap();
    assert!(matches!(crate::require_current(&mut conn).await,Err(Error::UnsafeReadBoundary)));
    assert!(matches!(crate::apply(&mut conn).await,Err(Error::UnsafeReadBoundary)));
    conn.execute(format!("REVOKE {role} FROM sinex_reader").as_str()).await.unwrap();
    conn.execute(format!("DROP OWNED BY {role}").as_str()).await.unwrap();
    conn.execute(format!("DROP ROLE {role}").as_str()).await.unwrap();
    crate::require_current(&mut conn).await.unwrap();
}
