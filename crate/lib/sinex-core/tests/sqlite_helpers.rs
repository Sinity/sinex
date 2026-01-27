use camino::Utf8Path;
use rusqlite::Connection;
use sinex_core::types::utils::sqlite_helpers::{
    QueryResultExt, SqliteConnection, SqliteQueryBuilder, SqliteStatementExt,
};
use xtask::sandbox::sinex_test;
use tempfile::NamedTempFile;

#[sinex_test]
fn sqlite_connection_helpers_cover_read_modes() -> TestResult<()> {
    let temp_file = NamedTempFile::new().unwrap();
    let path = Utf8Path::from_path(temp_file.path()).unwrap();

    let conn = SqliteConnection::open_readwrite(path, "test_operation").unwrap();
    conn.execute("CREATE TABLE test (id INTEGER)", []).unwrap();
    drop(conn);

    let conn = SqliteConnection::open_readonly(path, "test_operation").unwrap();
    let count: i32 = conn
        .query_row("SELECT COUNT(*) FROM test", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 0);
    Ok(())
}

#[sinex_test]
fn statement_and_query_helpers_attach_context() -> TestResult<()> {
    let temp_file = NamedTempFile::new().unwrap();
    let conn = Connection::open(temp_file.path()).unwrap();
    conn.execute("CREATE TABLE test (id INTEGER)", []).unwrap();

    let stmt = conn
        .prepare_with_context("SELECT * FROM test", "test_query")
        .unwrap();
    drop(stmt);

    let err = conn
        .prepare_with_context("SELECT * FROM nonexistent", "test_query")
        .unwrap_err();
    assert!(err.to_string().contains("test_query"));

    let failing_result: std::result::Result<usize, _> =
        Err(rusqlite::Error::ExecuteReturnedResults);

    let err = failing_result
        .with_context(
            SqliteQueryBuilder::new("insert_record")
                .query_type("insert")
                .context("table", "test"),
        )
        .unwrap_err();
    assert!(err.to_string().contains("insert_record"));
    assert!(err.to_string().contains("table: test"));
    Ok(())
}
