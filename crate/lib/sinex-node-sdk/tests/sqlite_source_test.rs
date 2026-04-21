use camino::Utf8PathBuf;
use rusqlite::Connection;
use sinex_node_sdk::read_rows_after;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn sqlite_row_reader_rejects_malformed_rows_without_advancing() -> TestResult<()> {
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("history.sqlite");
    let conn = Connection::open(&db_path)?;
    conn.execute(
        "CREATE TABLE history (
            command TEXT NOT NULL,
            \"when\" INTEGER
        )",
        [],
    )?;
    conn.execute(
        "INSERT INTO history (command, \"when\") VALUES (?1, ?2)",
        rusqlite::params!["echo ok", 1_234_567_890_i64],
    )?;
    conn.execute(
        "INSERT INTO history (command, \"when\") VALUES (?1, ?2)",
        rusqlite::params!["echo broken", "not-a-timestamp"],
    )?;

    let path = Utf8PathBuf::from_path_buf(db_path)
        .map_err(|_| color_eyre::eyre::eyre!("temporary sqlite path should be valid UTF-8"))?;

    let error = read_rows_after(
        &path,
        "SELECT ROWID, command, \"when\" FROM history WHERE ROWID > ? ORDER BY ROWID ASC",
        0,
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<i64>>(2)?,
            ))
        },
    )
    .expect_err("malformed sqlite row should fail the read");

    assert!(
        error.to_string().contains("failed to map SQLite row 2"),
        "unexpected error: {error}"
    );

    Ok(())
}
