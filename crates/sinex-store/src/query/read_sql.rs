//! Read-only SQL under the caller's grants: the statement runs in a read-only
//! transaction as the reader role, so it can read the stable views and tables
//! and change nothing. Rows come back as JSON objects with the statement's own
//! column names, in order.
//!
//! The rows come out of a database cursor a page at a time, so the memory a
//! statement costs is one page however many rows it selects, and every row
//! stays reachable: [`begin`] opens the transaction and the cursor, [`fetch`]
//! reads the next page, and [`end`] closes both. The connection holds the open
//! cursor between pages, so the caller keeps that connection until [`end`].
use crate::{Error, PAGE_BYTES, Result};
use serde_json::Value as Json;
use sqlx::PgConnection;

/// Rows per page when the caller names none. Pages are bounded by bytes too
/// ([`PAGE_BYTES`]), so a page of wide rows ends early.
pub const DEFAULT_ROWS: u64 = 1000;

/// One page of a statement's rows.
#[derive(Debug)]
pub struct Rows {
    pub rows: Vec<Json>,
    /// No rows remain; the cursor is spent.
    pub done: bool,
}

/// Open a read-only transaction on `conn` and a cursor over `statement`.
/// Nothing is read yet.
pub async fn begin(conn: &mut PgConnection, statement: &str) -> Result<()> {
    sinex_schema::require_read_boundary(conn).await?;
    let statement = statement.trim().trim_end_matches(';').trim();
    if statement.is_empty() {
        return Err(Error::Invalid("empty SQL statement".into()));
    }
    sqlx::query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *conn)
        .await?;
    let opened = async {
        sqlx::query("SET LOCAL ROLE sinex_reader")
            .execute(&mut *conn)
            .await?;
        sqlx::query("SET LOCAL TimeZone = 'UTC'")
            .execute(&mut *conn)
            .await?;
        // One statement may not occupy the database indefinitely: the server
        // cancels each read of the cursor at this bound even after the caller
        // has gone away.
        sqlx::query("SET LOCAL statement_timeout = '120s'")
            .execute(&mut *conn)
            .await?;
        // A cursor whose reader stops asking for pages is closed by the server
        // too, so an abandoned statement cannot hold its snapshot for ever.
        sqlx::query("SET LOCAL idle_in_transaction_session_timeout = '10min'")
            .execute(&mut *conn)
            .await?;
        // Wrapping keeps column order (json, not jsonb) and refuses anything
        // that is not a query. A second statement cannot hide inside the
        // wrapping subquery: it is a syntax error there, and a prepared
        // statement is one statement.
        sqlx::query(&format!(
            "DECLARE sinex_sql NO SCROLL CURSOR FOR SELECT row_to_json(sinex_sql_row)::text FROM ({statement}) sinex_sql_row"
        ))
        .execute(&mut *conn)
        .await?;
        Ok::<_, Error>(())
    }
    .await;
    if opened.is_err() {
        let _ = end(conn).await;
    }
    opened
}

/// The next page of the open cursor: up to `rows` rows, ending early once the
/// page holds [`PAGE_BYTES`] of row text (a page always holds at least one
/// row). The cursor stays open until a page reports `done`.
pub async fn fetch(conn: &mut PgConnection, rows: u64) -> Result<Rows> {
    let want = rows.max(1);
    let mut page = Vec::new();
    let mut bytes: i64 = 0;
    loop {
        let chunk = (want - page.len() as u64).min(500);
        let texts: Vec<String> = sqlx::query_scalar(&format!("FETCH {chunk} FROM sinex_sql"))
            .fetch_all(&mut *conn)
            .await?;
        let got = texts.len() as u64;
        for text in texts {
            bytes = bytes.saturating_add(text.len() as i64);
            page.push(serde_json::from_str(&text).map_err(|e| Error::Invalid(e.to_string()))?);
        }
        if got < chunk {
            return Ok(Rows {
                rows: page,
                done: true,
            });
        }
        if page.len() as u64 >= want || bytes >= PAGE_BYTES {
            return Ok(Rows {
                rows: page,
                done: false,
            });
        }
    }
}

/// Close the cursor and its transaction. The connection is reusable after.
pub async fn end(conn: &mut PgConnection) -> Result<()> {
    sqlx::query("ROLLBACK").execute(&mut *conn).await?;
    Ok(())
}

/// Every row of `statement`, read page by page. For tests: the daemon hands
/// pages to its caller instead of collecting them.
#[cfg(test)]
pub async fn run(conn: &mut PgConnection, statement: &str) -> Result<Vec<Json>> {
    begin(conn, statement).await?;
    let mut all = Vec::new();
    loop {
        match fetch(conn, DEFAULT_ROWS).await {
            Ok(page) => {
                all.extend(page.rows);
                if page.done {
                    end(conn).await?;
                    return Ok(all);
                }
            }
            Err(e) => {
                let _ = end(conn).await;
                return Err(e);
            }
        }
    }
}
