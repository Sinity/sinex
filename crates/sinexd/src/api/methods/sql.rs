//! Read-only SQL, a page at a time. The first call opens a database cursor
//! over the statement and answers its first page; while rows remain, the
//! connection holding the cursor is kept under a random continuation token
//! that the next call presents. The daemon's memory for a statement is one
//! page, however many rows it selects, and every row stays reachable.
use super::*;
use crate::{Error, Result};
use sinex_store::query::read_sql;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A cursor nobody has read for this long is closed; its reader reruns the
/// statement. The server closes it too after ten idle minutes.
const IDLE: Duration = Duration::from_secs(5 * 60);

/// Open statements awaiting their next page, by continuation token.
pub(crate) type SqlCursors = Arc<tokio::sync::Mutex<HashMap<String, Held>>>;

/// The connection holding an open cursor, and who may read it.
pub(crate) struct Held {
    conn: crate::request_deadline::ReadConnection,
    statement: String,
    caller: (&'static str, String),
    used: Instant,
}

impl Api {
    pub(super) async fn sql(
        &self,
        caller: &Caller,
        statement: String,
        page: Option<u64>,
        cursor: Option<String>,
    ) -> Result<Reply> {
        let rows = page.unwrap_or(read_sql::DEFAULT_ROWS);
        let mut held = match cursor {
            Some(token) => self.take_cursor(caller, &statement, &token).await?,
            None => {
                // The cursor lives in the connection's transaction, so a
                // statement that spans pages keeps its own connection, outside
                // the pool, until its last page.
                let mut conn = self.acquire(caller).await?.detach();
                read_sql::begin(&mut conn, &statement).await?;
                Held {
                    conn,
                    statement,
                    caller: (caller.actor, caller.grants.clone()),
                    used: Instant::now(),
                }
            }
        };
        held.conn.prepare().await?;
        sinex_store::query::local_request_deadline(&mut held.conn).await?;
        let page = match read_sql::fetch(&mut held.conn, rows).await {
            Ok(page) => page,
            Err(e) => {
                let _ = read_sql::end(&mut held.conn).await;
                return Err(e.into());
            }
        };
        let count = page.rows.len();
        let next = if page.done {
            let _ = read_sql::end(&mut held.conn).await;
            None
        } else {
            let token = token();
            held.used = Instant::now();
            self.sql_cursors.lock().await.insert(token.clone(), held);
            Some(token)
        };
        Ok(data(json!({
            "rows": page.rows,
            "count": count,
            "page": { "size": rows, "cursor": next },
        })))
    }

    /// The held cursor for `token`, if it is still open and belongs to this
    /// caller and statement. Cursors left idle too long are closed on the way.
    async fn take_cursor(&self, caller: &Caller, statement: &str, token: &str) -> Result<Held> {
        let mut open = self.sql_cursors.lock().await;
        open.retain(|_, held| held.used.elapsed() < IDLE);
        let Some(held) = open.remove(token) else {
            return Err(Error::new(
                command_surface::FailureKind::StaleCursor,
                "this SQL cursor is closed (read to the end, idle for five minutes, or already in use); run the statement again",
            ));
        };
        if held.caller != (caller.actor, caller.grants.clone()) || held.statement != statement {
            open.insert(token.to_owned(), held);
            return Err(Error::refused(
                "this SQL cursor belongs to another statement or caller",
            ));
        }
        Ok(held)
    }
}

/// An unguessable continuation token.
fn token() -> String {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).expect("the system random source is available");
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
