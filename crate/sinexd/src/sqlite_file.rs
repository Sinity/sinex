//! Checks that an open SQLite connection still reads the configured file.
//! The answer comes from SQLite's own file handle, not from a path stat.
use sqlx::{Row, sqlite::SqliteConnection};
use std::{
    ffi::{c_char, c_int, c_void},
    path::{Path, PathBuf},
};

// Linked by SQLx's SQLite driver. The handle stays locked against SQLx's worker
// for the whole call and never escapes.
unsafe extern "C" {
    fn sqlite3_file_control(
        db: *mut c_void,
        name: *const c_char,
        operation: c_int,
        argument: *mut c_void,
    ) -> c_int;
}

async fn moved(connection: &mut SqliteConnection) -> Result<(), String> {
    let mut handle = connection
        .lock_handle()
        .await
        .map_err(|_| "SQLite handle unavailable")?;
    let mut moved: c_int = -1;
    // SAFETY: SQLx owns and locks a live sqlite3 connection. SQLite's documented
    // HAS_MOVED opcode (20) writes an int, and the static main name is NUL ended.
    let result = unsafe {
        sqlite3_file_control(
            handle.as_raw_handle().as_ptr().cast(),
            c"main".as_ptr(),
            20,
            (&mut moved as *mut c_int).cast(),
        )
    };
    if result != 0 || moved != 0 {
        return Err(format!(
            "SQLite file was moved or replaced (SQLite result {result}, moved {moved}); reopen and full read required"
        ));
    }
    Ok(())
}

pub(crate) async fn verify(
    connection: &mut SqliteConnection,
    configured: &Path,
) -> Result<PathBuf, String> {
    moved(connection).await?;
    let databases = sqlx::query("PRAGMA database_list")
        .fetch_all(&mut *connection)
        .await
        .map_err(|_| "SQLite opened filename unavailable")?;
    let opened = databases
        .iter()
        .find(|r| r.try_get::<String, _>("name").ok().as_deref() == Some("main"))
        .and_then(|r| r.try_get::<String, _>("file").ok())
        .filter(|s| !s.is_empty())
        .ok_or("SQLite main filename unavailable")?;
    let configured = std::fs::canonicalize(configured)
        .map_err(|_| "configured SQLite path no longer resolves; reopen and full read required")?;
    if Path::new(&opened) != configured {
        return Err(
            "configured SQLite path differs from the opened file; reopen and full read required"
                .into(),
        );
    }
    moved(connection).await?;
    Ok(configured)
}

#[cfg(test)]
#[path = "sqlite_file_tests.rs"]
mod tests;
