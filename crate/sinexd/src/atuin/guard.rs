//! The default SQLite VFS verifies the actual open file; path stat is diagnostic only.
use sqlx::{Row, sqlite::SqliteConnection};
use std::{
    ffi::{c_char, c_int, c_void},
    path::{Path, PathBuf},
};

// Public SQLite C ABI already linked by SQLx's SQLite driver. The opaque handle
// remains locked against SQLx's worker for the entire call; no handle escapes.
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
            "Atuin open-file correspondence unavailable (SQLite result {result}, moved {moved}); reopen and full read required"
        ));
    }
    Ok(())
}

pub(super) async fn verify(
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
        .map_err(|_| "configured Atuin path no longer resolves; reopen and full read required")?;
    if Path::new(&opened) != configured {
        return Err("configured Atuin path differs from SQLite opened filename; reopen and full read required".into());
    }
    moved(connection).await?;
    Ok(configured)
}

#[cfg(test)]
#[path = "guard_tests.rs"]
mod tests;
