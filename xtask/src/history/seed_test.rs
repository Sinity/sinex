//! Regression coverage for sinex-2lsq: seed_history's secondary inserts
//! (`invocation_packages`, `build_diagnostics`) use `let _ = db.conn.execute(...)`,
//! silently discarding failures instead of propagating them.

use super::*;
use tempfile::tempdir;
use xtask::sandbox::{TestResult, sinex_test};

#[sinex_test]
#[ignore = "sinex-2lsq open: seed_history swallows invocation_packages insert failures \
            instead of returning Err, so a schema/permission problem produces an \
            invocation history with silently-missing package rows and no error signal"]
async fn seed_history_does_not_silently_drop_invocation_packages_on_insert_failure()
-> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("seed-swallow.db");
    let db = HistoryDb::open(&db_path)?;

    // Break the secondary-insert target after schema creation but before
    // seeding, so every `invocation_packages` INSERT in seed_history fails.
    db.conn.execute_batch("DROP TABLE invocation_packages;")?;

    let options = SeedOptions {
        days: 7,
        invocations: 20,
    };

    // Today: this returns Ok(()) even though every invocation_packages
    // insert underneath it failed (`let _ = db.conn.execute(...)`).
    let result = seed_history(&db, &options);
    assert!(
        result.is_err(),
        "seed_history returned Ok(()) despite every invocation_packages insert failing \
         (table was dropped) — the secondary-insert failure was silently swallowed"
    );

    Ok(())
}
