//! Regression coverage for sinex-3k4i: `ensure_proof_schema` treats "both
//! tables exist" as "schema is correct" — it never checks columns, so a
//! stale/partial `proof_evidence` or `test_proof_units` table (e.g. from an
//! older schema version) is silently accepted as fine.

use super::*;
use tempfile::tempdir;
use xtask::sandbox::{TestResult, sinex_test};

#[sinex_test]
#[ignore = "sinex-3k4i open: ensure_proof_schema only checks table_exists(), not \
            column-level shape, so a stale proof_evidence table missing newer columns \
            is silently treated as already-migrated"]
async fn ensure_proof_schema_detects_stale_column_shape() -> TestResult<()> {
    let dir = tempdir()?;
    let db = HistoryDb::open(&dir.path().join("schema-shallow.db"))?;

    // Simulate an old schema version: both tables exist, but proof_evidence
    // is missing every column ensure_proof_schema's own CREATE TABLE
    // declares (e.g. scope_json, artifact_json).
    db.conn.execute_batch(
        "CREATE TABLE proof_evidence (id INTEGER PRIMARY KEY);
         CREATE TABLE test_proof_units (id INTEGER PRIMARY KEY);",
    )?;

    db.ensure_proof_schema()?;

    let has_scope_json = db.column_exists("proof_evidence", "scope_json")?;

    assert!(
        has_scope_json,
        "ensure_proof_schema left a stale proof_evidence table (missing scope_json) in \
         place because both tables already existed by name -- it never checked columns"
    );

    Ok(())
}
