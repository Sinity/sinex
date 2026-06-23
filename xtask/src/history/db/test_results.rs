use super::*;

impl HistoryDb {
    /// Record a test result.
    ///
    /// `test_mode` distinguishes execution lanes: `"nextest"`, `"vm"`, `"bench"`, `"fuzz"`.
    pub fn record_test_result(
        &self,
        invocation_id: i64,
        test_name: &str,
        package: &str,
        status: &str,
        duration_secs: f64,
        output: Option<&str>,
        test_mode: &str,
    ) -> Result<()> {
        self.conn.execute(
            r"
            INSERT INTO test_results (invocation_id, test_name, package, status, duration_secs, output, test_mode)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ",
            params![invocation_id, test_name, package, status, duration_secs, output, test_mode],
        )?;
        Ok(())
    }

    /// Attach NATS consumer snapshot context to a test result record.
    ///
    /// D8: stores serialized `ConsumerSnapshot` JSON on failing tests so that
    /// `xtask history tests failures --output` can surface NATS consumer state
    /// at the time of failure, helping debug pipeline test failures caused by
    /// consumer lag or delivery ordering issues.
    ///
    /// Matches by `test_name` within `invocation_id`. No-op if the test isn't found.
    pub fn record_test_nats_context(
        &self,
        invocation_id: i64,
        test_name: &str,
        context: &serde_json::Value,
    ) -> Result<()> {
        let json = serde_json::to_string(context).context("failed to serialize NATS context")?;
        self.conn.execute(
            r"UPDATE test_results SET nats_context = ?1
              WHERE invocation_id = ?2 AND test_name = ?3",
            params![json, invocation_id, test_name],
        )?;
        Ok(())
    }

    /// Back-fill test outputs from JUnit XML for an invocation.
    ///
    /// Updates `test_results.output` for tests that currently have NULL output.
    /// This is used after parsing JUnit XML to populate passing test output,
    /// since `libtest-json-plus` only includes stdout for failed tests.
    pub fn backfill_test_outputs(
        &self,
        invocation_id: i64,
        outputs: &std::collections::HashMap<String, String>,
    ) -> Result<usize> {
        let mut updated = 0usize;
        let mut stmt = self.conn.prepare(
            r"
            UPDATE test_results
            SET output = ?1
            WHERE invocation_id = ?2 AND test_name LIKE ?3 AND output IS NULL
            ",
        )?;

        for (test_name, output) in outputs {
            // The JUnit `name` attribute is the test function path (e.g.,
            // "repositories::events::tests::test_basic") which matches the
            // libtest-json `name` field stored in test_results.test_name.
            // Use suffix match with LIKE to handle potential differences.
            let pattern = format!("%{test_name}");
            let rows = stmt.execute(params![output, invocation_id, pattern])?;
            updated += rows;
        }

        Ok(updated)
    }

    /// Back-fill test metadata from JUnit XML for an invocation.
    ///
    /// Enriches `test_results` with:
    /// - Output (for tests with NULL output — passing tests from libtest-json-plus)
    /// - Failure message/type from JUnit `<failure>` elements
    /// - Sandbox infrastructure metadata (slot name, timing) parsed from slog events
    /// - Package correction from JUnit `classname` attribute
    pub fn backfill_test_metadata(
        &self,
        invocation_id: i64,
        metadata: &std::collections::HashMap<String, crate::nextest::junit::JunitTestMeta>,
    ) -> Result<usize> {
        let mut updated_tests = 0usize;

        // Back-fill output for tests that do not have it yet.
        let mut output_stmt = self.conn.prepare(
            r"
            UPDATE test_results
            SET output = ?1
            WHERE invocation_id = ?2 AND test_name LIKE ?3 AND output IS NULL
            ",
        )?;

        // Update failure info and package from classname.
        let mut meta_stmt = self.conn.prepare(
            r"
            UPDATE test_results
            SET failure_message = COALESCE(?1, failure_message),
                failure_type = COALESCE(?2, failure_type),
                package = COALESCE(?3, package)
            WHERE invocation_id = ?4 AND test_name LIKE ?5
            ",
        )?;

        for (test_name, meta) in metadata {
            let pattern = format!("%{test_name}");
            let mut touched = false;

            // Back-fill output if available and not already present
            if let Some(output) = &meta.output {
                let rows = output_stmt.execute(params![output, invocation_id, &pattern])?;
                touched |= rows > 0;
            }

            // Update failure info and classname-based package
            let has_meta = meta.failure_message.is_some()
                || meta.failure_type.is_some()
                || meta.classname.is_some();
            if has_meta {
                let normalized_package = meta
                    .classname
                    .as_deref()
                    .and_then(normalize_junit_classname_package);
                meta_stmt.execute(params![
                    meta.failure_message,
                    meta.failure_type,
                    normalized_package,
                    invocation_id,
                    &pattern,
                ])?;
                touched = true;
            }

            if touched {
                updated_tests += 1;
            }
        }

        drop(output_stmt);
        drop(meta_stmt);

        // Parse slog events from output to extract sandbox metadata.
        self.extract_sandbox_metadata(invocation_id)?;

        Ok(updated_tests)
    }

    /// Extract sandbox infrastructure metadata from slog events in test output.
    ///
    /// Scans the `output` column for `[sandbox:*] event=slot_acquired` lines and
    /// extracts `slot`, `duration_ms`, `clean_ms` into dedicated columns.
    fn extract_sandbox_metadata(&self, invocation_id: i64) -> Result<()> {
        // Fetch all tests with output for this invocation
        let mut fetch_stmt = self.conn.prepare(
            r"
            SELECT id, output FROM test_results
            WHERE invocation_id = ?1 AND output IS NOT NULL AND slot_name IS NULL
            ",
        )?;

        let mut update_stmt = self.conn.prepare(
            r"
            UPDATE test_results
            SET slot_name = ?1, slot_wait_ms = ?2, cleanup_ms = ?3
            WHERE id = ?4
            ",
        )?;

        let rows: Vec<(i64, String)> = fetch_stmt
            .query_map([invocation_id], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .wrap_err_with(|| {
                format!(
                    "failed to read stored sandbox metadata rows for invocation {invocation_id}"
                )
            })?;

        for (id, output) in &rows {
            let meta = parse_sandbox_meta(output).wrap_err_with(|| {
                format!("failed to parse sandbox metadata for stored test result row {id}")
            })?;
            if meta.slot_name.is_some() || meta.slot_wait_ms.is_some() {
                update_stmt.execute(params![
                    meta.slot_name,
                    meta.slot_wait_ms,
                    meta.cleanup_ms,
                    id,
                ])?;
            }
        }

        Ok(())
    }

    /// Get count of invocations.
    pub fn count(&self) -> Result<usize> {
        let count: usize = self
            .conn
            .query_row("SELECT COUNT(*) FROM invocations", [], |row| row.get(0))?;
        Ok(count)
    }

    // ──────────────────────────────────────────────────────────────────────
    // G3: Fix Session Analytics
    // ──────────────────────────────────────────────────────────────────────
}
