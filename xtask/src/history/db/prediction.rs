use super::*;

impl HistoryDb {
    /// R3: Compute the probability that `to_command` follows `from_command` within
    /// `window_mins` minutes, based on the `limit` most recent `from_command` successes.
    ///
    /// Returns a value 0.0–100.0 (percentage). Used for predictive compilation prefetch:
    /// if check→test transition is >70% likely, pre-compile tests while the developer
    /// reviews check output.
    ///
    /// Returns 0.0 when there is insufficient history.
    pub fn get_transition_probability(
        &self,
        from_command: &str,
        to_command: &str,
        window_mins: u32,
        limit: u32,
    ) -> Result<f64> {
        let window_str = format!("+{} seconds", window_mins * 60);

        // CTE: recent `from_command` successes, then count how many were followed by `to_command`
        let (total, followed): (i64, i64) = self
            .conn
            .query_row(
                r"
                WITH recent_from AS (
                    SELECT id, finished_at
                    FROM invocations
                    WHERE command = ?1
                      AND status = 'success'
                      AND finished_at IS NOT NULL
                    ORDER BY id DESC
                    LIMIT ?2
                )
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN EXISTS (
                        SELECT 1 FROM invocations next
                        WHERE next.command = ?3
                          AND next.id > rf.id
                          AND next.started_at > rf.finished_at
                          AND next.started_at <= datetime(rf.finished_at, ?4)
                    ) THEN 1 ELSE 0 END) AS followed
                FROM recent_from rf
                ",
                rusqlite::params![from_command, limit, to_command, window_str],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, Option<i64>>(1)?.unwrap_or(0),
                    ))
                },
            )
            .wrap_err_with(|| {
                format!(
                    "failed to compute transition probability from '{from_command}' to '{to_command}'"
                )
            })?;

        if total == 0 {
            return Ok(0.0);
        }

        Ok((followed as f64 / total as f64) * 100.0)
    }
}
