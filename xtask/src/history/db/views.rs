use super::*;

impl HistoryDb {
    /// Get all package names that have appeared in diagnostics (G4).
    pub fn get_known_packages(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT DISTINCT package
            FROM build_diagnostics
            WHERE package IS NOT NULL
            ORDER BY package
            ",
        )?;

        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    // ──────────────────────────────────────────────────────────────────────
    // I: Semantic Query Intelligence
    // ──────────────────────────────────────────────────────────────────────

    /// I4: Get cross-invocation chronological timeline with diagnostic counts.
    pub fn get_invocation_timeline(
        &self,
        command: Option<&str>,
        days: u32,
        limit: usize,
    ) -> Result<Vec<InvocationTimelineEntry>> {
        self.get_invocation_timeline_with_zombies(command, days, limit, false)
    }

    /// I4: Get cross-invocation chronological timeline, optionally including zombie cancellations.
    pub fn get_invocation_timeline_with_zombies(
        &self,
        command: Option<&str>,
        days: u32,
        limit: usize,
        include_zombies: bool,
    ) -> Result<Vec<InvocationTimelineEntry>> {
        let cutoff = format_history_timestamp(
            time::OffsetDateTime::now_utc() - time::Duration::days(i64::from(days)),
            "history timeline cutoff",
        )?;

        let mut sql = String::from(
            r"
            SELECT
                i.id,
                i.command,
                i.status,
                i.started_at,
                i.duration_secs,
                COALESCE(st.stage_count, 0) as stage_count,
                COALESCE(dc_err.error_count, 0) as error_count,
                COALESCE(dc_warn.warning_count, 0) as warning_count
            FROM invocations i
            LEFT JOIN (
                SELECT invocation_id, COUNT(*) as stage_count
                FROM stage_timings GROUP BY invocation_id
            ) st ON i.id = st.invocation_id
            LEFT JOIN (
                SELECT invocation_id, COUNT(*) as error_count
                FROM build_diagnostics WHERE level = 'error'
                GROUP BY invocation_id
            ) dc_err ON i.id = dc_err.invocation_id
            LEFT JOIN (
                SELECT invocation_id, COUNT(*) as warning_count
                FROM build_diagnostics WHERE level = 'warning'
                GROUP BY invocation_id
            ) dc_warn ON i.id = dc_warn.invocation_id
            WHERE i.status IN ('success', 'failed', 'cancelled')
              AND i.started_at >= ?1
            ",
        );
        if !include_zombies {
            sql.push_str(" AND ");
            sql.push_str(&non_zombie_cancel_filter("i."));
        }

        let mut params: Vec<String> = vec![cutoff];
        let mut idx = 2usize;

        if let Some(cmd) = command {
            sql.push_str(&format!(" AND i.command = ?{idx}"));
            params.push(cmd.to_string());
            idx += 1;
        }
        let _ = idx;
        sql.push_str(&format!(" ORDER BY i.id DESC LIMIT {limit}"));

        let mut stmt = self
            .conn
            .prepare(&sql)
            .context("failed to prepare timeline query")?;
        let refs: Vec<&dyn rusqlite::ToSql> =
            params.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
        let mut entries: Vec<InvocationTimelineEntry> = stmt
            .query_map(refs.as_slice(), |row| {
                let status_str: String = row.get(2)?;
                let started_at_raw: String = row.get(3)?;
                let started_at = format_invocation_timestamp(
                    3,
                    "started_at",
                    parse_invocation_timestamp(3, "started_at", &started_at_raw)?,
                )?;
                Ok(InvocationTimelineEntry {
                    id: row.get(0)?,
                    command: row.get(1)?,
                    status: parse_stored_invocation_status(status_str)?,
                    started_at,
                    duration_secs: row.get(4)?,
                    stage_count: row.get::<_, i64>(5)? as usize,
                    error_count: row.get::<_, i64>(6)? as usize,
                    warning_count: row.get::<_, i64>(7)? as usize,
                    diagnostic_delta: 0,
                })
            })
            .context("failed to execute timeline query")?
            .collect::<Result<Vec<_>, _>>()
            .context("failed to collect timeline entries")?;

        // Reverse to chronological order for delta computation, then re-reverse.
        entries.reverse();
        for i in 0..entries.len() {
            let curr_total = (entries[i].error_count + entries[i].warning_count) as i64;
            entries[i].diagnostic_delta = if i == 0 {
                0
            } else {
                let prev_total = (entries[i - 1].error_count + entries[i - 1].warning_count) as i64;
                curr_total - prev_total
            };
        }
        entries.reverse();
        Ok(entries)
    }

    /// I6: Group invocations into working sessions (consecutive runs < gap_minutes apart).
    pub fn get_working_sessions(
        &self,
        limit: usize,
        gap_minutes: u32,
    ) -> Result<Vec<WorkingSession>> {
        self.get_working_sessions_with_zombies(limit, gap_minutes, false)
    }

    /// I6: Group invocations into working sessions, optionally including zombie cancellations.
    pub fn get_working_sessions_with_zombies(
        &self,
        limit: usize,
        gap_minutes: u32,
        include_zombies: bool,
    ) -> Result<Vec<WorkingSession>> {
        struct Row {
            command: String,
            started_at: String,
            started_at_ts: OffsetDateTime,
            finished_at: Option<String>,
            duration_secs: Option<f64>,
            status: String,
        }

        let mut sql = String::from(
            r"
            SELECT command, started_at, finished_at, duration_secs, status
            FROM invocations
            WHERE status IN ('success', 'failed', 'cancelled')
            ",
        );
        if !include_zombies {
            sql.push_str(" AND ");
            sql.push_str(&non_zombie_cancel_filter(""));
        }
        sql.push_str(" ORDER BY started_at ASC LIMIT 2000");

        let mut stmt = self.conn.prepare(&sql)?;

        let rows: Vec<Row> = stmt
            .query_map([], |row| {
                let started_at: String = row.get(1)?;
                let finished_at: Option<String> = row.get(2)?;
                let started_at_ts = parse_invocation_timestamp(1, "started_at", &started_at)?;
                if let Some(finished_at_value) = finished_at.as_deref() {
                    let _ = parse_invocation_timestamp(2, "finished_at", finished_at_value)?;
                }
                Ok(Row {
                    command: row.get(0)?,
                    started_at,
                    started_at_ts,
                    finished_at,
                    duration_secs: row.get(3)?,
                    status: row.get(4)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        let gap_secs = i64::from(gap_minutes) * 60;
        let mut sessions: Vec<WorkingSession> = Vec::new();
        let mut current: Option<WorkingSession> = None;
        let mut prev_started: Option<OffsetDateTime> = None;

        for row in &rows {
            let gap_exceeded = prev_started
                .is_none_or(|prev| (row.started_at_ts - prev).whole_seconds() > gap_secs);

            if gap_exceeded {
                if let Some(s) = current.take() {
                    sessions.push(s);
                }
                current = Some(WorkingSession {
                    session_index: 0,
                    first_started: row.started_at.clone(),
                    last_finished: row.finished_at.clone(),
                    invocation_count: 1,
                    commands: vec![row.command.clone()],
                    total_duration_secs: row.duration_secs.unwrap_or(0.0),
                    success_count: usize::from(row.status == "success"),
                    failure_count: usize::from(row.status == "failed"),
                });
            } else if let Some(s) = current.as_mut() {
                s.invocation_count += 1;
                if row.finished_at.is_some() {
                    s.last_finished.clone_from(&row.finished_at);
                }
                if !s.commands.contains(&row.command) {
                    s.commands.push(row.command.clone());
                }
                s.total_duration_secs += row.duration_secs.unwrap_or(0.0);
                if row.status == "success" {
                    s.success_count += 1;
                }
                if row.status == "failed" {
                    s.failure_count += 1;
                }
            }
            prev_started = Some(row.started_at_ts);
        }
        if let Some(s) = current {
            sessions.push(s);
        }

        // Most recent first, assign 1-based indices, truncate.
        sessions.reverse();
        for (i, s) in sessions.iter_mut().enumerate() {
            s.session_index = i + 1;
        }
        sessions.truncate(limit);
        Ok(sessions)
    }

    /// I7: Get complete single-invocation picture (invocation + stages + diagnostics).
    pub fn get_invocation_full(&self, id: i64) -> Result<Option<InvocationFull>> {
        let inv = self
            .conn
            .query_row(
                r"SELECT id, command, subcommand, profile, args_json, git_commit, git_dirty,
                         started_at, finished_at, duration_secs, exit_code, status, host, cwd,
                         live_stage
                  FROM invocations WHERE id = ?1",
                params![id],
                row_to_invocation,
            )
            .optional()
            .context("failed to fetch invocation")?;

        let Some(inv) = inv else {
            return Ok(None);
        };

        let stages = self.get_stage_timings_for_invocation(id)?;

        let mut diag_stmt = self.conn.prepare(
            r"SELECT id, level, code, message, file_path, line, col, rendered, package,
                     fix_replacement, fix_applicability, fix_byte_start, fix_byte_end,
                     COALESCE(authority, 'proof') as authority, NULL as source_command,
                     NULL as source_time
              FROM build_diagnostics
              WHERE invocation_id = ?1
              ORDER BY level, package, file_path",
        )?;
        let diagnostics: Vec<StoredDiagnostic> = diag_stmt
            .query_map(params![id], row_to_diagnostic_full)?
            .collect::<Result<Vec<_>, _>>()?;

        let error_count = diagnostics.iter().filter(|d| d.level == "error").count();
        let warning_count = diagnostics.iter().filter(|d| d.level == "warning").count();
        Ok(Some(InvocationFull {
            invocation: inv,
            stages,
            diagnostics,
            error_count,
            warning_count,
        }))
    }

    /// I2: Execute a read-only SQL query and return rows as JSON objects.
    ///
    /// Only SELECT / WITH / PRAGMA statements are accepted (checked syntactically).
    /// Results are returned as a vector of JSON maps, keyed by column name.
    pub fn run_readonly_query(
        &self,
        sql: &str,
    ) -> Result<Vec<serde_json::Map<String, serde_json::Value>>> {
        let trimmed = sql.trim().to_uppercase();
        if !trimmed.starts_with("SELECT")
            && !trimmed.starts_with("WITH")
            && !trimmed.starts_with("PRAGMA")
        {
            return Err(color_eyre::eyre::eyre!(
                "Only SELECT, WITH, and PRAGMA queries are permitted (got: {})",
                &sql[..sql.len().min(40)]
            ));
        }
        let mut stmt = self.conn.prepare(sql).wrap_err("failed to prepare query")?;
        let col_names: Vec<String> = stmt
            .column_names()
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        let rows = stmt
            .query_map([], |row| {
                let mut map = serde_json::Map::new();
                for (i, name) in col_names.iter().enumerate() {
                    let val: rusqlite::types::Value = row.get(i)?;
                    let json_val = match val {
                        rusqlite::types::Value::Null => serde_json::Value::Null,
                        rusqlite::types::Value::Integer(n) => serde_json::Value::Number(n.into()),
                        rusqlite::types::Value::Real(f) => serde_json::Number::from_f64(f)
                            .map_or(serde_json::Value::Null, serde_json::Value::Number),
                        rusqlite::types::Value::Text(s) => serde_json::Value::String(s),
                        rusqlite::types::Value::Blob(_) => {
                            serde_json::Value::String("<blob>".to_string())
                        }
                    };
                    map.insert(name.clone(), json_val);
                }
                Ok(map)
            })
            .wrap_err("failed to execute query")?
            .collect::<Result<Vec<_>, _>>()
            .wrap_err("failed to collect query results")?;
        Ok(rows)
    }

    /// I2: Dump (table_name, CREATE TABLE sql) pairs for the history database schema.
    pub fn get_schema_dump(&self) -> Result<Vec<(String, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT name, sql FROM sqlite_schema WHERE type = 'table' AND sql IS NOT NULL ORDER BY name",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    fn parse_invocation_selector(selector: &str) -> Result<InvocationSelector> {
        if selector == "latest" {
            return Ok(InvocationSelector::Latest);
        }
        if selector == "previous" {
            return Ok(InvocationSelector::Previous);
        }
        if selector == "current" {
            return Ok(InvocationSelector::Current);
        }

        let (kind, raw_id) = if let Some(value) = selector.strip_prefix("job:") {
            ("job", value)
        } else if let Some(value) = selector.strip_prefix("background-job:") {
            ("job", value)
        } else if let Some(value) = selector.strip_prefix("inv:") {
            ("invocation", value)
        } else if let Some(value) = selector.strip_prefix("invocation:") {
            ("invocation", value)
        } else {
            ("invocation", selector)
        };

        let id = raw_id.parse::<i64>().map_err(|_| {
            color_eyre::eyre::eyre!(
                "invalid invocation selector: '{selector}' (expected 'latest', 'previous', 'current', a numeric invocation ID, 'inv:<id>', or 'job:<id>')"
            )
        })?;

        Ok(match kind {
            "job" => InvocationSelector::BackgroundJobId(id),
            _ => InvocationSelector::InvocationId(id),
        })
    }

    fn resolve_completed_invocation_offset(
        &self,
        command: Option<&str>,
        offset: usize,
    ) -> Result<Option<i64>> {
        let offset = offset as i64;
        let id = if let Some(cmd) = command {
            self.conn
                .query_row(
                    r"SELECT id FROM invocations WHERE status IN ('success', 'failed')
                      AND command = ?1 ORDER BY id DESC LIMIT 1 OFFSET ?2",
                    params![cmd, offset],
                    |row| row.get(0),
                )
                .optional()?
        } else {
            self.conn
                .query_row(
                    r"SELECT id FROM invocations WHERE status IN ('success', 'failed')
                      ORDER BY id DESC LIMIT 1 OFFSET ?1",
                    params![offset],
                    |row| row.get(0),
                )
                .optional()?
        };
        Ok(id)
    }

    fn resolve_current_invocation(&self, command: Option<&str>) -> Result<Option<i64>> {
        let host = crate::config::config().hostname.clone();
        let cwd = capture_working_directory(std::env::current_dir());
        let id = if let Some(cmd) = command {
            self.conn
                .query_row(
                    r"
                    SELECT id
                    FROM invocations
                    WHERE host = ?1
                      AND cwd = ?2
                      AND command = ?3
                    ORDER BY CASE WHEN status = 'running' THEN 0 ELSE 1 END, id DESC
                    LIMIT 1
                    ",
                    params![host, cwd, cmd],
                    |row| row.get(0),
                )
                .optional()?
        } else {
            self.conn
                .query_row(
                    r"
                    SELECT id
                    FROM invocations
                    WHERE host = ?1
                      AND cwd = ?2
                    ORDER BY CASE WHEN status = 'running' THEN 0 ELSE 1 END, id DESC
                    LIMIT 1
                    ",
                    params![host, cwd],
                    |row| row.get(0),
                )
                .optional()?
        };
        Ok(id)
    }

    fn resolve_background_job_invocation(
        &self,
        job_id: i64,
        command: Option<&str>,
    ) -> Result<Option<i64>> {
        // `invocation_id` is nullable in background_jobs — use Option<i64> to
        // distinguish "row not found" (outer None) from "row found but NULL id"
        // (inner None), then flatten both into Option<i64>.
        let id: Option<Option<i64>> = if let Some(cmd) = command {
            self.conn
                .query_row(
                    r"
                    SELECT invocation_id
                    FROM background_jobs
                    WHERE id = ?1
                      AND command = ?2
                    LIMIT 1
                    ",
                    params![job_id, cmd],
                    |row| row.get::<_, Option<i64>>(0),
                )
                .optional()?
        } else {
            self.conn
                .query_row(
                    r"SELECT invocation_id FROM background_jobs WHERE id = ?1 LIMIT 1",
                    params![job_id],
                    |row| row.get::<_, Option<i64>>(0),
                )
                .optional()?
        };
        Ok(id.flatten())
    }

    /// Resolve an invocation selector to a concrete invocation ID.
    ///
    /// Supports:
    /// - `latest`: most recent completed invocation (`success` / `failed`)
    /// - `previous`: invocation immediately before `latest`
    /// - `current`: most recent invocation from the current checkout, preferring a running one
    /// - numeric ID / `inv:<id>`: explicit invocation
    /// - `job:<id>`: background job handle mapped back to its invocation
    pub fn resolve_invocation_id(
        &self,
        id_or_latest: &str,
        command: Option<&str>,
    ) -> Result<Option<i64>> {
        match Self::parse_invocation_selector(id_or_latest)? {
            InvocationSelector::Latest => self.resolve_completed_invocation_offset(command, 0),
            InvocationSelector::Previous => self.resolve_completed_invocation_offset(command, 1),
            InvocationSelector::Current => self.resolve_current_invocation(command),
            InvocationSelector::BackgroundJobId(job_id) => {
                self.resolve_background_job_invocation(job_id, command)
            }
            InvocationSelector::InvocationId(invocation_id) => {
                if id_or_latest.chars().all(|ch| ch.is_ascii_digit())
                    && self
                        .conn
                        .query_row(
                            r"SELECT 1 FROM invocations WHERE id = ?1 LIMIT 1",
                            params![invocation_id],
                            |_| Ok(()),
                        )
                        .optional()?
                        .is_none()
                {
                    return self.resolve_background_job_invocation(invocation_id, command);
                }
                Ok(Some(invocation_id))
            }
        }
    }

    /// Get the invocation ID immediately before `before_id` for the same command (if given).
    pub fn get_previous_invocation_id(
        &self,
        before_id: i64,
        command: Option<&str>,
    ) -> Result<Option<i64>> {
        let id = if let Some(cmd) = command {
            self.conn
                .query_row(
                    r"SELECT id FROM invocations WHERE status IN ('success', 'failed')
                      AND id < ?1 AND command = ?2 ORDER BY id DESC LIMIT 1",
                    params![before_id, cmd],
                    |row| row.get(0),
                )
                .optional()?
        } else {
            self.conn
                .query_row(
                    r"SELECT id FROM invocations WHERE status IN ('success', 'failed')
                      AND id < ?1 ORDER BY id DESC LIMIT 1",
                    params![before_id],
                    |row| row.get(0),
                )
                .optional()?
        };
        Ok(id)
    }
}
