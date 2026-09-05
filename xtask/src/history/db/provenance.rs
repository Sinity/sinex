use super::*;

/// The runtime exports `AGENTCTL_<NAME>`; older hosts export the same values as
/// `SINNIXD_<NAME>`. Every field is read through [`runtime_env`] so the fallback
/// can be removed in one place.
const RUNTIME_ENV_PREFIX: &str = "AGENTCTL_";
const LEGACY_RUNTIME_ENV_PREFIX: &str = "SINNIXD_";

const AGENTCTL_FIELDS: [(&str, &str); 6] = [
    ("JOB_ID", "job_id"),
    ("CORRELATION_ID", "correlation_id"),
    ("PROJECT_ID", "project_id"),
    ("OPERATION", "operation"),
    ("CHECKOUT_ID", "checkout_id"),
    ("CHECKOUT_HEAD", "checkout_head"),
];

/// `AGENTCTL_<name>`, or `SINNIXD_<name>` when only that is set.
fn runtime_env(name: &str, lookup: &mut impl FnMut(&str) -> Option<String>) -> Option<String> {
    lookup(&format!("{RUNTIME_ENV_PREFIX}{name}"))
        .or_else(|| lookup(&format!("{LEGACY_RUNTIME_ENV_PREFIX}{name}")))
}

impl AgentctlProvenance {
    pub(super) fn from_environment() -> Result<Option<Self>> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Option<Self>> {
        let values = AGENTCTL_FIELDS.map(|(name, _)| runtime_env(name, &mut lookup));
        if values.iter().all(Option::is_none) {
            return Ok(None);
        }

        let mut required = Vec::with_capacity(AGENTCTL_FIELDS.len());
        for ((name, field), value) in AGENTCTL_FIELDS.into_iter().zip(values) {
            let value = value.ok_or_else(|| {
                color_eyre::eyre::eyre!(
                    "incomplete AgentCTL provenance: {RUNTIME_ENV_PREFIX}{name} ({field}) is missing"
                )
            })?;
            if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
                color_eyre::eyre::bail!(
                    "invalid AgentCTL provenance value for {RUNTIME_ENV_PREFIX}{name}: expected 1..=256 non-control characters"
                );
            }
            required.push(value);
        }

        let [
            job_id,
            correlation_id,
            project_id,
            operation,
            checkout_id,
            checkout_head,
        ]: [String; 6] = required
            .try_into()
            .expect("six AgentCTL fields were collected");
        Ok(Some(Self {
            invocation_id: 0,
            job_id,
            correlation_id,
            project_id,
            operation,
            checkout_id,
            checkout_head,
        }))
    }
}

impl HistoryDb {
    pub(super) fn insert_agentctl_provenance(
        &self,
        invocation_id: i64,
        provenance: &AgentctlProvenance,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO invocation_agentctl_provenance \
             (invocation_id, job_id, correlation_id, project_id, operation, checkout_id, checkout_head) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                invocation_id,
                provenance.job_id,
                provenance.correlation_id,
                provenance.project_id,
                provenance.operation,
                provenance.checkout_id,
                provenance.checkout_head,
            ],
        )?;
        Ok(())
    }

    /// Return the optional generic runtime envelope for an xtask invocation.
    pub fn get_agentctl_provenance(
        &self,
        invocation_id: i64,
    ) -> Result<Option<AgentctlProvenance>> {
        self.conn
            .query_row(
                "SELECT invocation_id, job_id, correlation_id, project_id, operation, \
                        checkout_id, checkout_head \
                 FROM invocation_agentctl_provenance WHERE invocation_id = ?1",
                params![invocation_id],
                |row| {
                    Ok(AgentctlProvenance {
                        invocation_id: row.get(0)?,
                        job_id: row.get(1)?,
                        correlation_id: row.get(2)?,
                        project_id: row.get(3)?,
                        operation: row.get(4)?,
                        checkout_id: row.get(5)?,
                        checkout_head: row.get(6)?,
                    })
                },
            )
            .optional()
            .context("failed to query AgentCTL invocation provenance")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn foreground_environment_has_no_provenance() -> Result<()> {
        assert_eq!(AgentctlProvenance::from_lookup(|_| None)?, None);
        Ok(())
    }

    #[test]
    fn partial_agentctl_envelope_is_rejected() {
        for job_id_name in ["AGENTCTL_JOB_ID", "SINNIXD_JOB_ID"] {
            let error = AgentctlProvenance::from_lookup(|name| {
                (name == job_id_name).then(|| "job".to_string())
            })
            .expect_err("partial envelope must not silently create ambiguous provenance");
            assert!(error.to_string().contains("AGENTCTL_CORRELATION_ID"));
        }
    }

    fn complete_envelope(prefix: &str) -> impl FnMut(&str) -> Option<String> + '_ {
        move |name| {
            name.strip_prefix(prefix)
                .map(|suffix| format!("{}-value", suffix.to_ascii_lowercase()))
        }
    }

    #[test]
    fn either_runtime_name_family_yields_the_same_provenance() -> Result<()> {
        let current = AgentctlProvenance::from_lookup(complete_envelope("AGENTCTL_"))?;
        let legacy = AgentctlProvenance::from_lookup(complete_envelope("SINNIXD_"))?;
        assert_eq!(current, legacy);
        assert_eq!(current.expect("complete envelope").job_id, "job_id-value");
        Ok(())
    }

    #[test]
    fn the_current_name_wins_over_the_legacy_one() -> Result<()> {
        let provenance = AgentctlProvenance::from_lookup(|name| match name {
            "AGENTCTL_JOB_ID" => Some("new".to_string()),
            "SINNIXD_JOB_ID" => Some("old".to_string()),
            other => complete_envelope("AGENTCTL_")(other),
        })?
        .expect("complete envelope");
        assert_eq!(provenance.job_id, "new");
        Ok(())
    }

    #[test]
    fn external_ids_join_to_xtask_owned_stage_evidence() -> Result<()> {
        let db = HistoryDb::open_in_memory()?;
        db.conn.execute(
            "INSERT INTO invocations (command, started_at, host, cwd, status) \
             VALUES ('check', '2026-08-24T00:00:00Z', 'fixture', '/fixture', 'running')",
            [],
        )?;
        let invocation_id = db.conn.last_insert_rowid();
        let provenance = AgentctlProvenance {
            invocation_id,
            job_id: "job-123".into(),
            correlation_id: "correlation-456".into(),
            project_id: "sinex".into(),
            operation: "check_changed".into(),
            checkout_id: "checkout-789".into(),
            checkout_head: "0123456789abcdef".into(),
        };
        db.insert_agentctl_provenance(invocation_id, &provenance)?;
        db.record_stage_timing(
            invocation_id,
            "compile",
            "2026-08-24T00:00:01Z",
            1.0,
            true,
            StagePressure::default(),
        )?;

        assert_eq!(db.get_agentctl_provenance(invocation_id)?, Some(provenance));
        let stage_count: i64 = db.conn.query_row(
            "SELECT COUNT(*) FROM stage_timings WHERE invocation_id = ?1 AND stage_name = 'compile'",
            params![invocation_id],
            |row| row.get(0),
        )?;
        assert_eq!(stage_count, 1, "stage attribution remains xtask-owned");
        Ok(())
    }

    #[test]
    fn existing_history_rows_migrate_without_synthetic_provenance() -> Result<()> {
        let directory = tempdir()?;
        let path = directory.path().join("history.db");
        {
            let db = HistoryDb::open(&path)?;
            db.conn.execute(
                "INSERT INTO invocations (command, started_at, host, cwd, status) \
                 VALUES ('check', '2026-08-23T00:00:00Z', 'fixture', '/fixture', 'success')",
                [],
            )?;
            db.conn
                .execute_batch("DROP TABLE invocation_agentctl_provenance")?;
        }

        let migrated = HistoryDb::open(&path)?;
        assert_eq!(migrated.get_agentctl_provenance(1)?, None);
        assert!(migrated.column_exists("invocation_agentctl_provenance", "job_id")?);
        Ok(())
    }
}
