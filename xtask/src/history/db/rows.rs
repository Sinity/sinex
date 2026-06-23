use super::{
    BackgroundJob, Invocation, InvocationStatus, JobLifecycleStatus, ProofEvidence, TestProofUnit,
};
use color_eyre::eyre::{Result, WrapErr};
use time::OffsetDateTime;

#[allow(
    clippy::needless_pass_by_value,
    reason = "called from rusqlite with String"
)]
pub(crate) fn parse_stored_invocation_status(
    status_str: String,
) -> rusqlite::Result<InvocationStatus> {
    InvocationStatus::try_from_str(&status_str).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid invocation status in history DB: {status_str}"),
            )),
        )
    })
}

fn invalid_invocation_field(
    column_index: usize,
    field_name: &'static str,
    error: impl std::error::Error + Send + Sync + 'static,
) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        column_index,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("invalid invocation {field_name}: {error}"),
        )),
    )
}

pub(super) fn parse_invocation_timestamp(
    column_index: usize,
    field_name: &'static str,
    value: &str,
) -> rusqlite::Result<OffsetDateTime> {
    OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
        .map_err(|error| invalid_invocation_field(column_index, field_name, error))
}

pub(super) fn format_invocation_timestamp(
    column_index: usize,
    field_name: &'static str,
    value: OffsetDateTime,
) -> rusqlite::Result<String> {
    value
        .format(&time::format_description::well_known::Rfc3339)
        .map_err(|error| invalid_invocation_field(column_index, field_name, error))
}

pub(super) fn format_history_timestamp(
    timestamp: OffsetDateTime,
    context: &'static str,
) -> Result<String> {
    timestamp
        .format(&time::format_description::well_known::Rfc3339)
        .wrap_err_with(|| format!("failed to format {context} as RFC3339"))
}

/// Map a SQLite row to a `BackgroundJob`.
///
/// Expected column order (0-indexed):
///   0: id, 1: invocation_id, 2: command, 3: args_json, 4: started_at,
///   5: pid, 6: stdout_path, 7: stderr_path, 8: job_status, 9: exit_code
pub(super) fn row_to_background_job(row: &rusqlite::Row<'_>) -> rusqlite::Result<BackgroundJob> {
    fn invalid_background_job_field(
        column_index: usize,
        field_name: &'static str,
        error: impl std::error::Error + Send + Sync + 'static,
    ) -> rusqlite::Error {
        rusqlite::Error::FromSqlConversionFailure(
            column_index,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid background job {field_name}: {error}"),
            )),
        )
    }

    let args_json: Option<String> = row.get(3)?;
    let started_at_str: String = row.get(4)?;
    let pid: Option<u32> = row.get(5)?;
    let job_status_str: String = row.get(8)?;
    Ok(BackgroundJob {
        id: row.get(0)?,
        invocation_id: row.get(1)?,
        command: row.get(2)?,
        args: match args_json {
            Some(args_json) => serde_json::from_str(&args_json)
                .map_err(|error| invalid_background_job_field(3, "args_json", error))?,
            None => Vec::new(),
        },
        started_at: OffsetDateTime::parse(
            &started_at_str,
            &time::format_description::well_known::Rfc3339,
        )
        .map_err(|error| invalid_background_job_field(4, "started_at", error))?,
        pid,
        stdout_path: row.get(6)?,
        stderr_path: row.get(7)?,
        job_status: JobLifecycleStatus::try_from_str(&job_status_str).map_err(|error| {
            invalid_background_job_field(
                8,
                "job_status",
                std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string()),
            )
        })?,
        exit_code: row.get(9)?,
    })
}

pub(super) fn row_to_proof_evidence(row: &rusqlite::Row<'_>) -> rusqlite::Result<ProofEvidence> {
    let status_str: String = row.get(6)?;
    Ok(ProofEvidence {
        id: row.get(0)?,
        invocation_id: row.get(1)?,
        command: row.get(2)?,
        proof_kind: row.get(3)?,
        scope_key: row.get(4)?,
        input_fingerprint: row.get(5)?,
        status: parse_stored_invocation_status(status_str)?,
        started_at: row.get(7)?,
        finished_at: row.get(8)?,
        duration_secs: row.get(9)?,
        scope_json: row.get(10)?,
        artifact_json: row.get(11)?,
    })
}

pub(super) fn row_to_test_proof_unit(row: &rusqlite::Row<'_>) -> rusqlite::Result<TestProofUnit> {
    let status_str: String = row.get(7)?;
    Ok(TestProofUnit {
        id: row.get(0)?,
        invocation_id: row.get(1)?,
        proof_kind: row.get(2)?,
        scope_key: row.get(3)?,
        input_fingerprint: row.get(4)?,
        manifest_json: row.get(5)?,
        reusable: row.get::<_, i64>(6)? != 0,
        status: parse_stored_invocation_status(status_str)?,
        started_at: row.get(8)?,
        finished_at: row.get(9)?,
        duration_secs: row.get(10)?,
        test_filter: row.get(11)?,
    })
}

pub(crate) fn row_to_invocation(row: &rusqlite::Row) -> rusqlite::Result<Invocation> {
    let started_at_str: String = row.get(7)?;
    let finished_at_str: Option<String> = row.get(8)?;
    let status_str: String = row.get(11)?;

    Ok(Invocation {
        id: row.get(0)?,
        command: row.get(1)?,
        subcommand: row.get(2)?,
        profile: row.get(3)?,
        args_json: row.get(4)?,
        git_commit: row.get(5)?,
        git_dirty: row.get::<_, i32>(6)? != 0,
        started_at: parse_invocation_timestamp(7, "started_at", &started_at_str)?,
        finished_at: finished_at_str
            .as_deref()
            .map(|value| parse_invocation_timestamp(8, "finished_at", value))
            .transpose()?,
        duration_secs: row.get(9)?,
        exit_code: row.get(10)?,
        status: parse_stored_invocation_status(status_str)?,
        host: row.get(12)?,
        cwd: row.get(13)?,
        live_stage: row.get(14)?,
    })
}
