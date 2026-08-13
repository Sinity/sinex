use super::*;
use crate::command::CommandContext;
use crate::output::{OutputFormat, OutputWriter};
use crate::sandbox::sinex_test;

fn silent_ctx() -> CommandContext {
    CommandContext::new(
        OutputWriter::new(OutputFormat::Silent),
        false,
        None,
        "schema",
    )
}

#[sinex_test]
async fn schema_backfill_run_requires_explicit_writer_quiescence() -> crate::sandbox::TestResult<()>
{
    let result = execute_backfill_run(
        PARSED_EVENT_COUNT_BACKFILL_KEY,
        None,
        50_000,
        false,
        false,
        &silent_ctx(),
    )
    .await?;

    assert_eq!(result.errors.len(), 1);
    assert_eq!(result.errors[0].code, "SCHEMA_BACKFILL_REQUIRES_QUIESCENCE");
    assert!(
        result.errors[0]
            .suggestion
            .as_deref()
            .is_some_and(|suggestion| suggestion.contains("wait for their transactions"))
    );
    Ok(())
}

#[sinex_test]
async fn schema_backfill_run_rejects_unknown_keys_before_connecting()
-> crate::sandbox::TestResult<()> {
    let result = execute_backfill_run("unknown", None, 50_000, true, false, &silent_ctx()).await?;

    assert_eq!(result.errors.len(), 1);
    assert_eq!(result.errors[0].code, "UNKNOWN_SCHEMA_BACKFILL");
    Ok(())
}
