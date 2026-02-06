// NOTE: Tests temporarily ignored pending API migration

use anyhow::ensure;
use serde_json::json;
use sinex_primitives::{DynamicPayload, Ulid};
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::WaitHelpers;

// FIXME: API removed, needs migration
// use xtask::sandbox::PipelineNamespace;

#[sinex_test]
#[ignore]
async fn pipeline_namespace_subjects_are_isolated(ctx: TestContext) -> TestResult<()> {
    // FIXME: PipelineNamespace and related APIs no longer available
    // let ctx = ctx.with_nats().await?;
    // let source = "namespace-isolation";
    // let event_type = "isolation.event";
    // let ns_a = PipelineNamespace::new("namespace-isolation-a");
    // let ns_b = PipelineNamespace::new("namespace-isolation-b");
    // ... rest of test commented out pending migration
    Ok(())
}
