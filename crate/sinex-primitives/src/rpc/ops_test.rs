use super::{OPS_CANCEL_METHOD, OPS_START_METHOD};
use crate::rpc::RpcRole;
use xtask::sandbox::prelude::{TestResult, sinex_test};

/// sinex-05cg: `ops.start`'s media `worker_command` runs `Command::new(&request.program)`
/// as the sinexd service user (arbitrary program execution, gated only by non-empty/arg-count
/// checks, no allowlist) -- yet the RPC method is registered at `RpcRole::Write`, a LOWER
/// trust tier than `ops.cancel`'s `RpcRole::Admin`. Starting an op that can execute arbitrary
/// binaries arguably needs a higher trust tier than cancelling one, not a lower one.
///
/// This asserts the two methods carry the SAME minimum role, which is currently false --
/// `OPS_START_METHOD.role` is `Write` while `OPS_CANCEL_METHOD.role` is `Admin`.
#[sinex_test]
#[ignore = "sinex-05cg open: ops.start requires only Write while its media worker_command can execute arbitrary programs; role should be >= ops.cancel's Admin"]
async fn ops_start_role_is_not_lower_than_ops_cancel_role() -> TestResult<()> {
    assert_eq!(
        OPS_START_METHOD.role, OPS_CANCEL_METHOD.role,
        "ops.start (which can execute an arbitrary worker_command as the sinexd service \
         user) must not require a lower trust tier than ops.cancel; sinex-05cg",
    );
    Ok(())
}

#[sinex_test]
#[ignore = "sinex-05cg open: ops.start's media worker_command executes arbitrary programs at RpcRole::Write, not Admin"]
async fn ops_start_requires_at_least_admin() -> TestResult<()> {
    assert_eq!(
        OPS_START_METHOD.role,
        RpcRole::Admin,
        "ops.start's media worker_command executes arbitrary programs; it should require \
         Admin, matching or exceeding ops.cancel's tier; sinex-05cg",
    );
    Ok(())
}
