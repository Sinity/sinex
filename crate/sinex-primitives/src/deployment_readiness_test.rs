use super::DeploymentReadinessDescriptor;
use std::env;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn configured_path_treats_empty_override_as_disabled() -> TestResult<()> {
    let previous = env::var_os("SINEX_DEPLOYMENT_READINESS_CONFIG");
    unsafe { env::set_var("SINEX_DEPLOYMENT_READINESS_CONFIG", "") };

    let configured = DeploymentReadinessDescriptor::configured_path();

    match previous {
        Some(value) => unsafe { env::set_var("SINEX_DEPLOYMENT_READINESS_CONFIG", value) },
        None => unsafe { env::remove_var("SINEX_DEPLOYMENT_READINESS_CONFIG") },
    }

    assert!(configured.is_none());
    Ok(())
}
