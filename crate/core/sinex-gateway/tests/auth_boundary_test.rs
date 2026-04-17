use sinex_gateway::{ServiceContainer, auth::Role, rpc_registry, rpc_server::RpcAuthContext};
use sinex_primitives::temporal::Timestamp;
use xtask::sandbox::prelude::*;

fn auth_with_role(role: Role) -> RpcAuthContext {
    RpcAuthContext {
        token_prefix: "test-tok".to_string(),
        actor_id: "token:test-tok".to_string(),
        authenticated_at: Timestamp::now(),
        role,
    }
}

fn roles_below(required: Role) -> Vec<Role> {
    match required {
        Role::ReadOnly => vec![],
        Role::Write => vec![Role::ReadOnly],
        Role::Admin => vec![Role::ReadOnly, Role::Write],
    }
}

#[sinex_test]
async fn registry_rejects_insufficient_roles_for_every_method(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let nats_url = ctx.nats_handle()?.client_url().to_string();
    let mut env_guard = EnvGuard::new();
    env_guard.set("SINEX_NATS_URL", &nats_url);

    let services = ServiceContainer::from_database_url(ctx.database_url().to_string()).await?;
    let registry = rpc_registry::build_registry();
    let method_roles = registry.method_roles();

    let mut tested = 0u32;
    let mut rejected = 0u32;

    for (&method, &required_role) in &method_roles {
        for insufficient_role in roles_below(required_role) {
            let auth = auth_with_role(insufficient_role);
            let result = registry
                .dispatch(method, serde_json::json!({}), &services, &auth)
                .await;

            match &result {
                Err(e) => {
                    let msg = format!("{e:#}");
                    assert!(
                        msg.contains("permission denied") || msg.contains("Permission denied"),
                        "Method '{method}' rejected {insufficient_role:?} but with wrong error: {msg}"
                    );
                    rejected += 1;
                }
                Ok(_) => {
                    panic!(
                        "Method '{method}' should reject {insufficient_role:?} (requires {required_role:?}) but succeeded"
                    );
                }
            }
            tested += 1;
        }
    }

    assert!(
        tested > 30,
        "Expected at least 30 auth boundary checks, got {tested}"
    );
    assert_eq!(
        rejected, tested,
        "All {tested} insufficient-role attempts should be rejected"
    );

    Ok(())
}

#[sinex_test]
async fn registry_has_expected_role_distribution(_ctx: TestContext) -> TestResult<()> {
    let registry = rpc_registry::build_registry();
    let method_roles = registry.method_roles();

    let readonly_count = method_roles
        .values()
        .filter(|r| **r == Role::ReadOnly)
        .count();
    let write_count = method_roles.values().filter(|r| **r == Role::Write).count();
    let admin_count = method_roles.values().filter(|r| **r == Role::Admin).count();
    let total = method_roles.len();

    assert!(
        readonly_count >= 15,
        "Expected 15+ ReadOnly methods, got {readonly_count}"
    );
    assert!(
        write_count >= 5,
        "Expected 5+ Write methods, got {write_count}"
    );
    assert!(
        admin_count >= 10,
        "Expected 10+ Admin methods, got {admin_count}"
    );
    assert!(total >= 40, "Expected 40+ total methods, got {total}");

    Ok(())
}
