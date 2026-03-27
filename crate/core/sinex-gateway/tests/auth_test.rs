use sinex_gateway::{
    auth::{Role, TokenRoleError},
    rpc_server::RpcAuthContext,
};
use sinex_primitives::temporal;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_role_from_suffix() -> TestResult<()> {
    assert_eq!(Role::from_token_suffix("readonly")?, Role::ReadOnly);
    assert_eq!(Role::from_token_suffix("read")?, Role::ReadOnly);
    assert_eq!(Role::from_token_suffix("ro")?, Role::ReadOnly);
    assert_eq!(Role::from_token_suffix("write")?, Role::Write);
    assert_eq!(Role::from_token_suffix("rw")?, Role::Write);
    assert_eq!(Role::from_token_suffix("admin")?, Role::Admin);
    assert!(matches!(
        Role::from_token_suffix("unknown"),
        Err(TokenRoleError::UnknownRole(_))
    ));
    Ok(())
}

#[sinex_test]
async fn test_role_from_token() -> TestResult<()> {
    assert!(matches!(
        Role::from_token("sinex_abc123def456"),
        Err(TokenRoleError::MissingRoleSuffix)
    ));

    let (base, role) = Role::from_token("sinex_abc123def456:readonly")?;
    assert_eq!(base, "sinex_abc123def456");
    assert_eq!(role, Role::ReadOnly);

    let (base, role) = Role::from_token("sinex_abc123def456:write")?;
    assert_eq!(base, "sinex_abc123def456");
    assert_eq!(role, Role::Write);

    let (base, role) = Role::from_token("sinex_abc123def456:admin")?;
    assert_eq!(base, "sinex_abc123def456");
    assert_eq!(role, Role::Admin);
    assert!(matches!(
        Role::from_token("sinex_abc123def456:owner"),
        Err(TokenRoleError::UnknownRole(_))
    ));
    Ok(())
}

#[sinex_test]
async fn test_role_hierarchy() -> TestResult<()> {
    assert!(Role::Admin.has_permission(Role::ReadOnly));
    assert!(Role::Admin.has_permission(Role::Write));
    assert!(Role::Admin.has_permission(Role::Admin));

    assert!(Role::Write.has_permission(Role::ReadOnly));
    assert!(Role::Write.has_permission(Role::Write));
    assert!(!Role::Write.has_permission(Role::Admin));

    assert!(Role::ReadOnly.has_permission(Role::ReadOnly));
    assert!(!Role::ReadOnly.has_permission(Role::Write));
    assert!(!Role::ReadOnly.has_permission(Role::Admin));
    Ok(())
}

#[sinex_test]
async fn test_role_display() -> TestResult<()> {
    assert_eq!(Role::ReadOnly.to_string(), "readonly");
    assert_eq!(Role::Write.to_string(), "write");
    assert_eq!(Role::Admin.to_string(), "admin");
    Ok(())
}

#[sinex_test]
async fn replay_actor_maps_gateway_roles_to_valid_replay_roles() -> TestResult<()> {
    let now = temporal::now();

    let readonly = RpcAuthContext {
        token_prefix: "readtok".to_string(),
        actor_id: "token:readtok".to_string(),
        authenticated_at: now,
        role: Role::ReadOnly,
    };
    assert_eq!(readonly.replay_actor(), "user:token:readtok");

    let write = RpcAuthContext {
        token_prefix: "writetok".to_string(),
        actor_id: "token:writetok".to_string(),
        authenticated_at: now,
        role: Role::Write,
    };
    assert_eq!(write.replay_actor(), "operator:token:writetok");

    let admin = RpcAuthContext {
        token_prefix: "admintok".to_string(),
        actor_id: "token:admintok".to_string(),
        authenticated_at: now,
        role: Role::Admin,
    };
    assert_eq!(admin.replay_actor(), "admin:token:admintok");

    Ok(())
}

#[sinex_test]
async fn replay_actor_preserves_system_identity() -> TestResult<()> {
    let auth = RpcAuthContext::system();
    assert_eq!(auth.replay_actor(), "system:local");
    Ok(())
}
