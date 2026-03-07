use sinex_gateway::auth::{Role, TokenRoleError};
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
