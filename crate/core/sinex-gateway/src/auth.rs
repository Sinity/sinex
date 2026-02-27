//! Authentication and authorization types for the gateway
//!
//! This module provides role-based access control (RBAC) for RPC methods.
//! Roles are encoded in the token suffix: `sinex_<random>:<role>`
//!
//! # Role Hierarchy
//!
//! ```text
//! Admin > Write > ReadOnly
//! ```
//!
//! Each higher role includes all permissions of lower roles:
//! - **`ReadOnly`**: Query operations (search, analytics, status)
//! - **Write**: `ReadOnly` + mutations (create entities, store blobs, ingest events)
//! - **Admin**: Write + destructive operations (tombstone, DLQ purge, shadow delete)

use serde::{Deserialize, Serialize};
use std::fmt;

/// Authorization role for RPC access control
///
/// Roles follow a hierarchy where higher roles include all permissions of lower roles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum Role {
    /// Read-only access: search, analytics, status queries
    #[default]
    ReadOnly,
    /// Write access: read + ingest events, create entities, store content
    Write,
    /// Admin access: write + tombstone, DLQ operations, shadow management
    Admin,
}

impl Role {
    /// Parse role from token suffix.
    pub fn from_token_suffix(suffix: &str) -> Result<Self, TokenRoleError> {
        match suffix {
            "readonly" | "read" | "ro" => Ok(Role::ReadOnly),
            "write" | "rw" => Ok(Role::Write),
            "admin" => Ok(Role::Admin),
            other => Err(TokenRoleError::UnknownRole(other.to_string())),
        }
    }

    /// Extract role from a full token string
    ///
    /// Parses the role suffix from tokens in format `sinex_<random>:<role>`
    pub fn from_token(token: &str) -> Result<(String, Self), TokenRoleError> {
        let (base, role_suffix) = token
            .rsplit_once(':')
            .ok_or(TokenRoleError::MissingRoleSuffix)?;
        let role = Self::from_token_suffix(role_suffix)?;
        Ok((base.to_string(), role))
    }

    /// Check if this role has at least the required permission level
    ///
    /// Role hierarchy: Admin > Write > `ReadOnly`
    ///
    /// # Examples
    ///
    /// ```
    /// use sinex_gateway::auth::Role;
    ///
    /// assert!(Role::Admin.has_permission(Role::ReadOnly)); // Admin can do everything
    /// assert!(Role::Admin.has_permission(Role::Write));
    /// assert!(Role::Admin.has_permission(Role::Admin));
    ///
    /// assert!(Role::Write.has_permission(Role::ReadOnly));
    /// assert!(Role::Write.has_permission(Role::Write));
    /// assert!(!Role::Write.has_permission(Role::Admin)); // Write can't do admin ops
    ///
    /// assert!(Role::ReadOnly.has_permission(Role::ReadOnly));
    /// assert!(!Role::ReadOnly.has_permission(Role::Write));
    /// assert!(!Role::ReadOnly.has_permission(Role::Admin));
    /// ```
    #[must_use]
    pub fn has_permission(&self, required: Role) -> bool {
        match (self, required) {
            // Everyone can read
            (_, Role::ReadOnly) => true,
            // Write and Admin can write
            (Role::Write | Role::Admin, Role::Write) => true,
            // Only Admin can do admin operations
            (Role::Admin, Role::Admin) => true,
            // All other combinations are denied
            _ => false,
        }
    }

    /// Get the permission level as a numeric value for comparison
    #[must_use]
    pub fn level(&self) -> u8 {
        match self {
            Role::ReadOnly => 0,
            Role::Write => 1,
            Role::Admin => 2,
        }
    }

    /// Check if this is an admin role
    #[must_use]
    pub fn is_admin(&self) -> bool {
        matches!(self, Role::Admin)
    }

    /// Check if this role can write
    #[must_use]
    pub fn can_write(&self) -> bool {
        matches!(self, Role::Write | Role::Admin)
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Role::ReadOnly => write!(f, "readonly"),
            Role::Write => write!(f, "write"),
            Role::Admin => write!(f, "admin"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenRoleError {
    MissingRoleSuffix,
    UnknownRole(String),
}

impl fmt::Display for TokenRoleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TokenRoleError::MissingRoleSuffix => {
                write!(f, "RPC token must include a role suffix (e.g. ':readonly')")
            }
            TokenRoleError::UnknownRole(role) => write!(f, "unknown role suffix '{role}'"),
        }
    }
}

impl std::error::Error for TokenRoleError {}

/// Error returned when a role check fails
#[derive(Debug, Clone)]
pub struct InsufficientPermissions {
    /// The role the token has
    pub actual: Role,
    /// The role required for the operation
    pub required: Role,
    /// The operation that was attempted
    pub operation: String,
}

impl fmt::Display for InsufficientPermissions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Operation '{}' requires {:?} role, but token has {:?}",
            self.operation, self.required, self.actual
        )
    }
}

impl std::error::Error for InsufficientPermissions {}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    fn test_role_from_suffix() -> TestResult<()> {
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
    fn test_role_from_token() -> TestResult<()> {
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
    fn test_role_hierarchy() -> TestResult<()> {
        // Admin can do everything
        assert!(Role::Admin.has_permission(Role::ReadOnly));
        assert!(Role::Admin.has_permission(Role::Write));
        assert!(Role::Admin.has_permission(Role::Admin));

        // Write can read and write
        assert!(Role::Write.has_permission(Role::ReadOnly));
        assert!(Role::Write.has_permission(Role::Write));
        assert!(!Role::Write.has_permission(Role::Admin));

        // ReadOnly can only read
        assert!(Role::ReadOnly.has_permission(Role::ReadOnly));
        assert!(!Role::ReadOnly.has_permission(Role::Write));
        assert!(!Role::ReadOnly.has_permission(Role::Admin));
        Ok(())
    }

    #[sinex_test]
    fn test_role_display() -> TestResult<()> {
        assert_eq!(Role::ReadOnly.to_string(), "readonly");
        assert_eq!(Role::Write.to_string(), "write");
        assert_eq!(Role::Admin.to_string(), "admin");
        Ok(())
    }
}
