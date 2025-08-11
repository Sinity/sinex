//! Security testing module
//!
//! This module contains comprehensive security tests including:
//! - Unicode-based attacks and vulnerabilities
//! - Input validation bypass attempts
//! - Injection attacks (SQL, command, etc.)
//! - Authentication and authorization edge cases
//! - Cryptographic edge cases

use color_eyre::eyre::Result;
use sinex_test_utils::prelude::*;

/// Unicode security testing including homograph attacks, normalization exploits, etc.
pub mod unicode_attack_test;

/// Path validation security testing including path traversal attack prevention
pub mod path_validation_test;

/// HistoryWatcher security testing including path validation and boundary enforcement
pub mod history_watcher_security_test;
