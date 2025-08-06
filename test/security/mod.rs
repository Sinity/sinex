//! Security testing module
//!
//! This module contains comprehensive security tests including:
//! - Unicode-based attacks and vulnerabilities
//! - Input validation bypass attempts
//! - Injection attacks (SQL, command, etc.)
//! - Authentication and authorization edge cases
//! - Cryptographic edge cases

use sinex_test_utils::prelude::*;

/// Unicode security testing including homograph attacks, normalization exploits, etc.
pub mod unicode_attack_test;
