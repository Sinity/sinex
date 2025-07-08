//! Transaction-based test database isolation
//!
//! This module provides a test database that runs each test in its own transaction
//! that gets automatically rolled back at the end, ensuring perfect isolation.

use crate::common::prelude::*;
use sqlx::{PgConnection, Postgres, Transaction};
use std::ops::{Deref, DerefMut};

/// A test database that runs in a transaction
pub struct TransactionTestDatabase<'a> {
    tx: Transaction<'a, Postgres>,
}

impl<'a> TransactionTestDatabase<'a> {
    /// Create a new test database with transaction isolation
    pub async fn new(pool: &'a DbPool) -> Result<Self> {
        let tx = pool.begin().await?;
        Ok(Self { tx })
    }
    
    /// Get a reference to the transaction for direct use
    pub fn transaction(&mut self) -> &mut Transaction<'a, Postgres> {
        &mut self.tx
    }
}

impl<'a> Deref for TransactionTestDatabase<'a> {
    type Target = Transaction<'a, Postgres>;
    
    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl<'a> DerefMut for TransactionTestDatabase<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tx
    }
}

impl<'a> Drop for TransactionTestDatabase<'a> {
    fn drop(&mut self) {
        // Transaction automatically rolls back on drop if not committed
        eprintln!("🔄 Rolling back test transaction");
    }
}

/// Extension trait for DbPool to create transaction-isolated test databases
pub trait TestDbPoolExt {
    async fn test_transaction(&self) -> Result<TransactionTestDatabase>;
}

impl TestDbPoolExt for DbPool {
    async fn test_transaction(&self) -> Result<TransactionTestDatabase> {
        TransactionTestDatabase::new(self).await
    }
}