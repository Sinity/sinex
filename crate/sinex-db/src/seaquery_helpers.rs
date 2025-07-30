//! SeaQuery ULID helper traits and utilities
//!
//! This module provides ergonomic helpers for working with ULIDs in SeaQuery,
//! automatically handling the ULID to UUID conversion required by PostgreSQL.

use sea_query::{Expr, SimpleExpr};
use sinex_ulid::Ulid;
use uuid::Uuid;
use crate::query_helpers::{ulid_to_uuid, uuid_to_ulid};

/// Helper trait for ULID-capable types
pub trait AsUlid {
    fn as_ulid(&self) -> &Ulid;
}

impl AsUlid for Ulid {
    fn as_ulid(&self) -> &Ulid {
        self
    }
}

/// Helper trait for converting arrays of ULID-capable types
pub trait IntoUlidArray {
    fn into_ulid_array(self) -> Vec<Ulid>;
}

impl<T: AsUlid> IntoUlidArray for Vec<T> {
    fn into_ulid_array(self) -> Vec<Ulid> {
        self.into_iter().map(|id| *id.as_ulid()).collect()
    }
}

impl<T: AsUlid, const N: usize> IntoUlidArray for [T; N] {
    fn into_ulid_array(self) -> Vec<Ulid> {
        self.into_iter().map(|id| *id.as_ulid()).collect()
    }
}

impl<T: AsUlid> IntoUlidArray for &[T] {
    fn into_ulid_array(self) -> Vec<Ulid> {
        self.iter().map(|id| *id.as_ulid()).collect()
    }
}

/// Extension trait for SeaQuery expressions to work with ULIDs
pub trait SeaQueryUlidExt {
    /// Compare equality with a ULID, automatically converting to UUID
    fn eq_ulid(self, id: impl AsUlid) -> SimpleExpr;
    
    /// Check if value is in a list of ULIDs, automatically converting to UUIDs
    fn in_ulids(self, ids: impl IntoUlidArray) -> SimpleExpr;
    
    /// Check if value is not in a list of ULIDs, automatically converting to UUIDs
    fn not_in_ulids(self, ids: impl IntoUlidArray) -> SimpleExpr;
}

impl SeaQueryUlidExt for Expr {
    fn eq_ulid(self, id: impl AsUlid) -> SimpleExpr {
        self.eq(ulid_to_uuid(*id.as_ulid()))
    }
    
    fn in_ulids(self, ids: impl IntoUlidArray) -> SimpleExpr {
        let uuids: Vec<Uuid> = ids.into_ulid_array()
            .into_iter()
            .map(|id| ulid_to_uuid(id))
            .collect();
        self.is_in(uuids)
    }
    
    fn not_in_ulids(self, ids: impl IntoUlidArray) -> SimpleExpr {
        let uuids: Vec<Uuid> = ids.into_ulid_array()
            .into_iter()
            .map(|id| ulid_to_uuid(id))
            .collect();
        self.is_not_in(uuids)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::{Query, PostgresQueryBuilder, Iden};
    
    // Simple test table for demonstration
    #[derive(Copy, Clone)]
    struct Events;
    
    impl Iden for Events {
        fn unquoted(&self, s: &mut dyn std::fmt::Write) {
            write!(s, "events").unwrap();
        }
    }
    
    #[derive(Copy, Clone)]
    enum EventCols {
        EventId,
    }
    
    impl Iden for EventCols {
        fn unquoted(&self, s: &mut dyn std::fmt::Write) {
            match self {
                EventCols::EventId => write!(s, "event_id").unwrap(),
            }
        }
    }
    
    #[test]
    fn test_eq_ulid() {
        let ulid = Ulid::new();
        let query = Query::select()
            .from(Events)
            .and_where(Expr::col(EventCols::EventId).eq_ulid(ulid))
            .to_owned();
            
        let (sql, _) = query.build(PostgresQueryBuilder);
        assert!(sql.contains("WHERE"));
    }
    
    #[test]
    fn test_in_ulids() {
        let ulids = vec![Ulid::new(), Ulid::new(), Ulid::new()];
        let query = Query::select()
            .from(Events)
            .and_where(Expr::col(EventCols::EventId).in_ulids(ulids))
            .to_owned();
            
        let (sql, _) = query.build(PostgresQueryBuilder);
        assert!(sql.contains("IN"));
    }
}