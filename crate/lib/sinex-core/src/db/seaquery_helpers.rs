//! SeaQuery ULID helper traits and utilities
//!
//! This module extends SeaQuery expressions with ULID-aware methods,
//! automatically handling the ULID to UUID conversion required by PostgreSQL.

use crate::query_helpers::ulid_to_uuid;
use crate::types::ulid::Ulid;
use sea_query::{Expr, SimpleExpr};
use sqlx::types::Uuid as SqlxUuid;

/// Extension trait for SeaQuery expressions to work seamlessly with ULIDs
pub trait SeaQueryUlidExt {
    /// Compare equality with a ULID
    fn eq_ulid(self, ulid: Ulid) -> SimpleExpr;

    /// Check if value is in a list of ULIDs
    fn in_ulids<I: IntoIterator<Item = Ulid>>(self, ulids: I) -> SimpleExpr;

    /// Check if value is not in a list of ULIDs
    fn not_in_ulids<I: IntoIterator<Item = Ulid>>(self, ulids: I) -> SimpleExpr;

    /// Compare with optional ULID (generates IS NULL if None)
    fn eq_ulid_opt(self, ulid: Option<Ulid>) -> SimpleExpr;

    /// Greater than comparison with ULID
    fn gt_ulid(self, ulid: Ulid) -> SimpleExpr;

    /// Greater than or equal comparison with ULID
    fn gte_ulid(self, ulid: Ulid) -> SimpleExpr;

    /// Less than comparison with ULID
    fn lt_ulid(self, ulid: Ulid) -> SimpleExpr;

    /// Less than or equal comparison with ULID
    fn lte_ulid(self, ulid: Ulid) -> SimpleExpr;
}

impl SeaQueryUlidExt for Expr {
    #[inline]
    fn eq_ulid(self, ulid: Ulid) -> SimpleExpr {
        self.eq(ulid_to_uuid(ulid))
    }

    fn in_ulids<I: IntoIterator<Item = Ulid>>(self, ulids: I) -> SimpleExpr {
        let uuids: Vec<SqlxUuid> = ulids.into_iter().map(ulid_to_uuid).collect();
        self.is_in(uuids)
    }

    fn not_in_ulids<I: IntoIterator<Item = Ulid>>(self, ulids: I) -> SimpleExpr {
        let uuids: Vec<SqlxUuid> = ulids.into_iter().map(ulid_to_uuid).collect();
        self.is_not_in(uuids)
    }

    fn eq_ulid_opt(self, ulid: Option<Ulid>) -> SimpleExpr {
        match ulid {
            Some(id) => self.eq(ulid_to_uuid(id)),
            None => self.is_null(),
        }
    }

    #[inline]
    fn gt_ulid(self, ulid: Ulid) -> SimpleExpr {
        self.gt(ulid_to_uuid(ulid))
    }

    #[inline]
    fn gte_ulid(self, ulid: Ulid) -> SimpleExpr {
        self.gte(ulid_to_uuid(ulid))
    }

    #[inline]
    fn lt_ulid(self, ulid: Ulid) -> SimpleExpr {
        self.lt(ulid_to_uuid(ulid))
    }

    #[inline]
    fn lte_ulid(self, ulid: Ulid) -> SimpleExpr {
        self.lte(ulid_to_uuid(ulid))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::{Alias, PostgresQueryBuilder, Query};

    #[sinex_test]
    fn test_eq_ulid() {
        let ulid = Ulid::new();
        let query = Query::select()
            .from(Alias::new("events"))
            .and_where(Expr::col(Alias::new("event_id")).eq_ulid(ulid))
            .to_owned();

        let (sql, _) = query.build(PostgresQueryBuilder);
        assert!(sql.contains("WHERE"));
    }

    #[sinex_test]
    fn test_in_ulids() {
        let ulids = vec![Ulid::new(), Ulid::new(), Ulid::new()];
        let query = Query::select()
            .from(Alias::new("events"))
            .and_where(Expr::col(Alias::new("event_id")).in_ulids(ulids))
            .to_owned();

        let (sql, _) = query.build(PostgresQueryBuilder);
        assert!(sql.contains("IN"));
    }
}
