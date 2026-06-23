use super::EventRepository;
use crate::repositories::common::{DbResult, db_error};
use sinex_primitives::events::{EquivalenceKey, ScopeKey};
use sqlx::{Postgres, QueryBuilder};
use uuid::Uuid;

/// Relation kind for event replacements.
///
/// Describes how old events relate to their replacement events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplacementKind {
    /// 1:1 - one old event directly replaced by one new event.
    Superseded,
    /// many:1 - multiple old events collapsed into one new event.
    Collapsed,
    /// 1:many - one old event split into multiple new events.
    Split,
    /// No confident equivalence match; linked by operation only.
    Recomputed,
}

impl ReplacementKind {
    fn as_str(&self) -> &'static str {
        match self {
            ReplacementKind::Superseded => "superseded",
            ReplacementKind::Collapsed => "collapsed",
            ReplacementKind::Split => "split",
            ReplacementKind::Recomputed => "recomputed",
        }
    }
}

/// A single replacement relation to be recorded.
#[derive(Debug, Clone)]
pub struct ReplacementRecord {
    pub old_event_id: Uuid,
    pub new_event_id: Uuid,
    pub relation_kind: ReplacementKind,
    pub scope_key: Option<ScopeKey>,
    pub equivalence_key: Option<EquivalenceKey>,
}

impl EventRepository<'_> {
    /// Record event replacement relations for a replay operation.
    ///
    /// Inserts rows into `audit.event_replacements` linking archived (old) events
    /// to their replacement (new) events under a given operation.
    pub async fn record_replacements(
        &self,
        operation_id: Uuid,
        replacements: &[ReplacementRecord],
    ) -> DbResult<u64> {
        if replacements.is_empty() {
            return Ok(0);
        }

        let mut builder: QueryBuilder<Postgres> = QueryBuilder::new(
            "INSERT INTO audit.event_replacements \
             (old_event_id, new_event_id, operation_id, relation_kind, scope_key, equivalence_key) ",
        );

        builder.push_values(replacements, |mut b, r| {
            b.push_bind(r.old_event_id)
                .push_bind(r.new_event_id)
                .push_bind(operation_id)
                .push_bind(r.relation_kind.as_str())
                .push_bind(r.scope_key.as_deref())
                .push_bind(r.equivalence_key.as_deref());
        });

        let result = builder
            .build()
            .execute(self.pool)
            .await
            .map_err(|e| db_error(e, "record event replacements"))?;

        Ok(result.rows_affected())
    }

    /// Query replacement relations for a specific operation.
    pub async fn get_replacements_by_operation(
        &self,
        operation_id: Uuid,
    ) -> DbResult<Vec<(Uuid, Uuid, String, Option<String>, Option<String>)>> {
        let rows = sqlx::query_as::<_, (Uuid, Uuid, String, Option<String>, Option<String>)>(
            "SELECT old_event_id, new_event_id, relation_kind, scope_key, equivalence_key \
             FROM audit.event_replacements WHERE operation_id = $1 ORDER BY replaced_at",
        )
        .bind(operation_id)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get replacements by operation"))?;
        Ok(rows)
    }

    /// Query what replaced a specific archived event.
    pub async fn get_replacements_for_event(
        &self,
        old_event_id: Uuid,
    ) -> DbResult<Vec<(Uuid, String, Uuid)>> {
        let rows = sqlx::query_as::<_, (Uuid, String, Uuid)>(
            "SELECT new_event_id, relation_kind, operation_id \
             FROM audit.event_replacements WHERE old_event_id = $1 ORDER BY replaced_at",
        )
        .bind(old_event_id)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get replacements for event"))?;
        Ok(rows)
    }
}
