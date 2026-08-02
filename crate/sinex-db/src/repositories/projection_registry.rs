//! Repository for `derivation.projection_registry` (sinex-68c.1): the
//! freshness/status tracker for rebuildable read-model ("projection") state.
//!
//! A projection instance is identified by `(projection_kind, scope_key,
//! semantics_version)`. There is no DB-level uniqueness on that triple —
//! each transition (`begin_build`, `mark_ready`, `mark_stale`, ...) either
//! inserts a fresh row (`begin_build`/`mark_absent`) or updates the row it
//! was handed the id of (`mark_ready`/`mark_stale`/`mark_failed`/
//! `mark_partial`), so the "current" state of a scope is the most recently
//! `updated_at` row for that `(projection_kind, scope_key)` pair —
//! `find_latest`/`list_latest_by_kind`/`list_all_latest` all read it that
//! way. This mirrors the epoch/lane append style used elsewhere in the
//! derivation control plane (`derivation.epochs`, `derivation.lanes`)
//! rather than a single mutated row per scope, so a scope's build history
//! stays queryable.
//!
//! See `sinex_primitives::derivation::{ProjectionStatus,
//! ProjectionFreshnessClass}` for the typed status/freshness vocabulary
//! this repository's raw-string columns mirror, and
//! `sinex_primitives::views::ProjectionReadinessView` for the read-surface
//! that renders these rows.

use super::common::{DbResult, Repository, db_error};
use crate::schema::records::DerivationProjectionRegistryRecord;
use sinex_primitives::derivation::{ProjectionFreshnessClass, ProjectionStatus};
use sinex_primitives::temporal::Timestamp;
use sqlx::PgPool;
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

pub struct ProjectionRegistryRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> Repository<'a> for ProjectionRegistryRepository<'a> {
    fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    fn pool(&self) -> &'a PgPool {
        self.pool
    }
}

/// Coverage window for a projection build: the input span the build is
/// scoped to. Mirrors `derivation.projection_registry.coverage_window`
/// (`tstzrange`).
#[derive(Debug, Clone, Copy)]
pub struct ProjectionCoverageWindow {
    pub start: OffsetDateTime,
    /// `None` = open-ended (still-growing live coverage).
    pub end: Option<OffsetDateTime>,
}

/// What a caller declares when starting a build or registering an absent
/// projection — the fields that don't change across a row's later status
/// transitions.
#[derive(Debug, Clone)]
pub struct ProjectionRegistrationInput<'a> {
    pub projection_kind: &'a str,
    pub scope_key: &'a str,
    pub semantics_version: &'a str,
    pub input_fingerprint: &'a str,
    pub coverage_window: ProjectionCoverageWindow,
    pub freshness_class: ProjectionFreshnessClass,
    pub acceptable_staleness_secs: i64,
    pub verification_command: &'a str,
}

impl ProjectionRegistryRepository<'_> {
    /// Start a build: inserts a new row with `status = 'building'` (no
    /// `built_at`, no `stale_reason` — neither is required by the `building`
    /// state's CHECK constraints). Returns the new row's id, to be passed to
    /// `mark_ready`/`mark_stale`/`mark_failed`/`mark_partial` once the build
    /// concludes.
    pub async fn begin_build(&self, input: &ProjectionRegistrationInput<'_>) -> DbResult<Uuid> {
        self.insert_row(input, ProjectionStatus::Building).await
    }

    /// Register a projection as expected-but-never-built. Distinct from
    /// simply having no row: a consumer that wants to warn about a projection
    /// nobody has started building yet needs an explicit `absent` row to
    /// query, not merely absence-of-evidence.
    pub async fn mark_absent(&self, input: &ProjectionRegistrationInput<'_>) -> DbResult<Uuid> {
        self.insert_row(input, ProjectionStatus::Absent).await
    }

    async fn insert_row(
        &self,
        input: &ProjectionRegistrationInput<'_>,
        status: ProjectionStatus,
    ) -> DbResult<Uuid> {
        // sinex-68c.4: a semantics-version bump is itself an invalidation
        // trigger. If the current row for this (projection_kind, scope_key)
        // was built under a different semantics_version, it must stop being
        // served as ready the moment we know a new build is expected under
        // the bumped version — not only once that new build completes. Do
        // this before inserting the new row so the window where both an
        // old-semantics "ready" row and a new "building" row could coexist
        // as ambiguous "current state" never opens.
        self.stale_superseded_semantics(input).await?;

        let row = sqlx::query!(
            r#"
            INSERT INTO derivation.projection_registry (
                id, projection_kind, scope_key, semantics_version, input_fingerprint,
                coverage_window, status, freshness_class, acceptable_staleness,
                verification_command
            ) VALUES (
                uuidv7(), $1, $2, $3, $4,
                tstzrange($5, $6), $7, $8, ($9 * interval '1 second'),
                $10
            )
            RETURNING id
            "#,
            input.projection_kind,
            input.scope_key,
            input.semantics_version,
            input.input_fingerprint,
            input.coverage_window.start,
            input.coverage_window.end,
            status.as_str(),
            input.freshness_class.as_str(),
            input.acceptable_staleness_secs as f64,
            input.verification_command,
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "insert projection registry row"))?;

        Ok(row.id)
    }

    /// Stale the current row for `(projection_kind, scope_key)` if its
    /// `semantics_version` differs from `input.semantics_version` and it is
    /// still in a status a read surface could mistake for current
    /// (`ready`/`partial`/`building`). No-op if there is no current row or
    /// the semantics version has not changed. See `insert_row`'s call site
    /// for why this runs before the new row is inserted.
    async fn stale_superseded_semantics(&self, input: &ProjectionRegistrationInput<'_>) -> DbResult<u64> {
        let result = sqlx::query!(
            r#"
            WITH latest AS (
                SELECT id, semantics_version, status
                FROM derivation.projection_registry
                WHERE projection_kind = $1 AND scope_key = $2
                ORDER BY updated_at DESC
                LIMIT 1
            )
            UPDATE derivation.projection_registry r
            SET status = 'stale',
                stale_reason = 'semantics_version bumped from ' || latest.semantics_version
                    || ' to ' || $3,
                updated_at = now()
            FROM latest
            WHERE r.id = latest.id
              AND latest.semantics_version <> $3
              AND latest.status IN ('ready', 'partial', 'building')
            "#,
            input.projection_kind,
            input.scope_key,
            input.semantics_version,
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "stale projection registry row on semantics version bump"))?;
        Ok(result.rows_affected())
    }

    /// Stale every current (`ready`/`partial`/`building`) registry row whose
    /// `scope_key` matches, across all `projection_kind`s.
    ///
    /// Used by replay/archive scope invalidation (sinex-68c.4): once the
    /// events a scope was built from have been archived and are being
    /// recomputed, any projection instance keyed to that scope can no
    /// longer be trusted at its current `built_at` — mark it stale so
    /// `ProjectionReadinessView` stops claiming readiness for it until a
    /// fresh build lands. "Current" here means the most-recently-updated
    /// row per `(projection_kind, scope_key)` — the same row
    /// `find_latest`/`list_all_latest` treat as authoritative; older
    /// superseded rows for the same scope are left untouched since they are
    /// not surfaced as current state anyway.
    pub async fn mark_stale_by_scope_key(&self, scope_key: &str, reason: &str) -> DbResult<u64> {
        let result = sqlx::query!(
            r#"
            WITH latest AS (
                SELECT DISTINCT ON (projection_kind, scope_key) id
                FROM derivation.projection_registry
                WHERE scope_key = $1
                ORDER BY projection_kind, scope_key, updated_at DESC
            )
            UPDATE derivation.projection_registry r
            SET status = 'stale', stale_reason = $2, updated_at = now()
            FROM latest
            WHERE r.id = latest.id AND r.status IN ('ready', 'partial', 'building')
            "#,
            scope_key,
            reason,
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "mark projection registry rows stale by scope key"))?;
        Ok(result.rows_affected())
    }

    /// Stale every current (`ready`/`partial`/`building`) registry row,
    /// across every `projection_kind` and `scope_key`.
    ///
    /// Used by redaction/privacy policy changes (sinex-68c.4): there is no
    /// per-projection dependency table yet tracking which projections read
    /// which fields, so a policy mutation (a new/removed/toggled rule, a
    /// changed field scope, a swapped recognizer backend) conservatively
    /// invalidates every tracked projection rather than silently continuing
    /// to serve pre-change content as ready. See the module doc for the
    /// `projection_dependencies` follow-up that would let this narrow to
    /// only the projections actually reading redacted fields.
    pub async fn mark_all_stale(&self, reason: &str) -> DbResult<u64> {
        let result = sqlx::query!(
            r#"
            WITH latest AS (
                SELECT DISTINCT ON (projection_kind, scope_key) id
                FROM derivation.projection_registry
                ORDER BY projection_kind, scope_key, updated_at DESC
            )
            UPDATE derivation.projection_registry r
            SET status = 'stale', stale_reason = $1, updated_at = now()
            FROM latest
            WHERE r.id = latest.id AND r.status IN ('ready', 'partial', 'building')
            "#,
            reason,
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "mark all projection registry rows stale"))?;
        Ok(result.rows_affected())
    }

    /// Transition `id` to `ready`: sets `built_at = now()`, clears
    /// `stale_reason`/`last_error`, records `source_counts`.
    pub async fn mark_ready(
        &self,
        id: Uuid,
        source_counts: serde_json::Value,
    ) -> DbResult<DerivationProjectionRegistryRecord> {
        sqlx::query_as!(
            DerivationProjectionRegistryRecord,
            r#"
            UPDATE derivation.projection_registry
            SET status = 'ready',
                built_at = now(),
                source_counts = $2,
                stale_reason = NULL,
                last_error = NULL,
                updated_at = now()
            WHERE id = $1
            RETURNING
                id, projection_kind, scope_key, semantics_version, input_fingerprint,
                status, freshness_class, built_at as "built_at: Timestamp",
                source_counts, stale_reason,
                last_error, verification_command, updated_at as "updated_at: Timestamp"
            "#,
            id,
            source_counts,
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "mark projection registry row ready"))
    }

    /// Transition `id` to `stale`: an existing build is no longer trusted
    /// (acceptable staleness exceeded, or an input dependency changed).
    pub async fn mark_stale(
        &self,
        id: Uuid,
        reason: &str,
    ) -> DbResult<DerivationProjectionRegistryRecord> {
        sqlx::query_as!(
            DerivationProjectionRegistryRecord,
            r#"
            UPDATE derivation.projection_registry
            SET status = 'stale',
                stale_reason = $2,
                updated_at = now()
            WHERE id = $1
            RETURNING
                id, projection_kind, scope_key, semantics_version, input_fingerprint,
                status, freshness_class, built_at as "built_at: Timestamp",
                source_counts, stale_reason,
                last_error, verification_command, updated_at as "updated_at: Timestamp"
            "#,
            id,
            reason,
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "mark projection registry row stale"))
    }

    /// Transition `id` to `failed`: the build attempt errored out. Sets both
    /// `last_error` (the operational detail) and `stale_reason` (required by
    /// the DB CHECK constraint on any of stale/failed/partial) to `reason`.
    pub async fn mark_failed(
        &self,
        id: Uuid,
        reason: &str,
    ) -> DbResult<DerivationProjectionRegistryRecord> {
        sqlx::query_as!(
            DerivationProjectionRegistryRecord,
            r#"
            UPDATE derivation.projection_registry
            SET status = 'failed',
                last_error = $2,
                stale_reason = $2,
                updated_at = now()
            WHERE id = $1
            RETURNING
                id, projection_kind, scope_key, semantics_version, input_fingerprint,
                status, freshness_class, built_at as "built_at: Timestamp",
                source_counts, stale_reason,
                last_error, verification_command, updated_at as "updated_at: Timestamp"
            "#,
            id,
            reason,
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "mark projection registry row failed"))
    }

    /// Transition `id` to `partial`: the build covered only part of the
    /// intended scope/coverage window.
    pub async fn mark_partial(
        &self,
        id: Uuid,
        reason: &str,
        source_counts: serde_json::Value,
    ) -> DbResult<DerivationProjectionRegistryRecord> {
        sqlx::query_as!(
            DerivationProjectionRegistryRecord,
            r#"
            UPDATE derivation.projection_registry
            SET status = 'partial',
                built_at = now(),
                source_counts = $3,
                stale_reason = $2,
                updated_at = now()
            WHERE id = $1
            RETURNING
                id, projection_kind, scope_key, semantics_version, input_fingerprint,
                status, freshness_class, built_at as "built_at: Timestamp",
                source_counts, stale_reason,
                last_error, verification_command, updated_at as "updated_at: Timestamp"
            "#,
            id,
            reason,
            source_counts,
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "mark projection registry row partial"))
    }

    /// The most recently updated row for `(projection_kind, scope_key)`,
    /// across all `semantics_version`s — this is "current state" for a
    /// scope.
    pub async fn find_latest(
        &self,
        projection_kind: &str,
        scope_key: &str,
    ) -> DbResult<Option<DerivationProjectionRegistryRecord>> {
        sqlx::query_as!(
            DerivationProjectionRegistryRecord,
            r#"
            SELECT
                id, projection_kind, scope_key, semantics_version, input_fingerprint,
                status, freshness_class, built_at as "built_at: Timestamp",
                source_counts, stale_reason,
                last_error, verification_command, updated_at as "updated_at: Timestamp"
            FROM derivation.projection_registry
            WHERE projection_kind = $1 AND scope_key = $2
            ORDER BY updated_at DESC
            LIMIT 1
            "#,
            projection_kind,
            scope_key,
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "find latest projection registry row"))
    }

    /// The most recently updated row per distinct `(projection_kind,
    /// scope_key)` across the whole registry — the "current state of every
    /// tracked projection scope" read `ProjectionReadinessView` renders.
    pub async fn list_all_latest(&self) -> DbResult<Vec<DerivationProjectionRegistryRecord>> {
        sqlx::query_as!(
            DerivationProjectionRegistryRecord,
            r#"
            SELECT DISTINCT ON (projection_kind, scope_key)
                id, projection_kind, scope_key, semantics_version, input_fingerprint,
                status, freshness_class, built_at as "built_at: Timestamp",
                source_counts, stale_reason,
                last_error, verification_command, updated_at as "updated_at: Timestamp"
            FROM derivation.projection_registry
            ORDER BY projection_kind, scope_key, updated_at DESC
            "#,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "list latest projection registry rows"))
    }
}
