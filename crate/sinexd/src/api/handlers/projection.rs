//! `ProjectionReadinessView` assembly (sinex-68c.1, first slice of sinex-68c).
//!
//! Two sources feed the view:
//!
//! 1. Registered rows: `derivation.projection_registry` current state per
//!    `(projection_kind, scope_key)` (`ProjectionRegistryRepository::
//!    list_all_latest`). Any writer that starts calling
//!    `pool.projection_registry().begin_build(...)` /
//!    `mark_ready`/`mark_stale`/`mark_failed`/`mark_partial` shows up here
//!    automatically — no code changes needed in this module.
//! 2. The first-slice hardcoded entry: `email_mailbox`, sourced at READ TIME
//!    from `core.email_mailbox_projection` (via
//!    `EmailMailboxProjectionRepository::summarize_by_source`) rather than
//!    from a registry row, because the email ingestion automata don't yet
//!    write `derivation.projection_registry` rows on every batch — migrating
//!    them is out of scope for sinex-68c.1 (AC: "first slice may hardcode
//!    current read-time projections"). `email_mailbox` is a real,
//!    already-existing read model (`EMAIL_THREAD_PROJECTION_DERIVATION_ID` /
//!    `EMAIL_BODY_TEXT_PROJECTION_DERIVATION_ID` in
//!    `sinex_primitives::derivations`), not a synthetic placeholder.

use sinex_db::DbPoolExt;
use sinex_primitives::SinexError;
use sinex_primitives::derivation::{ProjectionFreshnessClass, ProjectionStatus};
use sinex_primitives::views::{ProjectionReadinessInput, ProjectionReadinessView};
use sqlx::PgPool;
use time::{Duration, OffsetDateTime};

type Result<T> = std::result::Result<T, SinexError>;

/// `email_mailbox` is not continuously ingested, so a build older than this
/// is treated as stale rather than ready. Deliberately generous (mailbox
/// import is a batch/periodic source, not a live stream) — tightening this
/// belongs to the full migration of email automata onto the registry, not
/// this read-time stand-in.
const EMAIL_MAILBOX_ACCEPTABLE_STALENESS: Duration = Duration::hours(24);

/// Provisional `projection_kind` for the email mailbox read model. No
/// automaton currently declares a `projection_kind` via
/// `DerivationOutputDeclaration` (grep confirms every `projection_kind:` in
/// the automata registry is still `None`) — this first slice establishes
/// the vocabulary token migration onto the registry will adopt.
const EMAIL_MAILBOX_PROJECTION_KIND: &str = "email_mailbox";

const EMAIL_MAILBOX_SEMANTICS_VERSION: &str = "v1";

const EMAIL_MAILBOX_SOURCE_ID: &str = "email.mailbox";

/// Assemble the full `ProjectionReadinessView`: registered
/// `derivation.projection_registry` rows plus the first-slice
/// read-time-computed `email_mailbox` entries.
pub async fn build_projection_readiness_view(pool: &PgPool) -> Result<ProjectionReadinessView> {
    let mut rows = registered_projection_rows(pool).await?;
    rows.extend(email_mailbox_readiness_rows(pool).await?);
    Ok(ProjectionReadinessView::from_rows(rows))
}

/// Flatten registry readiness into caveats for read surfaces whose payload
/// already has its own stable shape. The registry remains the sole readiness
/// authority; callers do not reimplement status checks.
pub async fn projection_readiness_caveats(
    pool: &PgPool,
) -> Result<Vec<sinex_primitives::views::CaveatView>> {
    Ok(build_projection_readiness_view(pool)
        .await?
        .projections
        .into_iter()
        .flat_map(|projection| projection.caveats)
        .collect())
}

async fn registered_projection_rows(pool: &PgPool) -> Result<Vec<ProjectionReadinessInput>> {
    let records = pool
        .projection_registry()
        .list_all_latest()
        .await
        .map_err(|error| {
            SinexError::database("Failed to load projection registry rows").with_std_error(&error)
        })?;

    Ok(records
        .into_iter()
        .map(|record| ProjectionReadinessInput {
            projection_kind: record.projection_kind,
            scope_key: record.scope_key,
            semantics_version: record.semantics_version,
            status: record
                .status
                .parse::<ProjectionStatus>()
                .unwrap_or(ProjectionStatus::Failed),
            freshness_class: record
                .freshness_class
                .parse::<ProjectionFreshnessClass>()
                .unwrap_or(ProjectionFreshnessClass::Manual),
            built_at: record.built_at,
            stale_reason: record.stale_reason,
            last_error: record.last_error,
            verification_command: record.verification_command,
            read_time_computed: false,
        })
        .collect())
}

async fn email_mailbox_readiness_rows(pool: &PgPool) -> Result<Vec<ProjectionReadinessInput>> {
    let summaries = pool
        .email_mailbox_projections()
        .summarize_by_source(EMAIL_MAILBOX_SOURCE_ID)
        .await
        .map_err(|error| {
            SinexError::database("Failed to summarize email mailbox projection")
                .with_std_error(&error)
        })?;

    let now = OffsetDateTime::now_utc();
    Ok(summaries
        .into_iter()
        .map(|summary| {
            let age = now - summary.last_observed_at;
            let (status, stale_reason) = if age > EMAIL_MAILBOX_ACCEPTABLE_STALENESS {
                (
                    ProjectionStatus::Stale,
                    Some(format!(
                        "last observed {} ago (mode {}), exceeds {}h acceptable staleness",
                        format_duration(age),
                        summary.mode_id,
                        EMAIL_MAILBOX_ACCEPTABLE_STALENESS.whole_hours(),
                    )),
                )
            } else {
                (ProjectionStatus::Ready, None)
            };
            ProjectionReadinessInput {
                projection_kind: EMAIL_MAILBOX_PROJECTION_KIND.to_string(),
                scope_key: summary.mode_id,
                semantics_version: EMAIL_MAILBOX_SEMANTICS_VERSION.to_string(),
                status,
                freshness_class: ProjectionFreshnessClass::Hours,
                built_at: Some(summary.last_observed_at.into()),
                stale_reason,
                last_error: None,
                verification_command: "xtask test -p sinexd -E 'test(projection_readiness_view)'"
                    .to_string(),
                read_time_computed: true,
            }
        })
        .collect())
}

fn format_duration(duration: Duration) -> String {
    let hours = duration.whole_hours();
    if hours >= 1 {
        format!("{hours}h")
    } else {
        format!("{}m", duration.whole_minutes().max(0))
    }
}

#[cfg(test)]
#[path = "projection_test.rs"]
mod tests;
