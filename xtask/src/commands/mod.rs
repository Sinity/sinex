pub mod analytics;
pub mod build;
pub mod check;
pub mod ci;
pub mod completions;
pub mod coverage;
pub mod deps;
pub mod docs;
pub mod doctor;
pub mod exercise;
pub mod fix;
pub mod fuzz;
pub mod git_stack;
pub mod gitops;
pub mod history;
pub mod infra;
pub mod jobs;
pub mod lint_forbidden;
pub mod privacy;
pub mod reset;
pub mod run;
pub mod snapshot;
pub mod status;
pub mod test;
pub mod verify;
pub mod vm;
pub mod work;

pub use analytics::AnalyticsCommand;
pub use build::BuildCommand;
pub use check::CheckCommand;
pub use deps::DepsCommand;
pub use docs::DocsCommand;
pub use doctor::DoctorCommand;
pub use exercise::ExerciseCommand;
pub use fix::FixCommand;
pub use git_stack::GitStackCommand;
pub use gitops::GitOpsCommand;
pub use infra::InfraCommand;
pub use jobs::JobsCommand;
pub use privacy::PrivacyCommand;
pub use reset::ResetCommand;
pub use run::RunCommand;
pub use status::StatusCommand;
pub use test::TestCommand;
pub use work::WorkCommand;

/// Format an `OffsetDateTime` for human-readable display: `"YYYY-MM-DD HH:MM"`.
///
/// Shared between `jobs.rs` and `history.rs` to avoid duplicate format string definitions.
pub(crate) fn format_display_time(time: &time::OffsetDateTime) -> String {
    use std::sync::LazyLock;
    static FMT: LazyLock<Vec<time::format_description::BorrowedFormatItem<'static>>> =
        LazyLock::new(|| {
            time::format_description::parse("[year]-[month]-[day] [hour]:[minute]")
                .expect("static format string is valid")
        });
    time.format(&*FMT).unwrap_or_else(|_| "-".into())
}

/// Format an RFC 3339 timestamp string for human-readable display: `"YYYY-MM-DD HH:MM"`.
///
/// Returns "-" if parsing fails. Used for timestamps stored as strings in history records
/// (e.g. `StageTiming`, `StageTrendPoint`, `FixSession`).
pub(crate) fn format_display_time_str(ts: &str) -> String {
    time::OffsetDateTime::parse(ts, &time::format_description::well_known::Rfc3339)
        .map_or_else(|_| "-".into(), |t| format_display_time(&t))
}
