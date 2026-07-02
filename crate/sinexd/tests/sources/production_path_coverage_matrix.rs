use std::collections::{BTreeMap, BTreeSet};

use crate::AdapterKind;
use crate::obligations::drain;
use sinex_primitives::parser::SourceId;
use sinexd::sources::dispatch::find_parser_factory;
use sinexd::sources::registry::SourceContractRegistry;
use sinexd::sources::source_factory::registered_source_factory_ids;
use xtask::sandbox::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SmokeCoverage {
    BinaryPath,
    ObligationHarness,
    MonitorHarness,
    NoopHarness,
}

#[derive(Debug, Clone, Copy)]
struct SmokeMatrixEntry {
    source_id: &'static str,
    coverage: SmokeCoverage,
    evidence: &'static str,
    blocker_issue: Option<&'static str>,
}

const SMOKE_MATRIX: &[SmokeMatrixEntry] = &[
    entry(
        "ai-session-chatgpt",
        SmokeCoverage::ObligationHarness,
        "production_path/ai_session.rs",
    ),
    entry(
        "ai-session-claude",
        SmokeCoverage::ObligationHarness,
        "production_path/ai_session.rs",
    ),
    entry(
        "browser.history",
        SmokeCoverage::ObligationHarness,
        "production_path/browser.rs",
    ),
    entry(
        "desktop.activitywatch",
        SmokeCoverage::ObligationHarness,
        "production_path/desktop.rs",
    ),
    entry(
        "desktop.clipboard",
        SmokeCoverage::ObligationHarness,
        "production_path/desktop.rs",
    ),
    entry(
        "desktop.notification",
        SmokeCoverage::ObligationHarness,
        "production_path/system.rs test_desktop_notification_initial_ingestion",
    ),
    entry(
        "desktop.notification.action",
        SmokeCoverage::ObligationHarness,
        "production_path/system.rs test_desktop_notification_action_initial_ingestion",
    ),
    entry(
        "desktop.notification.closed",
        SmokeCoverage::ObligationHarness,
        "production_path/system.rs test_desktop_notification_closed_initial_ingestion",
    ),
    entry(
        "desktop.window-manager",
        SmokeCoverage::ObligationHarness,
        "production_path/desktop.rs unix socket fixture",
    ),
    entry(
        "docs-library-index",
        SmokeCoverage::ObligationHarness,
        "production_path/document.rs",
    ),
    entry(
        "document.staging",
        SmokeCoverage::ObligationHarness,
        "production_path/document.rs",
    ),
    entry(
        "email.mailbox",
        SmokeCoverage::ObligationHarness,
        "production_path/email.rs staged RFC822, Maildir, and MBOX fixtures",
    ),
    entry(
        "facebook-messenger-thread",
        SmokeCoverage::ObligationHarness,
        "production_path/export_parsers.rs",
    ),
    entry(
        "fs",
        SmokeCoverage::ObligationHarness,
        "production_path/fs.rs",
    ),
    entry(
        "git-commit-history",
        SmokeCoverage::ObligationHarness,
        "production_path/path_sensitive.rs",
    ),
    entry(
        "hledger-journal",
        SmokeCoverage::ObligationHarness,
        "production_path/export_parsers.rs",
    ),
    entry(
        "knowledgebase-vault",
        SmokeCoverage::ObligationHarness,
        "production_path/path_sensitive.rs",
    ),
    entry(
        "media.audio-transcript",
        SmokeCoverage::ObligationHarness,
        "production_path/media.rs",
    ),
    entry(
        "media.screen-ocr",
        SmokeCoverage::ObligationHarness,
        "production_path/media.rs",
    ),
    entry(
        "noop",
        SmokeCoverage::NoopHarness,
        "noop.rs noop_source_reports_zero_work",
    ),
    entry(
        "raindrop-bookmarks",
        SmokeCoverage::ObligationHarness,
        "production_path/export_parsers.rs",
    ),
    entry(
        "reddit-gdpr-comments",
        SmokeCoverage::ObligationHarness,
        "production_path/social_exports.rs",
    ),
    entry(
        "reddit-gdpr-posts",
        SmokeCoverage::ObligationHarness,
        "production_path/social_exports.rs",
    ),
    entry(
        "sleep-merged-summary",
        SmokeCoverage::ObligationHarness,
        "production_path/health_exports.rs",
    ),
    entry(
        "spotify-extended-history",
        SmokeCoverage::ObligationHarness,
        "production_path/export_parsers.rs",
    ),
    entry(
        "system.dbus",
        SmokeCoverage::ObligationHarness,
        "production_path/system.rs",
    ),
    entry(
        "system.journald",
        SmokeCoverage::ObligationHarness,
        "production_path/system.rs",
    ),
    entry(
        "system.monitor",
        SmokeCoverage::MonitorHarness,
        "monitor_driver.rs monitor_fire_once_opens_material_and_emits_event",
    ),
    entry(
        "system.systemd",
        SmokeCoverage::ObligationHarness,
        "production_path/system.rs",
    ),
    entry(
        "system.udev",
        SmokeCoverage::ObligationHarness,
        "production_path/system.rs",
    ),
    entry(
        "terminal.asciinema",
        SmokeCoverage::ObligationHarness,
        "production_path/terminal.rs terminal_asciinema_session_json_ingestion",
    ),
    entry(
        "terminal.atuin-history",
        SmokeCoverage::ObligationHarness,
        "production_path/terminal.rs",
    ),
    entry(
        "terminal.bash-history",
        SmokeCoverage::ObligationHarness,
        "production_path/terminal.rs",
    ),
    entry(
        "terminal.fish-history",
        SmokeCoverage::ObligationHarness,
        "production_path/terminal.rs",
    ),
    entry(
        "terminal.kitty-osc-live",
        SmokeCoverage::ObligationHarness,
        "production_path/terminal.rs terminal_kitty_osc_live_socket_adapter_parses_command_frame",
    ),
    entry(
        "terminal.monitor",
        SmokeCoverage::MonitorHarness,
        "monitor_driver.rs monitor_fire_once_opens_material_and_emits_event",
    ),
    entry(
        "terminal.text-history",
        SmokeCoverage::ObligationHarness,
        "production_path/terminal.rs",
    ),
    entry(
        "terminal.zsh-history",
        SmokeCoverage::ObligationHarness,
        "production_path/terminal.rs",
    ),
    entry(
        "weechat",
        SmokeCoverage::BinaryPath,
        "production_path/obligations/initial_ingestion.rs binary_path",
    ),
    entry(
        "wykop-entries",
        SmokeCoverage::ObligationHarness,
        "production_path/social_exports.rs",
    ),
    entry(
        "wykop-entry-comments",
        SmokeCoverage::ObligationHarness,
        "production_path/social_exports.rs",
    ),
];

const fn entry(
    source_id: &'static str,
    coverage: SmokeCoverage,
    evidence: &'static str,
) -> SmokeMatrixEntry {
    SmokeMatrixEntry {
        source_id,
        coverage,
        evidence,
        blocker_issue: None,
    }
}

#[sinex_test]
async fn source_driver_host_smoke_matrix_covers_every_registered_factory() -> TestResult<()> {
    let factory_ids: BTreeSet<String> = registered_source_factory_ids()
        .into_iter()
        .map(|id| id.as_str().to_string())
        .collect();
    let matrix_ids: BTreeSet<String> = SMOKE_MATRIX
        .iter()
        .map(|entry| entry.source_id.to_string())
        .collect();

    let missing: Vec<&String> = factory_ids.difference(&matrix_ids).collect();
    let stale: Vec<&String> = matrix_ids.difference(&factory_ids).collect();

    assert!(
        missing.is_empty(),
        "source host source factories missing smoke-matrix entries: {missing:#?}"
    );
    assert!(
        stale.is_empty(),
        "smoke-matrix entries without a registered source factory: {stale:#?}"
    );

    Ok(())
}

#[sinex_test]
async fn source_driver_host_smoke_matrix_entries_are_actionable() -> TestResult<()> {
    let registry = SourceContractRegistry::from_inventory();
    let mut seen = BTreeMap::new();

    for entry in SMOKE_MATRIX {
        assert!(
            !entry.evidence.trim().is_empty(),
            "{} must cite concrete smoke or fixture evidence",
            entry.source_id
        );

        if let Some(previous) = seen.insert(entry.source_id, entry.evidence) {
            panic!(
                "duplicate smoke-matrix entry for {}: {previous} and {}",
                entry.source_id, entry.evidence
            );
        }

        let id = SourceId::new(entry.source_id)?;
        let descriptor = registry
            .find(&id)
            .unwrap_or_else(|| panic!("{} descriptor must be registered", entry.source_id));
        assert_eq!(descriptor.id, entry.source_id);

        if matches!(
            entry.coverage,
            SmokeCoverage::BinaryPath | SmokeCoverage::ObligationHarness
        ) {
            assert!(
                find_parser_factory(&id).is_some(),
                "{} must have a parser factory for {:?} coverage",
                entry.source_id,
                entry.coverage
            );
        }

        if matches!(entry.coverage, SmokeCoverage::MonitorHarness) {
            assert!(
                find_parser_factory(&id).is_none(),
                "{} monitor smoke entry must remain parserless",
                entry.source_id
            );
        }

        if matches!(entry.coverage, SmokeCoverage::NoopHarness) {
            assert!(
                find_parser_factory(&id).is_none(),
                "{} noop smoke entry must remain parserless",
                entry.source_id
            );
            assert!(
                descriptor.event_types.is_empty(),
                "{} noop smoke entry must remain eventless",
                entry.source_id
            );
        }

        if let Some(blocker_issue) = entry.blocker_issue {
            assert!(
                blocker_issue.starts_with('#'),
                "{} blocker must cite a GitHub issue number",
                entry.source_id
            );
            assert!(
                entry.evidence.contains(blocker_issue),
                "{} evidence must mention its blocker issue {blocker_issue}",
                entry.source_id
            );
        }
    }

    Ok(())
}

#[sinex_test]
async fn source_driver_host_drain_obligation_covers_shared_controller() -> TestResult<()> {
    drain::run("source host.shared-drain", AdapterKind::StaticFile, b"")
        .await
        .map_err(SinexError::processing)?;

    Ok(())
}
