// Tests assert path structure that is constructed by the helper under test.
#![allow(clippy::expect_used)]
use super::*;
use sinex_primitives::{
    AutomataDeploymentSurface, BrowserDeploymentSurface, DeploymentSurface,
    DesktopDeploymentSurface, DocumentDeploymentSurface, TerminalDeploymentSurface,
};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn enabled_automata_follow_descriptor_shape() -> TestResult<()> {
    let enabled = enabled_automata(Some(&DeploymentReadinessDescriptor {
        automata: AutomataDeploymentSurface {
            canonicalizer: true,
            health_aggregator: false,
            analytics_automaton: true,
            session_detector: false,
            ..Default::default()
        },
        ..Default::default()
    }));

    assert!(enabled.canonicalizer);
    assert!(!enabled.health_aggregator);
    assert!(enabled.analytics_automaton);
    assert!(!enabled.session_detector);
    Ok(())
}

#[sinex_test]
async fn enabled_source_surfaces_follow_descriptor_shape() -> TestResult<()> {
    let enabled = enabled_source_surfaces(Some(&DeploymentReadinessDescriptor {
        filesystem: DeploymentSurface {
            enabled: true,
            instances: Some(1),
        },
        terminal: TerminalDeploymentSurface {
            surface: DeploymentSurface {
                enabled: false,
                instances: Some(1),
            },
            ..Default::default()
        },
        browser: BrowserDeploymentSurface {
            surface: DeploymentSurface {
                enabled: true,
                instances: Some(1),
            },
            ..Default::default()
        },
        desktop: DesktopDeploymentSurface {
            surface: DeploymentSurface {
                enabled: true,
                instances: Some(1),
            },
            ..Default::default()
        },
        system: DeploymentSurface {
            enabled: false,
            instances: Some(1),
        },
        ..Default::default()
    }));

    assert!(enabled.filesystem);
    assert!(!enabled.terminal);
    assert!(enabled.browser);
    assert!(enabled.desktop);
    assert!(!enabled.system);
    Ok(())
}

#[sinex_test]
async fn build_document_smoke_path_uses_declared_root() -> TestResult<()> {
    let path = build_document_smoke_path(&DeploymentReadinessDescriptor {
        document: DocumentDeploymentSurface {
            allowed_roots: vec![PathBuf::from("/tmp/sinex-docs")],
            ..Default::default()
        },
        ..Default::default()
    })?;

    assert_eq!(
        path.parent().expect("parent"),
        PathBuf::from("/tmp/sinex-docs")
    );
    assert!(
        path.file_name()
            .expect("file name")
            .to_string_lossy()
            .starts_with(".sinex-verify-")
    );
    Ok(())
}

#[sinex_test]
async fn document_smoke_query_targets_the_specific_file_path() -> TestResult<()> {
    let query = document_smoke_query("/tmp/sinex-docs/.sinex-verify-abc.md")?;

    assert_eq!(query.sources.len(), 1);
    assert_eq!(query.sources[0].as_str(), DOCUMENT_SOURCE);
    assert_eq!(query.event_types.len(), 1);
    assert_eq!(query.event_types[0].as_str(), DOCUMENT_INGESTED_EVENT_TYPE);
    assert!(matches!(
        query.payload,
        Some(PayloadFilter::Contains { value })
            if value == json!({ "file_path": "/tmp/sinex-docs/.sinex-verify-abc.md" })
    ));
    Ok(())
}
