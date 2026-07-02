// Small inline tests are justified here because they exercise private
// helper behavior without widening the preflight API surface.
use super::{
    collect_hyprland_runtime_sockets, document_root_error_is_blocking,
    parse_systemd_version_line,
};
use crate::runtime::SinexError;
use std::fs;
use std::io;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn collect_hyprland_runtime_sockets_reports_entry_failures() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let hypr_dir = temp.path().join("hypr");
    fs::create_dir_all(&hypr_dir)?;

    let error = collect_hyprland_runtime_sockets(
        &hypr_dir,
        vec![Err::<std::path::PathBuf, _>(io::Error::other("boom"))],
    )
    .expect_err("entry failure should be reported");

    assert!(error.contains("Failed to inspect Hyprland runtime directory entry"));
    assert!(error.contains("boom"));
    Ok(())
}

#[sinex_test]
async fn collect_hyprland_runtime_sockets_returns_present_event_socket_only() -> TestResult<()>
{
    let temp = tempfile::tempdir()?;
    let hypr_dir = temp.path().join("hypr");
    let instance_a = hypr_dir.join("instance-a");
    let instance_b = hypr_dir.join("instance-b");
    fs::create_dir_all(&instance_a)?;
    fs::create_dir_all(&instance_b)?;
    fs::write(instance_a.join(".socket2.sock"), [])?;

    let sockets = collect_hyprland_runtime_sockets(
        &hypr_dir,
        vec![
            Ok::<std::path::PathBuf, io::Error>(instance_a.clone()),
            Ok::<std::path::PathBuf, io::Error>(instance_b.clone()),
        ],
    )
    .map_err(SinexError::processing)?;

    assert_eq!(sockets, vec![instance_a.join(".socket2.sock")]);
    Ok(())
}

#[sinex_test]
async fn parse_systemd_version_line_rejects_empty_output() -> TestResult<()> {
    let error = parse_systemd_version_line(b"\n\n")
        .expect_err("empty systemctl version output must fail honestly");

    assert!(
        error
            .to_string()
            .contains("systemctl --version returned empty output")
    );
    Ok(())
}

#[sinex_test]
async fn parse_systemd_version_line_uses_first_non_empty_line() -> TestResult<()> {
    let version_line = parse_systemd_version_line(b"\n systemd 256 (256.7)\n+PAM\n")?;

    assert_eq!(version_line, " systemd 256 (256.7)");
    Ok(())
}

#[sinex_test]
async fn document_root_permission_denied_is_advisory() -> TestResult<()> {
    let permission_denied = SinexError::processing("denied")
        .with_context("document_root_probe", "permission_denied");
    let missing =
        SinexError::processing("missing").with_context("document_root_probe", "missing");

    assert!(
        !document_root_error_is_blocking(&permission_denied),
        "permission-denied roots should not block preflight if they otherwise exist"
    );
    assert!(
        document_root_error_is_blocking(&missing),
        "missing roots must still block preflight"
    );
    Ok(())
}
