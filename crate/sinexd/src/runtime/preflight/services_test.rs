// Small inline tests are justified here because they exercise private
// preflight helpers without widening the service-verification API surface.
use super::{SystemdServiceDetails, discover_unit_files_in_path, parse_systemd_watchdog_usec};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn systemd_service_details_reject_invalid_watchdog_usec() -> TestResult<()> {
    let error = SystemdServiceDetails::from_show_output(
        "ActiveState=active\nSubState=running\nLoadState=loaded\nWatchdogUSec=not-a-number\n",
    )
    .expect_err("invalid WatchdogUSec should fail honestly");

    assert!(error.to_string().contains("WatchdogUSec"));
    assert!(error.to_string().contains("not-a-number"));
    Ok(())
}

#[sinex_test]
async fn systemd_service_details_parse_watchdog_usec_when_valid() -> TestResult<()> {
    let details = SystemdServiceDetails::from_show_output(
        "ActiveState=active\nSubState=running\nLoadState=loaded\nType=notify\nNotifyAccess=main\nWatchdogUSec=60000000\n",
    )?;

    assert_eq!(details.watchdog_usec, Some(60_000_000));
    assert_eq!(details.unit_type.as_deref(), Some("notify"));
    Ok(())
}

#[sinex_test]
async fn systemd_service_details_parse_watchdog_usec_human_duration() -> TestResult<()> {
    let details = SystemdServiceDetails::from_show_output(
        "ActiveState=active\nSubState=running\nLoadState=loaded\nType=notify\nNotifyAccess=main\nWatchdogUSec=3min\n",
    )?;

    assert_eq!(details.watchdog_usec, Some(180_000_000));
    Ok(())
}

#[sinex_test]
async fn systemd_service_details_treat_infinity_watchdog_as_inactive_placeholder()
-> TestResult<()> {
    let details = SystemdServiceDetails::from_show_output(
        "ActiveState=inactive\nSubState=dead\nLoadState=loaded\nType=notify\nNotifyAccess=main\nWatchdogUSec=infinity\n",
    )?;

    assert_eq!(details.watchdog_usec, None);
    assert!(details.notify_contract_violations().is_empty());
    Ok(())
}

#[sinex_test]
async fn systemd_service_details_reject_missing_required_states() -> TestResult<()> {
    let error = SystemdServiceDetails::from_show_output(
        "ActiveState=active\nType=notify\nNotifyAccess=main\nWatchdogUSec=60000000\n",
    )
    .expect_err("missing SubState/LoadState should fail honestly");

    assert!(error.to_string().contains("missing required field"));
    assert!(error.to_string().contains("SubState"));
    Ok(())
}

#[sinex_test]
async fn discover_unit_files_in_path_reports_non_directory_paths() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let bogus_path = temp.path().join("not-a-directory");
    std::fs::write(&bogus_path, "x")?;

    let error = discover_unit_files_in_path(bogus_path.to_str().expect("utf8 path"))
        .await
        .expect_err("non-directory path should fail honestly");

    assert!(
        error
            .to_string()
            .contains("Failed to inspect systemd unit directory")
    );
    Ok(())
}

#[sinex_test]
async fn discover_unit_files_in_path_finds_only_sinex_service_units() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    std::fs::write(temp.path().join("sinexd.service"), [])?;
    std::fs::write(temp.path().join("postgresql.service"), [])?;
    std::fs::create_dir(temp.path().join("sinex-dir.service"))?;

    let mut found =
        discover_unit_files_in_path(temp.path().to_str().expect("utf8 path")).await?;
    found.sort();

    assert_eq!(
        found,
        vec![format!("{}/sinexd.service", temp.path().display())]
    );
    Ok(())
}

#[sinex_test]
async fn parse_systemd_watchdog_usec_rejects_unknown_units() -> TestResult<()> {
    let error =
        parse_systemd_watchdog_usec("forever").expect_err("unknown watchdog unit should fail");

    assert!(error.to_string().contains("WatchdogUSec"));
    assert!(error.to_string().contains("forever"));
    Ok(())
}

#[cfg(unix)]
#[sinex_test]
async fn discover_unit_files_in_path_rejects_non_utf8_entry_names() -> TestResult<()> {
    use std::os::unix::ffi::OsStringExt;

    let temp = tempfile::tempdir()?;
    let invalid_name = std::ffi::OsString::from_vec(vec![
        b's', b'i', b'n', b'e', b'x', b'-', 0xff, b'.', b's', b'e', b'r', b'v', b'i', b'c',
        b'e',
    ]);
    std::fs::write(temp.path().join(invalid_name), [])?;

    let error = discover_unit_files_in_path(temp.path().to_str().expect("utf8 path"))
        .await
        .expect_err("non-utf8 unit entries should fail honestly");

    assert!(error.to_string().contains("decode systemd unit entry name"));
    Ok(())
}
