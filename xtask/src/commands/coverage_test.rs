use super::*;
use crate::output::OutputFormat;
use crate::sandbox::sinex_test;
use std::os::unix::fs::PermissionsExt;

#[sinex_test]
async fn test_command_name() -> ::xtask::sandbox::TestResult<()> {
    let cmd = CoverageCommand {
        subcommand: CoverageSubcommand::Clean,
    };
    assert_eq!(cmd.name(), "coverage");
    Ok(())
}

#[sinex_test]
async fn test_command_metadata() -> ::xtask::sandbox::TestResult<()> {
    let cmd = CoverageCommand {
        subcommand: CoverageSubcommand::Summary {
            package: None,
            files: false,
        },
    };
    let metadata = cmd.metadata();
    assert_eq!(metadata.category, Some("test"));
    assert!(metadata.timeout.is_some());
    assert!(!metadata.modifies_state);
    Ok(())
}

#[sinex_test]
async fn test_threshold_validation() -> ::xtask::sandbox::TestResult<()> {
    let ctx = CommandContext::new(
        crate::output::OutputWriter::new(OutputFormat::Silent),
        false,
        None,
        "coverage",
    );

    let result = execute_enforce(150.0, None, false, ".sinex/coverage/html", &ctx);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("between 0 and 100")
    );
    Ok(())
}

#[sinex_test]
async fn test_clean_command() -> ::xtask::sandbox::TestResult<()> {
    let cmd = CoverageCommand {
        subcommand: CoverageSubcommand::Clean,
    };
    assert_eq!(cmd.name(), "coverage");
    Ok(())
}

#[sinex_test]
async fn test_open_report_in_browser_reports_missing_openers() -> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let report = temp.path().join("index.html");
    std::fs::write(&report, "<html></html>")?;

    let original_path = std::env::var("PATH").ok();
    unsafe { std::env::set_var("PATH", temp.path()) };

    let error = open_report_in_browser(&report)
        .expect_err("missing browser opener commands must fail honestly");
    let message = error.to_string();
    assert!(message.contains("failed to open coverage report"));
    assert!(message.contains("xdg-open"));
    assert!(message.contains("open"));

    match original_path {
        Some(path) => unsafe { std::env::set_var("PATH", path) },
        None => unsafe { std::env::remove_var("PATH") },
    }
    Ok(())
}

#[sinex_test]
async fn test_open_report_in_browser_accepts_xdg_open() -> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let report = temp.path().join("index.html");
    let opener = temp.path().join("xdg-open");
    std::fs::write(&report, "<html></html>")?;
    std::fs::write(
        &opener,
        "#!/bin/sh\nprintf '%s' \"$1\" > \"$TMPDIR/coverage-opened-path\"\n",
    )?;
    std::fs::set_permissions(&opener, std::fs::Permissions::from_mode(0o755))?;

    let capture_dir = tempfile::tempdir()?;
    let original_path = std::env::var("PATH").ok();
    let original_tmpdir = std::env::var("TMPDIR").ok();
    unsafe {
        std::env::set_var("PATH", temp.path());
        std::env::set_var("TMPDIR", capture_dir.path());
    }

    open_report_in_browser(&report)?;

    let capture_path = capture_dir.path().join("coverage-opened-path");
    for _ in 0..50 {
        if capture_path.exists() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    let captured = std::fs::read_to_string(&capture_path)?;
    assert_eq!(captured, report.to_string_lossy());

    match original_path {
        Some(path) => unsafe { std::env::set_var("PATH", path) },
        None => unsafe { std::env::remove_var("PATH") },
    }
    match original_tmpdir {
        Some(path) => unsafe { std::env::set_var("TMPDIR", path) },
        None => unsafe { std::env::remove_var("TMPDIR") },
    }
    Ok(())
}
