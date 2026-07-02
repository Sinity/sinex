use super::*;
use crate::sandbox::sinex_test;
use std::process::Output;

#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;

#[sinex_test]
async fn test_check_schema_contract_guard_reports_invalid_base_schema()
-> ::xtask::sandbox::TestResult<()> {
    let error = check_schema_contract_guard("not json", r#"{"type":"object"}"#)
        .expect_err("invalid base schema should surface");
    assert!(error.to_string().contains("base schema JSON"));
    Ok(())
}

#[sinex_test]
async fn test_check_schema_contract_guard_reports_invalid_candidate_schema()
-> ::xtask::sandbox::TestResult<()> {
    let error = check_schema_contract_guard(r#"{"type":"object"}"#, "not json")
        .expect_err("invalid candidate schema should surface");
    assert!(error.to_string().contains("candidate schema JSON"));
    Ok(())
}

#[sinex_test]
async fn test_check_schema_contract_guard_rejects_new_required_fields()
-> ::xtask::sandbox::TestResult<()> {
    let success = check_schema_contract_guard(
        r#"{"type":"object","required":["a"]}"#,
        r#"{"type":"object","required":["a","b"]}"#,
    )?;
    assert!(
        !success,
        "new required fields should fail the contract guard"
    );
    Ok(())
}

#[sinex_test]
async fn test_check_schema_contract_guard_rejects_type_changes()
-> ::xtask::sandbox::TestResult<()> {
    let success = check_schema_contract_guard(
        r#"{"type":"object","properties":{"count":{"type":"integer"}}}"#,
        r#"{"type":"object","properties":{"count":{"type":"string"}}}"#,
    )?;
    assert!(
        !success,
        "property type change should fail the contract guard"
    );
    Ok(())
}

#[sinex_test]
async fn test_check_schema_contract_guard_rejects_enum_value_removals()
-> ::xtask::sandbox::TestResult<()> {
    let success = check_schema_contract_guard(
        r#"{"type":"object","properties":{"status":{"enum":["active","inactive","pending"]}}}"#,
        r#"{"type":"object","properties":{"status":{"enum":["active","inactive"]}}}"#,
    )?;
    assert!(
        !success,
        "enum value removal should fail the contract guard"
    );
    Ok(())
}

#[sinex_test]
async fn test_check_schema_contract_guard_allows_new_optional_properties()
-> ::xtask::sandbox::TestResult<()> {
    let success = check_schema_contract_guard(
        r#"{"type":"object","properties":{"a":{"type":"string"}}}"#,
        r#"{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"integer"}}}"#,
    )?;
    assert!(
        success,
        "adding new optional properties should pass the contract guard"
    );
    Ok(())
}

#[sinex_test]
async fn test_check_schema_contract_guard_allows_enum_value_additions()
-> ::xtask::sandbox::TestResult<()> {
    let success = check_schema_contract_guard(
        r#"{"type":"object","properties":{"status":{"enum":["active","inactive"]}}}"#,
        r#"{"type":"object","properties":{"status":{"enum":["active","inactive","pending"]}}}"#,
    )?;
    assert!(
        success,
        "adding new enum values should pass the contract guard"
    );
    Ok(())
}

#[sinex_test]
async fn test_resolve_socket_dir_reports_current_dir_failures()
-> ::xtask::sandbox::TestResult<()> {
    let error = resolve_socket_dir(None, Err(std::io::Error::other("cwd exploded")))
        .expect_err("current_dir failure should surface");
    let message = format!("{error:#}");
    assert!(message.contains("cwd exploded"));
    assert!(message.contains("socket dir"));
    Ok(())
}

#[sinex_test]
async fn test_read_base_contract_contents_treats_missing_object_as_new_contract()
-> ::xtask::sandbox::TestResult<()> {
    #[cfg(unix)]
    {
        let result = read_base_contract_contents_with(
            "main:schemas/foo.json",
            Ok(Output {
                status: std::process::ExitStatus::from_raw(32768),
                stdout: Vec::new(),
                stderr: b"fatal: path 'schemas/foo.json' exists on disk, but not in 'main'\n"
                    .to_vec(),
            }),
        )?;
        assert!(result.is_none());
    }
    Ok(())
}

#[sinex_test]
async fn test_read_base_contract_contents_reports_git_show_failures()
-> ::xtask::sandbox::TestResult<()> {
    #[cfg(unix)]
    {
        let error = read_base_contract_contents_with(
            "main:schemas/foo.json",
            Ok(Output {
                status: std::process::ExitStatus::from_raw(512),
                stdout: Vec::new(),
                stderr: b"fatal: repository exploded\n".to_vec(),
            }),
        )
        .expect_err("git show failures should surface");
        let message = format!("{error:#}");
        assert!(message.contains("failed to read base contract"));
        assert!(message.contains("repository exploded"));
    }
    Ok(())
}
