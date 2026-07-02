use super::*;
use crate::sandbox::sinex_test;

#[sinex_test]
async fn test_parse_fuzz_target_count_accepts_valid_count() -> ::xtask::sandbox::TestResult<()> {
    let result = CommandResult::success().with_data(serde_json::json!({
        "target_count": 3u64
    }));

    assert_eq!(super::parse_fuzz_target_count(&result)?, 3);
    Ok(())
}

#[sinex_test]
async fn test_parse_fuzz_target_count_rejects_missing_count() -> ::xtask::sandbox::TestResult<()> {
    let result = CommandResult::success().with_data(serde_json::json!({
        "items": []
    }));

    let error =
        super::parse_fuzz_target_count(&result).expect_err("missing target count must surface");
    assert!(format!("{error:#}").contains("missing target_count"));
    Ok(())
}

#[sinex_test]
async fn test_parse_fuzz_target_count_rejects_non_numeric_count() -> ::xtask::sandbox::TestResult<()>
{
    let result = CommandResult::success().with_data(serde_json::json!({
        "target_count": "three"
    }));

    let error =
        super::parse_fuzz_target_count(&result).expect_err("non-numeric target count must surface");
    assert!(format!("{error:#}").contains("invalid target_count"));
    Ok(())
}

#[sinex_test]
async fn test_classify_disk_space_probe_reports_low_space() -> ::xtask::sandbox::TestResult<()> {
    let status = super::classify_disk_space_probe_result(Ok(1), 2);
    assert!(matches!(
        status,
        DiskSpaceStatus::Low {
            available_gb: 1,
            min_gb: 2
        }
    ));
    Ok(())
}

#[sinex_test]
async fn test_classify_disk_space_probe_reports_sufficient_space()
-> ::xtask::sandbox::TestResult<()> {
    let status = super::classify_disk_space_probe_result(Ok(4), 2);
    assert!(matches!(
        status,
        DiskSpaceStatus::Sufficient {
            available_gb: 4,
            min_gb: 2
        }
    ));
    Ok(())
}

#[sinex_test]
async fn test_classify_disk_space_probe_surfaces_probe_failures() -> ::xtask::sandbox::TestResult<()>
{
    let status = super::classify_disk_space_probe_result(Err("statvfs failed".to_string()), 2);
    let DiskSpaceStatus::Unknown { issue } = status else {
        panic!("expected unknown disk-space status");
    };
    assert!(issue.contains("statvfs failed"));
    Ok(())
}
