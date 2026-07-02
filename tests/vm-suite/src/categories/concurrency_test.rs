    use super::{
        classify_stale_cancel_output, classify_zombie_reaping_status, json_u64_at,
        last_json_object, parse_xtask_json_output,
    };

    #[cfg(unix)]
    fn exit_status(code: i32) -> std::process::ExitStatus {
        use std::os::unix::process::ExitStatusExt;

        std::process::ExitStatus::from_raw(code << 8)
    }

    #[test]
    fn xtask_json_parser_accepts_pretty_trailing_json() {
        let output = std::process::Output {
            status: exit_status(0),
            stdout: br#"warning: noisy prelude
{
  "status": "success",
  "data": {
    "job_id": 42
  }
}
"#
            .to_vec(),
            stderr: Vec::new(),
        };

        let parsed = parse_xtask_json_output("fixture", &output)
            .expect("VM helpers must parse pretty xtask JSON, not only one-line JSON");
        assert_eq!(json_u64_at(&parsed, &["data", "job_id"]), Some(42));
    }

    #[test]
    fn xtask_json_parser_rejects_nonzero_status_even_with_json() {
        let output = std::process::Output {
            status: exit_status(2),
            stdout: br#"{"status":"failed","errors":[{"code":"BROKEN"}]}"#.to_vec(),
            stderr: b"boom".to_vec(),
        };

        let error = parse_xtask_json_output("fixture", &output)
            .expect_err("VM polling helpers must not treat failed xtask commands as evidence");
        assert!(error.to_string().contains("exited with rc=2"));
    }

    #[test]
    fn stale_cancel_classifier_accepts_structured_job_not_found() {
        let output = r#"{"status":"failed","errors":[{"code":"JOB_NOT_FOUND","message":"Job 7 not found or not running"}]}"#;

        classify_stale_cancel_output(&exit_status(1), output, "")
            .expect("structured stale-job rejection should prove the VM safety branch");
    }

    #[test]
    fn stale_cancel_classifier_rejects_success() {
        let output = r#"{"status":"success","message":"Job 7 cancelled"}"#;

        let error = classify_stale_cancel_output(&exit_status(0), output, "")
            .expect_err("success would not prove stale-process rejection");
        assert!(error.contains("errors array") || error.contains("JOB_NOT_FOUND"));
    }

    #[test]
    fn stale_cancel_classifier_rejects_missing_errors_array() {
        let output = r#"{"status":"failed","message":"Job 7 not found"}"#;

        let error = classify_stale_cancel_output(&exit_status(1), output, "")
            .expect_err("stale cancellation evidence must contain the structured error code");
        assert!(error.contains("errors array"));
    }

    #[test]
    fn zombie_reaping_classifier_accepts_failed_or_cancelled_after_kill() {
        classify_zombie_reaping_status("failed")
            .expect("failed is terminal orphan-reaping evidence after SIGKILL");
        classify_zombie_reaping_status("cancelled")
            .expect("cancelled is terminal orphan-reaping evidence after SIGKILL");
    }

    #[test]
    fn zombie_reaping_classifier_rejects_natural_completion_after_kill() {
        let error = classify_zombie_reaping_status("completed")
            .expect_err("natural completion is not zombie-reaping evidence");

        assert!(error.contains("natural completion"));
    }

    #[test]
    fn last_json_object_uses_trailing_json_object() {
        let parsed = last_json_object("noise\n{\"status\":\"running\"}\n{\"status\":\"failed\"}\n")
            .expect("expected trailing JSON object");

        assert_eq!(parsed["status"].as_str(), Some("failed"));
    }

    #[test]
    fn last_json_object_accepts_pretty_xtask_output() {
        let parsed = last_json_object(
            r#"warning: ignored
{
  "status": "failed",
  "errors": [
    {
      "code": "JOB_NOT_FOUND",
      "message": "Job 7 not found or not running"
    }
  ]
}
"#,
        )
        .expect("expected trailing pretty JSON object");

        assert_eq!(parsed["status"].as_str(), Some("failed"));
        assert_eq!(parsed["errors"][0]["code"].as_str(), Some("JOB_NOT_FOUND"));
    }
