use super::*;

fn command(root: &std::path::Path, script: &str) -> std::process::Command {
    let mut command = std::process::Command::new("sh");
    command
        .args(["-c", script, "synthetic-backup"])
        .arg(root.join("pids"));
    command
}

fn pids(root: &std::path::Path) -> Vec<u32> {
    std::fs::read_to_string(root.join("pids"))
        .unwrap()
        .split_whitespace()
        .map(|pid| pid.parse().unwrap())
        .collect()
}

fn gone(pids: &[u32]) {
    for pid in pids {
        assert!(
            !std::path::Path::new(&format!("/proc/{pid}")).exists(),
            "owned child {pid} must be reaped"
        );
    }
}

#[tokio::test]
async fn leader_failure_reaps_a_descendant_that_inherits_stdout() {
    let root = tempfile::tempdir().unwrap();
    let command = command(
        root.path(),
        "sleep 300 & child=$!; printf '%s %s\\n' $$ $child > \"$1\"; kill -KILL $$",
    );
    let result = backup_process(command, "{}".into(), std::future::pending()).await;
    assert!(result.is_err());
    let pids = pids(root.path());
    assert_eq!(pids.len(), 2);
    gone(&pids);
}

#[tokio::test]
async fn stream_failure_waits_until_the_whole_copy_group_is_reaped() {
    let root = tempfile::tempdir().unwrap();
    let command = command(
        root.path(),
        "sleep 300 & child=$!; printf '%s %s\\n' $$ $child > \"$1\"; wait",
    );
    let cancelled = async {
        loop {
            if std::fs::read_to_string(root.path().join("pids"))
                .ok()
                .is_some_and(|text| text.split_whitespace().count() == 2)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        Failure::new(FailureKind::Cancelled, "synthetic daemon stream closed")
    };
    let failure = backup_process(command, "{}".into(), cancelled)
        .await
        .unwrap_err();
    assert!(
        failure
            .to_string()
            .contains("synthetic daemon stream closed")
    );
    gone(&pids(root.path()));
}
