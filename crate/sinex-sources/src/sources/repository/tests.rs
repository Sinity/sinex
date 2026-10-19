use super::*;
use std::process::Command;

fn git(dir: &std::path::Path, args: &[&str]) {
    let status = Command::new("git")
        .args(args)
        .current_dir(dir)
        .env("GIT_AUTHOR_NAME", "A")
        .env("GIT_AUTHOR_EMAIL", "a@example.org")
        .env("GIT_COMMITTER_NAME", "C")
        .env("GIT_COMMITTER_EMAIL", "c@example.org")
        .env("GIT_AUTHOR_DATE", "2026-01-01T00:00:00Z")
        .env("GIT_COMMITTER_DATE", "2026-01-01T00:00:10Z")
        .status()
        .unwrap();
    assert!(status.success());
}

fn commits(a: &crate::drivers::Acquired) -> Vec<Map<String, Value>> {
    let m = a.material.as_ref().unwrap();
    m.records
        .iter()
        .map(|(s, e)| serde_json::from_slice(&m.bytes[*s..*e]).unwrap())
        .collect()
}

#[test]
fn only_newly_reachable_commits_are_read() {
    let dir = std::env::temp_dir().join(format!("sinex-git-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    git(&dir, &["init", "-q", "-b", "main"]);
    git(&dir, &["commit", "-q", "--allow-empty", "-m", "one"]);
    git(
        &dir,
        &["commit", "-q", "--allow-empty", "-m", "two\n\nbody"],
    );
    let d = driver(dir.clone(), "h".into());
    let first = d.poll(&Value::Null).unwrap();
    let read = commits(&first);
    assert_eq!(read.len(), 2);
    let newest = &read[0];
    // The message exactly as stored, trailing newline included.
    assert_eq!(newest["message"], json!("two\n\nbody\n"));
    assert_eq!(newest["author_email"], json!("a@example.org"));
    assert_eq!(newest["committed_at"], json!("2026-01-01T00:00:10Z"));
    assert!(
        newest["refs"]
            .as_array()
            .unwrap()
            .contains(&json!("refs/heads/main"))
    );
    let parsed = GitParser.parse(newest, &()).unwrap().0.remove(0);
    assert_eq!(
        parsed.parents,
        vec![read[1]["commit"].as_str().unwrap().to_owned()]
    );

    let m = first.material.as_ref().unwrap();
    let cursor = (m.cursor_at)(m.bytes.len());
    assert!(d.poll(&cursor).unwrap().material.is_none());

    git(&dir, &["checkout", "-q", "-b", "side"]);
    git(&dir, &["commit", "-q", "--allow-empty", "-m", "three"]);
    let next = d.poll(&cursor).unwrap();
    let read = commits(&next);
    assert_eq!(read.len(), 1);
    assert_eq!(read[0]["message"], json!("three\n"));
}
