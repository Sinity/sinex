use super::*;
use std::path::Path;
use std::process::Command;

fn git(dir: &Path, args: &[&str]) {
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
    assert!(status.success(), "git {args:?}");
}

fn rev(dir: &Path, name: &str) -> String {
    let out = Command::new("git")
        .args(["rev-parse", name])
        .current_dir(dir)
        .output()
        .unwrap();
    String::from_utf8(out.stdout).unwrap().trim().to_owned()
}

fn repository() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("sinex-git-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    git(&dir, &["init", "-q", "-b", "main"]);
    dir
}

fn records(a: &crate::drivers::Acquired) -> Vec<Map<String, Value>> {
    let m = a.material.as_ref().unwrap();
    m.records
        .iter()
        .map(|(s, e)| serde_json::from_slice(&m.bytes[*s..*e]).unwrap())
        .collect()
}

fn commits(a: &crate::drivers::Acquired) -> Vec<Map<String, Value>> {
    records(a)
        .into_iter()
        .filter(|r| !r.contains_key("record"))
        .collect()
}

fn updates(a: &crate::drivers::Acquired) -> Vec<GitRefUpdated> {
    records(a)
        .into_iter()
        .filter(|r| r.get("record") == Some(&json!("ref_updated")))
        .map(|r| serde_json::from_value(Value::Object(r)).unwrap())
        .collect()
}

fn cursor(a: &crate::drivers::Acquired) -> Value {
    let m = a.material.as_ref().unwrap();
    (m.cursor_at)(m.bytes.len()).unwrap()
}

/// The ref updates of the poll after `previous`.
fn updates_after(
    d: &ApiCursor<Repository>,
    previous: &crate::drivers::Acquired,
) -> Vec<GitRefUpdated> {
    updates(&d.poll(&cursor(previous)).unwrap())
}

fn parse(record: &Map<String, Value>) -> Result<GitRecord, RecordError> {
    GitParser
        .parse(record, &(), &crate::RecordContext::none())
        .map(|(mut out, ())| out.remove(0))
}

#[devkit::test]
fn only_newly_reachable_commits_are_read() {
    let dir = repository();
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
    let GitRecord::Commit(parsed) = parse(newest).unwrap() else {
        panic!("a commit")
    };
    assert_eq!(
        parsed.parents,
        vec![read[1]["commit"].as_str().unwrap().to_owned()]
    );
    assert_eq!(
        parsed.files,
        Some(vec![]),
        "an empty commit changes nothing"
    );

    let cursor = cursor(&first);
    assert!(d.poll(&cursor).unwrap().material.is_none());

    git(&dir, &["checkout", "-q", "-b", "side"]);
    git(&dir, &["commit", "-q", "--allow-empty", "-m", "three"]);
    let next = d.poll(&cursor).unwrap();
    let read = commits(&next);
    assert_eq!(read.len(), 1);
    assert_eq!(read[0]["message"], json!("three\n"));
}

#[devkit::test]
fn the_first_read_records_where_each_ref_points() {
    let dir = repository();
    git(&dir, &["commit", "-q", "--allow-empty", "-m", "one"]);
    let first = driver(dir.clone(), "h".into()).poll(&Value::Null).unwrap();
    let updates = updates(&first);
    assert_eq!(updates.len(), 1, "HEAD is not compared: {updates:?}");
    assert_eq!(updates[0].ref_name, "refs/heads/main");
    assert_eq!(updates[0].change, RefChange::Created);
    assert_eq!(updates[0].before, None);
    assert_eq!(
        updates[0].after.as_deref(),
        Some(rev(&dir, "main").as_str())
    );
}

#[devkit::test]
fn a_force_push_is_a_rewrite_listing_the_commits_left_behind() {
    let dir = repository();
    git(&dir, &["commit", "-q", "--allow-empty", "-m", "one"]);
    git(&dir, &["commit", "-q", "--allow-empty", "-m", "two"]);
    git(&dir, &["commit", "-q", "--allow-empty", "-m", "three"]);
    let two = rev(&dir, "HEAD~1");
    let three = rev(&dir, "HEAD");
    let d = driver(dir.clone(), "h".into());
    let first = cursor(&d.poll(&Value::Null).unwrap());

    git(&dir, &["reset", "-q", "--hard", "HEAD~2"]);
    git(&dir, &["commit", "-q", "--allow-empty", "-m", "rewritten"]);
    let next = d.poll(&first).unwrap();
    let read = commits(&next);
    assert_eq!(read.len(), 1);
    assert_eq!(read[0]["message"], json!("rewritten\n"));
    let updates = updates(&next);
    assert_eq!(updates.len(), 1);
    let update = &updates[0];
    assert_eq!(update.change, RefChange::Rewritten);
    assert_eq!(update.before.as_deref(), Some(three.as_str()));
    assert_eq!(update.after.as_deref(), Some(rev(&dir, "main").as_str()));
    assert_eq!(update.unreachable, vec![three.clone(), two]);
    let GitRecord::RefUpdated(parsed) = parse(&records(&next)[1]).unwrap() else {
        panic!("a ref update")
    };
    assert_eq!(&parsed, update);

    // Moving forward again is a fast-forward and leaves nothing behind.
    git(&dir, &["commit", "-q", "--allow-empty", "-m", "four"]);
    let forward = updates_after(&d, &next);
    assert_eq!(forward.len(), 1);
    assert_eq!(forward[0].change, RefChange::FastForward);
    assert!(forward[0].unreachable.is_empty());
}

#[devkit::test]
fn commits_another_ref_still_reaches_are_not_unreachable() {
    let dir = repository();
    git(&dir, &["commit", "-q", "--allow-empty", "-m", "one"]);
    git(&dir, &["branch", "keep"]);
    git(&dir, &["commit", "-q", "--allow-empty", "-m", "two"]);
    let two = rev(&dir, "HEAD");
    git(&dir, &["checkout", "-q", "-b", "side"]);
    git(&dir, &["commit", "-q", "--allow-empty", "-m", "side"]);
    let side = rev(&dir, "HEAD");
    git(&dir, &["checkout", "-q", "main"]);
    let d = driver(dir.clone(), "h".into());
    let first = d.poll(&Value::Null).unwrap();

    // main goes back to `keep`; `side` still reaches `two`.
    git(&dir, &["reset", "-q", "--hard", "keep"]);
    let reset = d.poll(&cursor(&first)).unwrap();
    let moved = updates(&reset);
    assert_eq!(moved.len(), 1);
    assert_eq!(moved[0].change, RefChange::Rewritten);
    assert!(moved[0].unreachable.is_empty(), "{moved:?}");

    // Deleting `side` leaves both its commit and `two` behind.
    git(&dir, &["branch", "-q", "-D", "side"]);
    let deleted = updates_after(&d, &reset);
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].ref_name, "refs/heads/side");
    assert_eq!(deleted[0].change, RefChange::Deleted);
    assert_eq!(deleted[0].after, None);
    assert_eq!(deleted[0].unreachable, vec![side, two]);
}

#[devkit::test]
fn each_commit_lists_its_changed_files_against_its_first_parent() {
    let dir = repository();
    std::fs::write(dir.join("a.txt"), "a\n").unwrap();
    std::fs::create_dir_all(dir.join("sub")).unwrap();
    std::fs::write(dir.join("sub/b.txt"), "b\n").unwrap();
    git(&dir, &["add", "."]);
    git(&dir, &["commit", "-q", "-m", "add"]);
    std::fs::write(dir.join("a.txt"), "a2\n").unwrap();
    git(&dir, &["mv", "sub/b.txt", "c.txt"]);
    git(&dir, &["commit", "-q", "-am", "change"]);
    let read = commits(&driver(dir.clone(), "h".into()).poll(&Value::Null).unwrap());
    let parsed: Vec<GitCommit> = read
        .iter()
        .map(|r| serde_json::from_value(Value::Object(r.clone())).unwrap())
        .collect();
    let added = parsed[1].files.as_ref().unwrap();
    assert_eq!(
        added.iter().map(|f| f.path.as_str()).collect::<Vec<_>>(),
        ["a.txt", "sub/b.txt"],
        "files only, in path order"
    );
    assert_eq!(added[0].change, FileChange::Added);
    assert_eq!(added[0].mode.as_deref(), Some("100644"));
    let changed = parsed[0].files.as_ref().unwrap();
    let summary: Vec<(&str, FileChange)> = changed
        .iter()
        .map(|f| (f.path.as_str(), f.change))
        .collect();
    assert_eq!(
        summary,
        [
            ("a.txt", FileChange::Modified),
            ("c.txt", FileChange::Added),
            ("sub/b.txt", FileChange::Deleted),
        ]
    );
    // A rename keeps the exact object on both sides.
    assert_eq!(changed[1].object, changed[2].previous_object);
    assert_ne!(changed[0].object, changed[0].previous_object);
}

#[devkit::test]
fn a_linked_worktree_head_is_read_and_named() {
    let dir = repository();
    git(&dir, &["commit", "-q", "--allow-empty", "-m", "one"]);
    let linked = dir.with_extension("linked");
    git(
        &dir,
        &[
            "worktree",
            "add",
            "-q",
            "--detach",
            linked.to_str().unwrap(),
        ],
    );
    git(
        &linked,
        &["commit", "-q", "--allow-empty", "-m", "detached"],
    );
    let read = commits(&driver(dir.clone(), "h".into()).poll(&Value::Null).unwrap());
    let detached = read
        .iter()
        .find(|r| r["message"] == json!("detached\n"))
        .expect("a commit only a worktree's HEAD reaches is read");
    let worktrees = detached["worktrees"].as_array().unwrap();
    assert_eq!(worktrees.len(), 1);
    assert!(
        worktrees[0]
            .as_str()
            .unwrap()
            .ends_with(linked.file_name().unwrap().to_str().unwrap())
    );
}

/// Run git with `input` on stdin and return its output.
fn git_input(dir: &Path, args: &[&str], input: &[u8]) -> String {
    use std::io::Write;
    let mut child = Command::new("git")
        .args(args)
        .current_dir(dir)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(input).unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success(), "git {args:?}");
    String::from_utf8(out.stdout).unwrap().trim().to_owned()
}

#[devkit::test]
fn a_name_that_is_not_utf8_keeps_its_bytes() {
    let dir = repository();
    // Written as raw bytes: git itself would warn and might convert them.
    let tree = git_input(&dir, &["mktree"], b"");
    let object = format!(
        "tree {tree}\nauthor Andr\u{e9} <a@example.org> 1767225600 +0000\ncommitter C <c@example.org> 1767225610 +0000\n\none\n"
    );
    let latin1: Vec<u8> = object.chars().map(|c| c as u8).collect();
    let commit = git_input(
        &dir,
        &["hash-object", "-t", "commit", "-w", "--stdin"],
        &latin1,
    );
    git(&dir, &["update-ref", "refs/heads/main", &commit]);
    let read = commits(&driver(dir.clone(), "h".into()).poll(&Value::Null).unwrap());
    let GitRecord::Commit(commit) = parse(&read[0]).unwrap() else {
        panic!("a commit")
    };
    assert_eq!(commit.author_name, "Andr\u{fffd}");
    assert_eq!(
        commit
            .original_bytes
            .get("/author_name")
            .map(String::as_str),
        Some("QW5kcuk=")
    );
}

#[devkit::test]
fn an_unreadable_commit_is_a_record_failure() {
    let record = json!({"record": "unreadable", "repository": "/r", "commit": "abc",
                        "reason": "corrupt object"});
    let error = parse(record.as_object().unwrap()).unwrap_err();
    assert!(error.0.contains("abc") && error.0.contains("corrupt object"));
}
