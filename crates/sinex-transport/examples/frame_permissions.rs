//! Explicit synthetic filesystem custody qualification. Run as root; it uses
//! isolated numeric UIDs only inside its temporary fixture and child processes.
use sinex_transport::frame_bytes::{directory, discard, reader, write};
use std::{
    io::Read,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::Path,
    process::Command,
};
const BODY: &[u8] = b"protected synthetic producer bytes";
const SENDER: u32 = 60001;
const RECEIVER: u32 = 60002;
const OUTSIDER: u32 = 60003;
const GROUP: u32 = 60010;
fn blob() -> String {
    format!("blake3:{}", blake3::hash(BODY).to_hex())
}
fn write_all_frames(root: &Path, producer: &str) {
    write(root, producer, "frame", &blob(), BODY.len() as u64, BODY).unwrap();
}
fn file(root: &Path, producer: &str) -> std::path::PathBuf {
    directory(root, producer, "frame").join(&blob()[7..])
}
fn actor(operation: &str) {
    let expected_uid = std::env::var("SINEX_FIXTURE_UID").unwrap();
    let uid = Command::new("id").arg("-u").output().unwrap();
    assert_eq!(String::from_utf8(uid.stdout).unwrap().trim(), expected_uid);
    let groups = Command::new("id").arg("-G").output().unwrap();
    let mut groups: Vec<u32> = String::from_utf8(groups.stdout)
        .unwrap()
        .split_whitespace()
        .map(|value| value.parse().unwrap())
        .collect();
    groups.sort_unstable();
    groups.dedup();
    let uid: u32 = expected_uid.parse().unwrap();
    let expected = if uid == SENDER {
        vec![SENDER, GROUP]
    } else if uid == RECEIVER {
        vec![GROUP]
    } else {
        vec![OUTSIDER]
    };
    assert_eq!(
        groups, expected,
        "inherited supplementary groups were not kept"
    );
    let shared = Path::new("/proc/self/fd/0/shared");
    let private = Path::new("/proc/self/fd/0/private");
    let length = BODY.len() as u64;
    match operation {
        "write" => {
            write_all_frames(shared, "node");
            write_all_frames(shared, "other");
            write_all_frames(private, "node");
        }
        "denied" => {
            assert!(reader(shared, "node", "frame", &blob(), length).is_err());
            assert!(write(shared, "node", "frame", &blob(), length, BODY).is_err());
        }
        "receiver" => {
            let mut copy = reader(shared, "node", "frame", &blob(), length).unwrap();
            assert!(
                std::fs::OpenOptions::new()
                    .write(true)
                    .open(file(shared, "node"))
                    .is_err(),
                "group read access does not let the receiver write the sender's file"
            );
            let private_copy = std::fs::read_dir("/proc/self/fd")
                .unwrap()
                .filter_map(|entry| entry.ok())
                .filter_map(|entry| std::fs::metadata(entry.path()).ok())
                .any(|metadata| {
                    metadata.is_file()
                        && metadata.uid() == RECEIVER
                        && metadata.nlink() == 0
                        && metadata.len() > 0
                        && metadata.mode() & 0o777 == 0o600
                });
            assert!(
                private_copy,
                "decoded reader retains a private receiver-owned encoded inode"
            );
            discard(shared, "node", "frame").unwrap();
            let mut bytes = Vec::new();
            copy.read_to_end(&mut bytes).unwrap();
            assert_eq!(bytes, BODY);
            assert!(reader(shared, "other", "frame", &blob(), length).is_ok());
            assert!(reader(private, "node", "frame", &blob(), length).is_err());
        }
        "retry" => {
            assert!(reader(shared, "node", "frame", &blob(), length).is_err());
            write_all_frames(shared, "node");
            assert!(reader(shared, "node", "frame", &blob(), length).is_ok());
        }
        _ => panic!("unknown synthetic actor operation"),
    }
}

fn run_actor(root: &Path, operation: &str, uid: u32, gid: u32) {
    let groups = if uid == SENDER {
        format!("--groups={GROUP}")
    } else {
        "--clear-groups".into()
    };
    let output = Command::new("/run/current-system/sw/bin/setpriv")
        // Clear inherited supplementary groups before selecting the fixture's
        // explicit credentials. The sender then gets only its configured group.
        .args([
            "--clear-groups",
            "--",
            "/run/current-system/sw/bin/setpriv",
            "--reuid",
            &uid.to_string(),
            "--regid",
            &gid.to_string(),
            &groups,
            "--",
            "./actor",
        ])
        .current_dir(root)
        // An inherited directory handle reaches only this owned fixture even
        // when the job scratch's parent is private. No parent modes change.
        .stdin(std::fs::File::open(root).unwrap())
        .env("SINEX_FIXTURE_ACTOR", operation)
        .env("SINEX_FIXTURE_UID", uid.to_string())
        .env("TMPDIR", format!("/proc/self/fd/0/tmp-{uid}"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{operation} as {uid}: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn main() {
    if let Ok(operation) = std::env::var("SINEX_FIXTURE_ACTOR") {
        return actor(&operation);
    }
    let uid = Command::new("id").arg("-u").output().unwrap();
    assert_eq!(
        String::from_utf8(uid.stdout).unwrap().trim(),
        "0",
        "run this test as root"
    );
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o711)).unwrap();
    let binary = root.path().join("actor");
    std::fs::copy(std::env::current_exe().unwrap(), &binary).unwrap();
    std::fs::set_permissions(binary, std::fs::Permissions::from_mode(0o755)).unwrap();
    for (name, uid, gid, permissions) in [
        ("shared".to_owned(), RECEIVER, GROUP, 0o2770),
        ("private".to_owned(), SENDER, SENDER, 0o700),
        (format!("tmp-{SENDER}"), SENDER, SENDER, 0o700),
        (format!("tmp-{RECEIVER}"), RECEIVER, GROUP, 0o700),
        (format!("tmp-{OUTSIDER}"), OUTSIDER, OUTSIDER, 0o700),
    ] {
        let path = root.path().join(name);
        std::fs::create_dir(&path).unwrap();
        std::os::unix::fs::chown(&path, Some(uid), Some(gid)).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(permissions)).unwrap();
    }
    // Mirror the configured tmpfiles parents; leaf frame directories are
    // created by the actual helper under the sender's restrictive umask.
    for relative in ["incoming", "incoming/producers"] {
        let path = root.path().join("shared").join(relative);
        std::fs::create_dir(&path).unwrap();
        std::os::unix::fs::chown(&path, Some(RECEIVER), Some(GROUP)).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o2770)).unwrap();
    }
    run_actor(root.path(), "write", SENDER, SENDER);
    run_actor(root.path(), "denied", OUTSIDER, OUTSIDER);
    run_actor(root.path(), "receiver", RECEIVER, GROUP);
    run_actor(root.path(), "retry", SENDER, SENDER);
}
