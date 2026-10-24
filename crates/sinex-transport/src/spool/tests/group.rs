use super::*;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;
use std::process::Command;

const BODY: &[u8] = b"protected synthetic producer bytes";
const SENDER: u32 = 60001;
const RECEIVER: u32 = 60002;
const OUTSIDER: u32 = 60003;
const GROUP: u32 = 60010;

fn frozen() -> Frozen {
    let material = MaterialFrame {
        id: MaterialId(uuid::Uuid::from_u128(42)),
        input: InputRef {
            kind: "file".into(),
            locator: "synthetic".into(),
            host: "fixture".into(),
        },
        blob: format!("blake3:{}", blake3::hash(BODY).to_hex()),
        length: BODY.len() as u64,
        parser: "synthetic".into(),
        parser_version: 1,
        part_index: 0,
        previous_part: None,
        manifest: serde_json::json!({}),
    };
    let frame = crate::Ingress::Material {
        frame: "inner".into(),
        material: Box::new(material),
    };
    let mut publication = declaration(&sinex_protocol::producer_bytes::encode_frame(&frame), "final");
    let crate::Ingress::Material { material, .. } = &frame else {
        unreachable!()
    };
    publication.material = Some(Material::from_frame(material));
    Frozen { publication, frame }
}

fn stage_all(root: &Path, producer: &str) {
    let frozen = frozen();
    assert_eq!(freeze(root, producer, &frozen).unwrap(), frozen);
    let envelope = sinex_protocol::producer_bytes::encode_frame(&frozen.frame);
    stage(root, producer, &frozen.publication, Role::Material, BODY).unwrap();
    stage(
        root,
        producer,
        &frozen.publication,
        Role::Envelope,
        envelope.as_slice(),
    )
    .unwrap();
}

fn mode(path: &Path) -> u32 {
    std::fs::metadata(path).unwrap().mode() & 0o7777
}

#[test]
fn configured_group_files_are_readable_without_writable_sender_inodes() {
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o2770)).unwrap();
    stage_all(root.path(), "node");
    let frozen = frozen();
    for (role, _) in frozen.publication.roles() {
        let path = directory(root.path(), "node", &frozen.publication).join(role.name());
        assert_eq!(mode(&path), 0o640);
        assert_eq!(
            std::fs::metadata(path).unwrap().gid(),
            std::fs::metadata(root.path()).unwrap().gid()
        );
    }
    assert_eq!(mode(&artifact_path(root.path(), "node", "final")), 0o640);
    assert_eq!(
        mode(&directory(root.path(), "node", &frozen.publication)),
        0o2770
    );
}

#[test]
fn private_roots_keep_private_files_and_existing_directories_are_not_adopted() {
    let root = tempfile::tempdir().unwrap();
    stage_all(root.path(), "node");
    let frozen = frozen();
    assert_eq!(mode(&artifact_path(root.path(), "node", "final")), 0o600);
    assert_eq!(
        mode(&directory(root.path(), "node", &frozen.publication).join("material")),
        0o600
    );
    let upload = declaration(b"upload", "upload");
    assert!(receive(root.path(), "remote", &upload, Role::Envelope, 0, b"upload").unwrap());
    assert_eq!(
        mode(&directory(root.path(), "remote", &upload).join("envelope")),
        0o600
    );
    let shared = tempfile::tempdir().unwrap();
    std::fs::set_permissions(shared.path(), std::fs::Permissions::from_mode(0o2770)).unwrap();
    let private = shared.path().join("incoming");
    std::fs::create_dir(&private).unwrap();
    std::fs::set_permissions(&private, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(freeze(shared.path(), "node", &frozen).is_err());
    assert_eq!(mode(&private), 0o700);
}

#[test]
fn existing_private_handoff_files_refuse_without_changing_their_bytes_or_modes() {
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o2770)).unwrap();
    stage_all(root.path(), "node");
    let frozen = frozen();
    let artifact = artifact_path(root.path(), "node", "final");
    let material = directory(root.path(), "node", &frozen.publication).join("material");
    for path in [&artifact, &material] {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let before = std::fs::read(&artifact).unwrap();
    assert!(freeze(root.path(), "node", &frozen).is_err());
    assert!(
        stage(
            root.path(),
            "node",
            &frozen.publication,
            Role::Material,
            BODY
        )
        .is_err()
    );
    assert_eq!(std::fs::read(&artifact).unwrap(), before);
    assert_eq!(std::fs::read(&material).unwrap(), BODY);
    assert_eq!(mode(&artifact), 0o600);
    assert_eq!(mode(&material), 0o600);
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
        "inherited supplementary groups were not retained"
    );
    let shared = Path::new("/proc/self/fd/0/shared");
    let private = Path::new("/proc/self/fd/0/private");
    let frozen = frozen();
    match operation {
        "stage" => {
            stage_all(shared, "node");
            stage_all(shared, "other");
            stage_all(private, "node");
        }
        "denied" => {
            assert!(snapshot(shared, "node", &frozen.publication, Role::Material).is_err());
            assert!(load(shared, "node", "final").is_err());
            assert!(stage(shared, "node", &frozen.publication, Role::Material, BODY).is_err());
        }
        "receiver" => {
            let mut snapshots = Vec::new();
            for (role, bytes) in frozen.publication.roles() {
                let file = snapshot(shared, "node", &frozen.publication, role).unwrap();
                assert!(
                    std::fs::OpenOptions::new()
                        .write(true)
                        .open(directory(shared, "node", &frozen.publication).join(role.name()))
                        .is_err(),
                    "receiver group read access does not grant writes to sender inodes"
                );
                assert_eq!(file.metadata().unwrap().uid(), RECEIVER);
                assert_eq!(file.metadata().unwrap().mode() & 0o777, 0o600);
                snapshots.push((file, bytes.blob.clone()));
            }
            discard_bytes(shared, "node", &frozen.publication).unwrap();
            for (mut file, name) in snapshots {
                let mut bytes = Vec::new();
                file.read_to_end(&mut bytes).unwrap();
                assert_eq!(format!("blake3:{}", blake3::hash(&bytes).to_hex()), name);
            }
            assert_eq!(
                load(shared, "node", "final").unwrap(),
                Some(frozen.clone()),
                "role cleanup cannot retire the sender's lost-ack artifact"
            );
            assert!(snapshot(shared, "other", &frozen.publication, Role::Material).is_ok());
            assert!(snapshot(private, "node", &frozen.publication, Role::Material).is_err());
            assert!(load(private, "node", "final").is_err());
        }
        "retry" => {
            assert_eq!(load(shared, "node", "final").unwrap(), Some(frozen.clone()));
            assert!(snapshot(shared, "node", &frozen.publication, Role::Material).is_err());
            stage_all(shared, "node");
            assert_eq!(
                pending(shared, "node")
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap(),
                [frozen]
            );
        }
        "finish" => {
            discard(shared, "node", &frozen.publication).unwrap();
            assert!(load(shared, "node", "final").unwrap().is_none());
            assert!(snapshot(shared, "other", &frozen.publication, Role::Material).is_ok());
            assert_eq!(mode(&artifact_path(private, "node", "final")), 0o600);
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
    let test_name = format!(
        "{}::configured_group_handoff_between_distinct_users",
        module_path!().split_once("::").unwrap().1,
    );
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
            "--exact",
            "producer_bytes::spool::tests::group::configured_group_handoff_between_distinct_users",
            "--ignored",
            "--nocapture",
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
        output.status.success()
            && String::from_utf8_lossy(&output.stdout)
                .contains("test result: ok. 1 passed; 0 failed"),
        "{operation} as {uid}: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
#[ignore = "requires root for isolated numeric-UID/group filesystem controls"]
fn configured_group_handoff_between_distinct_users() {
    if let Ok(operation) = std::env::var("SINEX_FIXTURE_ACTOR") {
        return actor(&operation);
    }
    let uid = Command::new("id").arg("-u").output().unwrap();
    assert_eq!(
        String::from_utf8(uid.stdout).unwrap().trim(),
        "0",
        "run this native control as root"
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
    // Mirror the configured tmpfiles parents; leaf publication directories are
    // created by the actual helper under the sender's restrictive umask.
    for relative in [
        "incoming",
        "incoming/producers",
        "incoming/producer-requests",
    ] {
        let path = root.path().join("shared").join(relative);
        std::fs::create_dir(&path).unwrap();
        std::os::unix::fs::chown(&path, Some(RECEIVER), Some(GROUP)).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o2770)).unwrap();
    }
    run_actor(root.path(), "stage", SENDER, SENDER);
    run_actor(root.path(), "denied", OUTSIDER, OUTSIDER);
    run_actor(root.path(), "receiver", RECEIVER, GROUP);
    run_actor(root.path(), "retry", SENDER, SENDER);
    run_actor(root.path(), "finish", SENDER, SENDER);
}
