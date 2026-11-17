//! Checks, by explicit numeric UID, who may read the protected shared store.
use hashed_blobs::{Extent, Preparation, SegmentWriter};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;
const BODY: &[u8] = b"a tiny receiver-installed plain blob\n";
const CLIENT: u32 = 60001;
const RECEIVER: u32 = 60002;
const OUTSIDER: u32 = 60003;
const GROUP: u32 = 60010;
fn read(root: &Path) -> hashed_blobs::Result<Vec<u8>> {
    let descriptor = std::fs::read(root.join("descriptor"))?;
    hashed_blobs::Reader::new(root).read(&[Extent::decode(&descriptor)?], None)
}
fn actor(operation: &str) {
    let expected = std::env::var("SINEX_FIXTURE_UID").unwrap();
    let actual = Command::new("id").arg("-u").output().unwrap();
    assert_eq!(String::from_utf8(actual.stdout).unwrap().trim(), expected);
    let root = Path::new("/proc/self/fd/0");
    match operation {
        "install" => {
            let shared = root.join("shared");
            let prepared = Preparation::new(shared.join("incoming"))
                .unwrap()
                .plain(BODY, 3)
                .unwrap();
            let mut writer =
                SegmentWriter::open(&shared, "segments/current.hb", 0, &[], 0o640).unwrap();
            let group = writer
                .append_group([Ok(prepared)], |_| Ok(None))
                .unwrap()
                .wait()
                .unwrap();
            writer.acknowledge(&group).unwrap();
            std::fs::write(root.join("shared/descriptor"), group.extents[0].encode()).unwrap();
            std::fs::set_permissions(
                root.join("shared/descriptor"),
                std::fs::Permissions::from_mode(0o640),
            )
            .unwrap();
            assert_eq!(read(&root.join("shared")).unwrap(), BODY);
        }
        "read" => assert_eq!(read(&root.join("shared")).unwrap(), BODY),
        "denied" => assert!(read(&root.join("shared")).is_err()),
        _ => panic!("unknown readback operation"),
    }
}
fn run_actor(root: &Path, operation: &str, uid: u32, gid: u32) {
    let groups = if uid == CLIENT {
        format!("--groups={GROUP}")
    } else {
        "--clear-groups".into()
    };
    let output = Command::new("/run/current-system/sw/bin/setpriv")
        .args([
            "--reuid",
            &uid.to_string(),
            "--regid",
            &gid.to_string(),
            &groups,
            "--",
            "./actor",
            operation,
        ])
        .current_dir(root)
        .stdin(std::fs::File::open(root).unwrap())
        .env("SINEX_FIXTURE_UID", uid.to_string())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{operation}: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
fn main() {
    if let Some(operation) = std::env::args().nth(1) {
        return actor(&operation);
    }
    let identity = Command::new("id").arg("-u").output().unwrap();
    assert_eq!(
        String::from_utf8(identity.stdout).unwrap().trim(),
        "0",
        "numeric-UID fixture requires root"
    );
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o711)).unwrap();
    std::fs::copy(std::env::current_exe().unwrap(), root.path().join("actor")).unwrap();
    std::fs::set_permissions(
        root.path().join("actor"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    let shared = root.path().join("shared");
    std::fs::create_dir(&shared).unwrap();
    std::os::unix::fs::chown(&shared, Some(RECEIVER), Some(GROUP)).unwrap();
    std::fs::set_permissions(shared, std::fs::Permissions::from_mode(0o2770)).unwrap();
    run_actor(root.path(), "install", RECEIVER, GROUP);
    run_actor(root.path(), "read", CLIENT, CLIENT);
    run_actor(root.path(), "denied", OUTSIDER, OUTSIDER);
}
