use super::*;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;

const BODY: &[u8] = b"protected synthetic producer bytes";

fn blob() -> String {
    format!("blake3:{}", blake3::hash(BODY).to_hex())
}

fn write_all_frames(root: &Path, producer: &str) {
    write(root, producer, "frame", &blob(), BODY.len() as u64, BODY).unwrap();
}

fn file(root: &Path, producer: &str) -> std::path::PathBuf {
    directory(root, producer, "frame").join(&blob()[7..])
}

fn mode(path: &Path) -> u32 {
    std::fs::metadata(path).unwrap().mode() & 0o7777
}

#[devkit::test]
fn configured_group_files_are_readable_without_writable_sender_inodes() {
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o2770)).unwrap();
    write_all_frames(root.path(), "node");
    let path = file(root.path(), "node");
    assert_eq!(mode(&path), 0o640);
    assert_eq!(
        std::fs::metadata(path).unwrap().gid(),
        std::fs::metadata(root.path()).unwrap().gid()
    );
    assert_eq!(mode(&directory(root.path(), "node", "frame")), 0o2770);
}

#[devkit::test]
fn private_roots_keep_private_files_and_existing_directories_are_not_adopted() {
    let root = tempfile::tempdir().unwrap();
    write_all_frames(root.path(), "node");
    assert_eq!(mode(&file(root.path(), "node")), 0o600);
    let upload = format!("blake3:{}", blake3::hash(b"upload").to_hex());
    let (descriptor, bytes) = super::encoding(root.path(), b"upload");
    assert!(receive(root.path(), "remote", "frame", &descriptor, 0, &bytes).unwrap());
    assert_eq!(
        mode(&directory(root.path(), "remote", "frame").join(&upload[7..])),
        0o600
    );
    let shared = tempfile::tempdir().unwrap();
    std::fs::set_permissions(shared.path(), std::fs::Permissions::from_mode(0o2770)).unwrap();
    let private = shared.path().join("incoming");
    std::fs::create_dir(&private).unwrap();
    std::fs::set_permissions(&private, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(
        write(
            shared.path(),
            "node",
            "frame",
            &blob(),
            BODY.len() as u64,
            BODY
        )
        .is_err()
    );
    assert_eq!(mode(&private), 0o700);
}

#[devkit::test]
fn existing_private_frame_files_are_refused_without_changing_their_bytes_or_modes() {
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o2770)).unwrap();
    write_all_frames(root.path(), "node");
    let path = file(root.path(), "node");
    let original = std::fs::read(&path).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert!(
        write(
            root.path(),
            "node",
            "frame",
            &blob(),
            BODY.len() as u64,
            BODY
        )
        .is_err()
    );
    assert_eq!(std::fs::read(&path).unwrap(), original);
    assert_eq!(mode(&path), 0o600);
}
