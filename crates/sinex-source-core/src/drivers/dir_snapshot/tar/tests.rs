use super::*;
use super::{final_cursor, poll_entries};
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::{FileExt, MetadataExt, PermissionsExt};
use std::process::Command;

#[devkit::test]
fn gnu_tar_restores_native_names_sparse_data_attributes_and_access_acl() {
    let root = tempfile::tempdir().unwrap();
    let destination = tempfile::tempdir().unwrap();
    let name = std::ffi::OsString::from_vec(b"long\npercent%\xff.md".to_vec());
    let path = root.path().join(&name);
    let file = std::fs::File::create(&path).unwrap();
    file.set_len(2 << 20).unwrap();
    file.write_all_at(b"head", 0).unwrap();
    file.write_all_at(b"tail", (1 << 20) + 3).unwrap();
    file.set_permissions(std::fs::Permissions::from_mode(0o640))
        .unwrap();
    rustix::fs::fsetxattr(
        &file,
        "user.synthetic",
        b"binary\0value\n",
        rustix::fs::XattrFlags::empty(),
    )
    .unwrap();
    let mut acl = 2u32.to_le_bytes().to_vec();
    for (tag, permissions, id) in [
        (1u16, 6u16, u32::MAX),
        (2, 4, 12345),
        (4, 4, u32::MAX),
        (16, 4, u32::MAX),
        (32, 0, u32::MAX),
    ] {
        acl.extend(tag.to_le_bytes());
        acl.extend(permissions.to_le_bytes());
        acl.extend(id.to_le_bytes());
    }
    rustix::fs::fsetxattr(
        &file,
        "system.posix_acl_access",
        &acl,
        rustix::fs::XattrFlags::empty(),
    )
    .unwrap();
    file.set_modified(std::time::UNIX_EPOCH - std::time::Duration::from_nanos(125_000_001)).unwrap();
    let d = driver(root.path().to_owned(), 16 << 20);
    let acquired = d.poll(&Value::Null, &mut DirState::default()).unwrap();
    let material = acquired.material.as_ref().unwrap();
    let node = material.manifest.members[0].node.as_ref().unwrap();
    assert_eq!(node.size, 2 << 20);
    assert!(node.sparse.is_some());
    assert_eq!(node.mode & 0o777, 0o640);
    let bytes = record(material, 0);
    assert_eq!(bytes.len(), 2 << 20);
    assert_eq!(&bytes[..4], b"head");
    assert_eq!(&bytes[(1 << 20) + 3..(1 << 20) + 7], b"tail");
    assert!(bytes[4..(1 << 20) + 3].iter().all(|byte| *byte == 0));
    let Some(StoredBytes::FramedFile(tar)) = &material.stored else {
        panic!()
    };
    assert!(tar.metadata().unwrap().len() < node.size);
    let archive = destination.path().join("snapshot.tar");
    let mut output = std::fs::File::create(&archive).unwrap();
    // Reading the whole tar starts at its beginning, independently of its shared cursor.
    let mut stored = tar.try_clone().unwrap();
    std::io::Seek::rewind(&mut stored).unwrap();
    std::io::Seek::rewind(&mut output).unwrap();
    std::io::copy(&mut stored, &mut output).unwrap();
    let restored = destination.path().join("restored");
    std::fs::create_dir(&restored).unwrap();
    let status = Command::new("tar")
        .args([
            "--extract",
            "--xattrs",
            "--acls",
            "--same-permissions",
            "--file",
        ])
        .arg(&archive)
        .arg("--directory")
        .arg(&restored)
        .status()
        .unwrap();
    assert!(status.success());
    let restored_path = restored.join(&name);
    assert_eq!(std::fs::read(&restored_path).unwrap(), bytes);
    let restored_file = std::fs::File::open(&restored_path).unwrap();
    let metadata = restored_file.metadata().unwrap();
    assert_eq!(metadata.mode() & 0o777, 0o640);
    assert!(metadata.blocks() * 512 < metadata.len());
    assert_eq!(metadata.uid(), node.uid);
    assert_eq!(metadata.gid(), node.gid);
    assert_eq!(metadata_mtime(&metadata).to_string(), node.mtime_ns);
    let mut attribute = vec![0; 128];
    let count =
        rustix::fs::fgetxattr(&restored_file, "user.synthetic", attribute.as_mut_slice()).unwrap();
    assert_eq!(&attribute[..count], b"binary\0value\n");
    let count = rustix::fs::fgetxattr(
        &restored_file,
        "system.posix_acl_access",
        attribute.as_mut_slice(),
    )
    .unwrap();
    assert_eq!(&attribute[..count], acl);

}

#[devkit::test]
fn empty_files_and_long_native_names_have_distinct_restorable_tar_members() {
    let root = tempfile::tempdir().unwrap();
    let name = format!("{}💡.md", "x".repeat(160));
    for path in ["a.md", "b.md", &name, "percent%FF.md"] {
        std::fs::write(root.path().join(path), "").unwrap();
    }
    let acquired = driver(root.path().to_owned(), 1 << 20)
        .poll(&Value::Null, &mut DirState::default())
        .unwrap();
    let material = acquired.material.unwrap();
    assert_eq!(material.records.len(), 4);
    for pair in material.records.windows(2) {
        assert!(pair[0].0 < pair[1].0);
    }
    for index in 0..4 {
        assert!(record(&material, index).is_empty());
    }
    let Some(StoredBytes::FramedFile(file)) = &material.stored else {
        panic!()
    };
    let mut input = file.try_clone().unwrap();
    std::io::Seek::rewind(&mut input).unwrap();
    let mut archive = ::tar::Archive::new(input);
    let names = archive
        .entries()
        .unwrap()
        .map(|entry| entry.unwrap().path_bytes().into_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        material
            .manifest
            .members
            .iter()
            .map(|member| member.path.as_bytes().to_vec())
            .collect::<Vec<_>>()
    );
}

#[devkit::test]
fn membership_facts_do_not_replace_native_files_when_tar_is_extracted() {
    let root = tempfile::tempdir().unwrap();
    let destination = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join(".sinex-membership")).unwrap();
    std::fs::write(root.path().join(".sinex-membership/0"), b"original bytes").unwrap();
    std::fs::write(root.path().join("removed.md"), b"removed bytes").unwrap();
    let d = DirSnapshot {
        root: root.path().to_owned(),
        host: "host".into(),
        select: |_| true,
        part_bytes: 1 << 20,
    };
    let mut state = DirState::default();
    let first = d.poll(&Value::Null, &mut state).unwrap();
    let cursor = final_cursor(&first);
    std::fs::remove_file(root.path().join("removed.md")).unwrap();
    std::fs::write(
        root.path().join(".sinex-membership/0"),
        b"new original bytes",
    )
    .unwrap();
    let second = poll_entries(
        &d,
        &cursor,
        &mut state,
        BTreeMap::from([
            (
                PathBuf::from(".sinex-membership/0"),
                Ok((
                    18,
                    tar::change_time(
                        &std::fs::metadata(root.path().join(".sinex-membership/0")).unwrap(),
                    ),
                )),
            ),
            (
                PathBuf::from("unreadable.md"),
                Err("synthetic error".into()),
            ),
        ]),
    );
    let material = second.material.as_ref().unwrap();
    assert_eq!(material.records.len(), 3);
    let Some(StoredBytes::FramedFile(file)) = &material.stored else {
        panic!()
    };
    let archive = tempfile::NamedTempFile::new().unwrap();
    let mut input = file.try_clone().unwrap();
    std::io::Seek::rewind(&mut input).unwrap();
    std::io::copy(&mut input, &mut archive.reopen().unwrap()).unwrap();
    let output = Command::new("tar")
        .arg("--extract")
        .arg("--file")
        .arg(archive.path())
        .arg("--directory")
        .arg(destination.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        std::fs::read(destination.path().join(".sinex-membership/0")).unwrap(),
        b"new original bytes"
    );
    assert!(!destination.path().join("removed.md").exists());
    assert!(!destination.path().join("unreadable.md").exists());
    assert_eq!(
        std::fs::read_dir(destination.path().join(".sinex-membership"))
            .unwrap()
            .count(),
        1
    );
}
