use super::*;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::{FileExt, MetadataExt, PermissionsExt};
use std::process::Command;

struct FileHashParser;

impl crate::Parser for FileHashParser {
    type Record<'a> = &'a [u8];
    type Carry = ();
    type Payload = sinex_model::event_types::media::NoteDocument;

    fn parse(
        &self,
        bytes: &&[u8],
        carry: &(),
        context: &crate::RecordContext<'_>,
    ) -> Result<(Vec<Self::Payload>, ()), crate::RecordError> {
        let (mut notes, ()) =
            crate::Parser::parse(&crate::test_parser::NotesParser, bytes, carry, context)?;
        for note in &mut notes {
            note.text = Some(blake3::hash(bytes).to_hex().to_string());
        }
        Ok((notes, ()))
    }
}

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
    let collision = paths::display(Path::new(&name));
    std::fs::write(root.path().join(&collision), "literal spelling").unwrap();
    let hole = std::fs::File::create(root.path().join("hole.md")).unwrap();
    hole.set_len(1 << 20).unwrap();
    let d = driver(root.path().to_owned(), 16 << 20);
    let acquired = d.poll(&Value::Null, &mut DirState::default()).unwrap();
    let material = acquired.material.as_ref().unwrap();
    let index = material
        .manifest
        .members
        .iter()
        .position(|member| member.details.contains_key("native_path_bytes"))
        .unwrap();
    let node = material.manifest.members[index].node.as_ref().unwrap();
    assert_eq!(node.size, 2 << 20);
    assert!(node.sparse.is_some());
    assert_eq!(node.mode & 0o777, 0o640);
    let bytes = record(material, index);
    assert_eq!(bytes.len(), 2 << 20);
    assert_eq!(node.content_hash, *blake3::hash(&bytes).as_bytes());
    assert_eq!(&bytes[..4], b"head");
    assert_eq!(&bytes[(1 << 20) + 3..(1 << 20) + 7], b"tail");
    assert!(bytes[4..(1 << 20) + 3].iter().all(|byte| *byte == 0));
    let Some(StoredBytes::FramedFile(tar)) = &material.stored else {
        panic!()
    };
    assert!(tar.metadata().unwrap().len() < node.size);
    let attribute = node
        .extended_attributes
        .iter()
        .find(|attribute| attribute.name == b"user.synthetic")
        .unwrap();
    let mut value = vec![0; attribute.length as usize];
    tar.read_exact_at(&mut value, attribute.value_at).unwrap();
    assert_eq!(value, b"binary\0value\n");
    assert_eq!(attribute.hash, *blake3::hash(&value).as_bytes());
    let mut repacked = material.manifest.clone();
    repacked.members[index]
        .node
        .as_mut()
        .unwrap()
        .extended_attributes[0]
        .value_at += 4096;
    repacked.members[index].node.as_mut().unwrap().atime_ns = "999999999".into();
    let context = crate::RecordContext {
        manifest: &material.manifest,
        member: Some(&material.manifest.members[index]),
    };
    let other = crate::RecordContext {
        manifest: &repacked,
        member: Some(&repacked.members[index]),
    };
    assert_eq!(context.hash_with_path(&bytes), other.hash_with_path(&bytes));
    assert!(
        serde_json::to_value(node).unwrap()["extended_attributes"]
            .as_array()
            .unwrap()
            .iter()
            .all(|attribute| attribute.get("value").is_none())
    );
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
    assert_eq!(
        std::fs::read_to_string(restored.join(&collision)).unwrap(),
        "literal spelling"
    );
    let restored_hole = restored.join("hole.md");
    assert_eq!(restored_hole.metadata().unwrap().len(), 1 << 20);
    assert!(restored_hole.metadata().unwrap().blocks() * 512 < 1 << 20);
    assert!(
        std::fs::read(restored_hole)
            .unwrap()
            .iter()
            .all(|byte| *byte == 0)
    );
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
    // The same recorded member expands through the ordinary drain during replay.
    let source = crate::DeclaredSource {
        spec: crate::test_parser::source().spec,
        parser: FileHashParser,
    };
    use crate::Source;
    let chunks = source
        .drain(crate::DrainInput {
            source: "synthetic.notes",
            class: sinex_model::Class::Open,
            material: material.id,
            bytes: material.record_bytes(),
            records: &material.records,
            carry: Value::Null,
            reading_order: &*material.reading_order,
            limits: crate::Limits::default(),
            manifest: &material.manifest,
        })
        .unwrap();
    assert_eq!(chunks[0].events, 3);
    for event in chunks
        .iter()
        .flat_map(|chunk| &chunk.outcomes)
        .flat_map(|outcome| match outcome {
            sinex_model::RecordOutcome::Events { events, .. } => events.as_slice(),
            sinex_model::RecordOutcome::Failure { reason, .. } => panic!("{reason}"),
        })
    {
        let path = event.payload["path"].as_str().unwrap();
        let member = material
            .manifest
            .members
            .iter()
            .find(|member| {
                member.path == path
                    && member
                        .details
                        .get("native_path_bytes")
                        .unwrap_or(&serde_json::Value::Null)
                        == &event.payload["native_path_bytes"]
            })
            .unwrap();
        let node = member.node.as_ref().unwrap();
        assert_eq!(event.payload["size"], node.size);
        assert_eq!(
            event.payload["text"],
            blake3::Hash::from_bytes(node.content_hash)
                .to_hex()
                .to_string()
        );
    }
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
fn membership_markers_do_not_replace_a_source_directory_with_the_same_name() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join(".sinex-membership")).unwrap();
    std::fs::write(root.path().join(".sinex-membership/0.md"), "source file").unwrap();
    std::fs::write(root.path().join("gone.md"), "removed file").unwrap();
    let d = driver(root.path().to_owned(), 1 << 20);
    let mut state = DirState::default();
    let first = d.poll(&Value::Null, &mut state).unwrap();
    let material = first.material.unwrap();
    let cursor = (material.cursor_at)(usize::MAX).unwrap();
    std::fs::remove_file(root.path().join("gone.md")).unwrap();
    std::fs::write(
        root.path().join(".sinex-membership/0.md"),
        "changed source file",
    )
    .unwrap();
    let second = d.poll(&cursor, &mut state).unwrap();
    let material = second.material.unwrap();
    assert_eq!(
        material.manifest.detail("membership_path"),
        ".sinex-membership_"
    );
    let Some(StoredBytes::FramedFile(file)) = &material.stored else {
        panic!()
    };
    let mut input = file.try_clone().unwrap();
    std::io::Seek::rewind(&mut input).unwrap();
    let mut archive = ::tar::Archive::new(input);
    let destination = tempfile::tempdir().unwrap();
    archive.unpack(destination.path()).unwrap();
    assert_eq!(
        std::fs::read_to_string(destination.path().join(".sinex-membership/0.md")).unwrap(),
        "changed source file"
    );
    assert!(destination.path().join(".sinex-membership_/1").is_file());
}
