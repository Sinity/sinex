use super::*;
use std::os::unix::fs::MetadataExt;

fn files() -> (tempfile::TempDir, File, File) {
    let root = tempfile::tempdir().unwrap();
    let input = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(root.path().join("source"))
        .unwrap();
    let output = file(&root.path().join("copy")).unwrap();
    (root, input, output)
}

#[test]
#[cfg(target_os = "linux")]
fn sparse_ranges_keep_leading_middle_and_final_holes_and_the_logical_digest() {
    use rustix::fs::{FallocateFlags, fallocate};
    let (root, mut input, mut output) = files();
    let mut expected = vec![0x5a; 8 << 20];
    input.write_all(&expected).unwrap();
    for (offset, length) in [(0, 1 << 20), (2 << 20, 4 << 20)] {
        fallocate(
            &input,
            FallocateFlags::PUNCH_HOLE | FallocateFlags::KEEP_SIZE,
            offset,
            length,
        )
        .unwrap();
        expected[offset as usize..(offset + length) as usize].fill(0);
    }
    expected.resize(10 << 20, 0);
    input.set_len(expected.len() as u64).unwrap();
    assert!(input.metadata().unwrap().blocks() * 512 < expected.len() as u64 / 2);
    let (_sender, stop) = watch::channel(false);
    let digest = copy(&mut input, &mut output, expected.len() as u64, &stop).unwrap();
    assert_eq!(std::fs::read(root.path().join("copy")).unwrap(), expected);
    assert_eq!(digest, blake3::hash(&expected));
    assert!(
        output.metadata().unwrap().blocks() * 512 < expected.len() as u64 / 2,
        "the destination must retain physical holes, not merely zero bytes"
    );
}

#[test]
fn unsupported_range_queries_copy_the_same_frozen_prefix_densely() {
    let (root, mut input, mut output) = files();
    let bytes = b"data\0\0\0and a later suffix";
    input.write_all(bytes).unwrap();
    let (_sender, stop) = watch::channel(false);
    let mut queries = 0;
    let digest = copy_ranges(&mut input, &mut output, 12, &stop, |_, _, _| {
        queries += 1;
        Err(io::ErrorKind::Unsupported.into())
    })
    .unwrap();
    assert_eq!(queries, 1);
    assert_eq!(
        std::fs::read(root.path().join("copy")).unwrap(),
        &bytes[..12]
    );
    assert_eq!(digest, blake3::hash(&bytes[..12]));
}

#[test]
fn append_after_the_range_query_does_not_extend_the_copied_boundary() {
    let (root, mut input, mut output) = files();
    let original = b"the held original prefix";
    input.write_all(original).unwrap();
    let mut tail = OpenOptions::new()
        .append(true)
        .open(root.path().join("source"))
        .unwrap();
    let (_sender, stop) = watch::channel(false);
    let digest = copy_ranges(
        &mut input,
        &mut output,
        original.len() as u64,
        &stop,
        |input, position, length| {
            tail.write_all(b"later live writer append").unwrap();
            data_range(input, position, length)
        },
    )
    .unwrap();
    assert!(input.metadata().unwrap().len() > original.len() as u64);
    assert_eq!(std::fs::read(root.path().join("copy")).unwrap(), original);
    assert_eq!(digest, blake3::hash(original));
}

#[test]
fn a_missing_or_disappearing_suffix_is_never_filled_as_a_hole() {
    let (_root, mut input, mut output) = files();
    input.write_all(b"short").unwrap();
    let (_sender, stop) = watch::channel(false);
    assert!(copy(&mut input, &mut output, 6, &stop).is_err());
    assert_eq!(output.metadata().unwrap().len(), 0);
    input.set_len(6).unwrap();
    let result = copy_ranges(&mut input, &mut output, 6, &stop, |input, _, length| {
        input.set_len(5)?;
        Ok((length, length))
    });
    assert!(result.is_err());
    assert_eq!(output.metadata().unwrap().len(), 0);
    input.set_len(6).unwrap();
    let result = copy_ranges(
        &mut input,
        &mut output,
        6,
        &stop,
        |input, position, length| {
            input.set_len(5)?;
            Ok((position, length))
        },
    );
    assert!(
        result.is_err(),
        "a short read must not become an implicit hole"
    );
}

#[test]
fn cancellation_keeps_an_incomplete_copy_without_final_length_or_digest() {
    let (root, mut input, mut output) = files();
    input.write_all(&[0x41; 4096]).unwrap();
    input.set_len(8 << 20).unwrap();
    let (sender, stop) = watch::channel(false);
    let result = copy_ranges(
        &mut input,
        &mut output,
        8 << 20,
        &stop,
        |_, position, length| {
            if position == 0 {
                Ok((0, 4096))
            } else {
                sender.send(true).unwrap();
                Ok((length, length))
            }
        },
    );
    assert!(result.is_err());
    assert_eq!(output.metadata().unwrap().len(), 4096);
    drop(output);
    assert!(
        file(&root.path().join("copy")).is_err(),
        "a failed attempt cannot overwrite and silently reuse the partial copy"
    );
}

#[test]
fn unexpected_range_errors_are_not_treated_as_unsupported_or_holes() {
    let (_root, mut input, mut output) = files();
    input.write_all(b"retained").unwrap();
    let (_sender, stop) = watch::channel(false);
    assert!(
        copy_ranges(&mut input, &mut output, 8, &stop, |_, _, _| Err(
            io::ErrorKind::PermissionDenied.into()
        ))
        .is_err()
    );
    assert_eq!(output.metadata().unwrap().len(), 0);
}
