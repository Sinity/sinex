use sinex_source_core::drivers::{
    StoredBytes,
    dir_snapshot::{DirSnapshot, DirState},
};
use std::{fs::File, io::Write};

#[devkit::test]
fn the_bundle_does_not_hold_whole_file_bodies_in_memory() {
    let dir = tempfile::tempdir().unwrap();
    let block = [b'x'; 64 * 1024];
    for n in 0..6 {
        let mut file = File::create(dir.path().join(format!("{n}.md"))).unwrap();
        for _ in 0..64 {
            file.write_all(&block).unwrap();
        }
    }
    let driver = DirSnapshot {
        root: dir.path().to_owned(),
        host: "synthetic".into(),
        select: |path| path.ends_with(".md"),
        part_bytes: 32 << 20,
        removed_at_source: false,
    };
    let mut state = DirState::default();
    // Counting is local to this synchronous poll. It excludes executable page
    // faults and filesystem cache, and includes buffers released before return.
    let known_allocation = allocation_counter::measure(|| {
        let body = vec![0u8; 24 << 20];
        std::hint::black_box(&body);
    });
    assert!(known_allocation.bytes_max >= 24 << 20);

    let mut acquired = None;
    let measured = allocation_counter::measure(|| {
        acquired = Some(driver.poll(&serde_json::Value::Null, &mut state).unwrap());
    });
    let acquired = acquired.unwrap();
    let material = acquired.material.unwrap();
    assert!(material.bytes.is_empty());
    assert_eq!(material.records.len(), 6);
    let Some(StoredBytes::FramedFile(file)) = material.stored else {
        panic!("the bundle must be in a temporary file")
    };
    let size = file.metadata().unwrap().len();
    assert_eq!(size % 512, 0);
    assert!(size > 24 << 20);
    assert_eq!(material.manifest.members.iter().map(|member| member.node.as_ref().unwrap().size).sum::<u64>(), 24 << 20);
    assert!(
        measured.bytes_max < 8 << 20,
        "bundle allocations: {measured:?}"
    );
}
