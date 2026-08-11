use super::*;

#[test]
#[ignore = "sinex-ouyv open: estimate_file_count's max_depth=Some(0) short-circuit (`depth >= max` \
            is true at depth=0) returns 0 for the root directory's own contents instead of counting \
            them, making max_watched_files checks ineffective for zero-depth watch configurations. \
            (This bead also covers a 1000-entry estimator cap and a symlink_metadata-error fail-open \
            gap, neither exercised by this test -- see sinex-ouyv for those.)"]
fn zero_depth_watch_still_counts_root_level_files_against_the_file_limit() {
    let dir = tempfile::tempdir().expect("tempdir");
    for i in 0..5 {
        std::fs::write(dir.path().join(format!("file{i}.txt")), b"x").expect("write");
    }
    let root = Utf8PathBuf::from_path_buf(dir.path().to_path_buf()).expect("utf8 path");

    let mut policy = FileWatchingSecurityPolicy::permissive();
    policy.max_watch_depth = Some(0);
    policy.max_watched_files = Some(3); // 5 real files exceed this

    let result = validate_watch_paths(&[root.to_string()], &policy);
    assert!(
        result.is_err(),
        "5 root-level files under max_watched_files=3 with max_watch_depth=0 should be rejected, \
         but the zero-depth estimator returns 0 and lets it through: {result:?}"
    );
}
