use super::*;
use std::os::unix::fs::PermissionsExt;

fn converter(root: &Path, fail: bool) -> PathBuf {
    let path = root.join(if fail { "failed-readpst" } else { "readpst" });
    std::fs::write(
        &path,
        format!(
            r##"#!/bin/sh
if [ "$1" = "-V" ]; then printf 'synthetic-readpst 1\n'; exit 0; fi
while [ "$1" != "-o" ]; do shift; done
out="$2"
input="$3"
printf '%s' "$out" > "$(dirname "$0")/last-stage"
{}
mkdir -p "$out/Inbox"
cat "$input" > "$out/Inbox/mbox"
"##,
            if fail { "exit 23" } else { ":" }
        ),
    )
    .unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    path
}

fn message(subject: &str) -> Vec<u8> {
    format!("From sender@example.test Tue Sep 15 10:00:00 2026\nMessage-ID: <{subject}@example.test>\nFrom: sender@example.test\nDate: Tue, 15 Sep 2026 10:00:00 +0000\nSubject: {subject}\n\n{subject} body\n\n").into_bytes()
}

#[devkit::test]
fn same_named_exports_have_stable_bindings_and_independent_reader_owned_staging() {
    let root = tempfile::tempdir().unwrap();
    let converter = converter(root.path(), false);
    let inputs = root.path().join("inputs");
    std::fs::create_dir_all(inputs.join("a")).unwrap();
    std::fs::create_dir_all(inputs.join("b")).unwrap();
    let first = inputs.join("a/archive.pst");
    let second = inputs.join("b/archive.pst");
    let first_bytes = message("first");
    let second_bytes = message("second");
    std::fs::write(&first, &first_bytes).unwrap();
    std::fs::write(&second, &second_bytes).unwrap();
    let settings = Settings {
        host: "fixture".into(),
        ..Default::default()
    };
    let bindings = plan_with(&inputs, &settings, &converter).unwrap();
    assert_eq!(bindings.len(), 2);
    assert_eq!(
        bindings,
        plan_with(&inputs, &settings, &converter).unwrap(),
        "scratch names cannot become source identities"
    );
    assert_ne!(bindings[0].name, bindings[1].name);
    assert_eq!(bindings[0].input, first);
    assert_eq!(bindings[1].input, second);
    let Reading::Shared(mut a) = reader_with(&bindings[0], &converter).unwrap() else {
        panic!("shared mbox reader");
    };
    let a_root = a.path().parent().unwrap().to_owned();
    assert_eq!(a_root.parent().unwrap(), std::env::temp_dir());
    let Reading::Shared(b) = reader_with(&bindings[1], &converter).unwrap() else {
        panic!("shared mbox reader");
    };
    let b_root = b.path().parent().unwrap().to_owned();
    assert_ne!(a_root, b_root);
    assert_eq!(std::fs::read(a.path()).unwrap(), first_bytes);
    assert_eq!(std::fs::read(b.path()).unwrap(), second_bytes);
    assert_eq!(std::fs::read(&first).unwrap(), first_bytes);
    assert_eq!(std::fs::read(&second).unwrap(), second_bytes);
    let changed = message("first revised");
    std::fs::write(&first, &changed).unwrap();
    a.refresh().unwrap();
    assert!(
        !a_root.exists(),
        "refresh releases the previous private conversion"
    );
    let refreshed_root = a.path().parent().unwrap().to_owned();
    assert_eq!(std::fs::read(a.path()).unwrap(), changed);
    assert_eq!(a.acquisition().unwrap().path, first);
    drop(a);
    assert!(
        !refreshed_root.exists(),
        "reader drop removes its private conversion"
    );
    assert_eq!(
        std::fs::read(b.path()).unwrap(),
        second_bytes,
        "another reader remains intact"
    );
    drop(b);
    assert!(!b_root.exists());
    assert!(first.is_file() && second.is_file());
}

#[devkit::test]
fn converter_failure_cleans_its_private_directory_and_preserves_the_export() {
    let root = tempfile::tempdir().unwrap();
    let converter = converter(root.path(), true);
    let input = root.path().join("archive.pst");
    let original = message("unchanged");
    std::fs::write(&input, &original).unwrap();
    let error = plan_with(&input, &Settings::default(), &converter).unwrap_err();
    assert!(error.contains("23"), "{error}");
    let stage = std::fs::read_to_string(root.path().join("last-stage")).unwrap();
    assert!(!Path::new(&stage).exists());
    assert_eq!(std::fs::read(input).unwrap(), original);
}
