use super::*;

fn temp() -> PathBuf {
    let d = std::env::temp_dir().join(format!("sinex-fw-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn rec(v: Value) -> Map<String, Value> {
    serde_json::from_slice(&live_stream::frame(
        Timestamp::now(),
        v.to_string().as_bytes(),
    ))
    .unwrap()
}

#[test]
fn directories_skip_declared_names_and_symlinks() {
    let root = temp();
    std::fs::create_dir_all(root.join("a/b")).unwrap();
    std::fs::create_dir_all(root.join(".git/objects")).unwrap();
    std::os::unix::fs::symlink("/", root.join("escape")).unwrap();
    let (dirs, _) = directories(&root, &[".git".to_owned()]);
    let mut rel: Vec<_> = dirs
        .iter()
        .map(|d| d.strip_prefix(&root).unwrap().to_owned())
        .collect();
    rel.sort();
    assert_eq!(
        rel,
        vec![PathBuf::from(""), PathBuf::from("a"), PathBuf::from("a/b")]
    );
}

#[test]
fn renames_keep_both_ends_and_odd_paths_survive() {
    let (out, ()) = FileWatchParser
        .parse(
            &rec(json!({"kind": "renamed", "paths": ["/a/x", "/a/y"], "is_dir": null})),
            &(),
            &crate::RecordContext::none(),
        )
        .unwrap();
    assert_eq!(out[0].path.as_deref(), Some("/a/x"));
    assert_eq!(out[0].to.as_deref(), Some("/a/y"));
    let (out, ()) = FileWatchParser
        .parse(
            &rec(json!({"kind": "created", "paths": [{"hex": "2fff"}], "is_dir": false})),
            &(),
            &crate::RecordContext::none(),
        )
        .unwrap();
    assert_eq!(
        (out[0].path.clone(), out[0].path_hex.as_deref()),
        (None, Some("2fff"))
    );
    let (out, ()) = FileWatchParser
        .parse(&rec(json!({"kind": "unwatched", "paths": ["/big"], "note": "inotify watch limit reached"})), &(), &crate::RecordContext::none())
        .unwrap();
    assert_eq!(out[0].note.as_deref(), Some("inotify watch limit reached"));
}

#[test]
fn capture_sees_changes_including_in_new_directories() {
    let root = temp();
    let spool = temp().join("spool");
    let roots = Roots {
        roots: vec![root.clone()],
        skip: vec!["target".into()],
    };
    let r2 = root.clone();
    let writer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(300));
        std::fs::write(r2.join("f"), b"x").unwrap();
        std::fs::create_dir(r2.join("new")).unwrap();
        std::thread::sleep(Duration::from_millis(300));
        std::fs::write(r2.join("new/g"), b"y").unwrap();
        std::fs::create_dir(r2.join("target")).unwrap();
        std::thread::sleep(Duration::from_millis(300));
        std::fs::write(r2.join("target/ignored"), b"z").unwrap();
    });
    capture(&roots, &spool, Some(Duration::from_millis(1500))).unwrap();
    writer.join().unwrap();
    let acquired = driver(spool, "h".into()).poll(&Value::Null).unwrap();
    let m = acquired.material.unwrap();
    let mut seen = Vec::new();
    for (s, e) in &m.records {
        let r: Map<String, Value> = serde_json::from_slice(&m.bytes[*s..*e]).unwrap();
        for c in FileWatchParser
            .parse(&r, &(), &crate::RecordContext::none())
            .unwrap()
            .0
        {
            seen.push((c.change, c.path.unwrap_or_default()));
        }
    }
    let has =
        |change: &str, suffix: &str| seen.iter().any(|(c, p)| c == change && p.ends_with(suffix));
    assert!(has("created", "/f"), "{seen:?}");
    assert!(has("created", "/new"), "{seen:?}");
    assert!(
        has("created", "/new/g"),
        "a file in a directory made after capture started: {seen:?}"
    );
    assert!(
        !seen.iter().any(|(_, p)| p.ends_with("target/ignored")),
        "{seen:?}"
    );
}
