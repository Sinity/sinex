use super::*;
use sinex_source_core::discovery::{Availability, DiscoveryContext};

#[devkit::test]
fn nested_includes_keep_context_duplicate_references_cycles_missing_files_and_changes() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("main.journal");
    let child = dir.path().join("child.journal");
    std::fs::write(&root, "Y2032\ninclude child.journal\ninclude child.journal\ninclude missing.journal\ninclude |never-execute\n").unwrap();
    std::fs::write(
        &child,
        "02/29 Child\n  Assets:Example  1 TEST\n\ninclude main.journal\n",
    )
    .unwrap();
    let settings = Settings::new("example-host");
    let bindings = plan(&root, &settings).unwrap();
    assert_eq!(bindings.len(), 3);
    assert_ne!(bindings[0].name, bindings[1].name);
    assert_eq!(bindings[0].settings.options["initial_year"], 2032);
    assert_eq!(
        bindings[1].settings.options["included_from"]["duplicate"],
        1
    );
    let main = bindings.iter().find(|b| b.input == root).unwrap();
    let targets = main.settings.options["includes"]
        .as_object()
        .unwrap()
        .values()
        .flat_map(|v| v.as_array().unwrap())
        .collect::<Vec<_>>();
    assert!(targets.iter().any(|target| {
        target["reason"]
            .as_str()
            .is_some_and(|r| r.contains("not executed"))
    }));
    assert!(targets.iter().any(|target| {
        target["path"]
            .as_str()
            .unwrap()
            .ends_with("missing.journal")
            && target["reason"].is_string()
    }));
    assert!(
        bindings[0].settings.options["includes"]
            .to_string()
            .contains("include cycle")
    );
    // The child is its own ordinary input: its revision is not hidden by unchanged parent bytes.
    assert_eq!(bindings[0].input, child);
    std::fs::write(&child, "03/01 Changed\n  Assets:Example  2 TEST\n").unwrap();
    let changed = plan(&root, &settings).unwrap();
    assert_eq!(changed.len(), 3);
    assert_eq!(changed[0].name, bindings[0].name);
    assert!(
        changed[0].settings.options["includes"]
            .as_object()
            .unwrap()
            .is_empty()
    );
}

#[devkit::test]
fn recursive_globs_follow_relative_paths_and_exclude_the_current_file() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("sub")).unwrap();
    let root = dir.path().join("main.journal");
    std::fs::write(&root, "include **.journal\n").unwrap();
    std::fs::write(dir.path().join("child.journal"), "; child\n").unwrap();
    std::fs::write(dir.path().join("sub/other.journal"), "; nested\n").unwrap();
    std::fs::write(dir.path().join(".hidden.journal"), "; hidden\n").unwrap();
    let bindings = plan(&root, &Settings::new("example-host")).unwrap();
    assert_eq!(bindings.len(), 3);
    assert!(
        bindings
            .iter()
            .all(|b| !b.settings.options["includes"].to_string().contains("cycle"))
    );
    assert_eq!(
        resolve(
            &root,
            "journal:child.journal",
            &Settings::new("example-host")
        )
        .unwrap(),
        vec![dir.path().join("child.journal")]
    );
    assert_eq!(
        resolve(
            &root,
            "sub/../child.journal",
            &Settings::new("example-host")
        )
        .unwrap(),
        vec![dir.path().join("sub/../child.journal")]
    );
}

#[devkit::test]
fn discovery_uses_finance_environment_home_and_text_history_inputs() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("journal_clean");
    std::fs::write(&root, "").unwrap();
    let mut context = DiscoveryContext {
        host: "example-host".into(),
        home: Some(dir.path().into()),
        ..Default::default()
    };
    context
        .input_roots
        .insert("finance".into(), vec![root.clone()]);
    let found = crate::discovery::discover_format(&super::super::FORMAT, &context);
    assert!(found.iter().any(|i| i.input.as_ref() == Some(&root)
        && i.availability == Availability::Reading
        && i.bindings.len() == 1));
    assert!(found.iter().any(
        |i| i.input.as_ref() == Some(&dir.path().join(".hledger.journal"))
            && i.availability == Availability::WaitingForInput
    ));
    context
        .environment
        .insert("LEDGER_FILE".into(), root.display().to_string());
    assert_eq!(
        crate::discovery::discover_format(&super::super::FORMAT, &context)
            .iter()
            .filter(|i| i.input.as_ref() == Some(&root))
            .count(),
        1
    );
    context
        .inputs
        .insert("text-history".into(), vec![root.clone()]);
    assert!(
        crate::discovery::discover_format(&crate::sources::text_history::FORMAT, &context)
            .iter()
            .any(|i| i.input.as_ref() == Some(&root) && i.availability == Availability::Reading)
    );
}

#[devkit::test]
fn nested_duplicate_include_paths_remain_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("main.journal");
    std::fs::write(&root, "include child.journal\ninclude child.journal\n").unwrap();
    std::fs::write(
        dir.path().join("child.journal"),
        "include leaf.journal\ninclude leaf.journal\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("leaf.journal"),
        "2030-01-01 Leaf\n  Assets:Example\n",
    )
    .unwrap();
    let bindings = plan(&root, &Settings::new("example-host")).unwrap();
    assert_eq!(bindings.len(), 7);
    assert_eq!(
        bindings
            .iter()
            .map(|b| &b.name)
            .collect::<BTreeSet<_>>()
            .len(),
        7
    );
}

#[devkit::test]
fn native_time_formats_are_kept_as_written_records_without_generating_transactions() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("main.journal");
    std::fs::write(
        &root,
        "include timeclock:hours.data\ninclude hours.timedot\n",
    )
    .unwrap();
    let clock = b"i 2030/01/01 09:00:00 activities:example\no 2030/01/01 10:00:00\n";
    let dot = b"2030/01/01\n  activities:example  ....\n";
    std::fs::write(dir.path().join("hours.data"), clock).unwrap();
    std::fs::write(dir.path().join("hours.timedot"), dot).unwrap();
    let bindings = plan(&root, &Settings::new("example-host")).unwrap();
    assert_eq!(bindings.len(), 3);
    for (name, bytes, count, format) in [
        ("hours.data", clock.as_slice(), 2, "timeclock"),
        ("hours.timedot", dot.as_slice(), 1, "timedot"),
    ] {
        let binding = bindings
            .iter()
            .find(|b| b.input.file_name().unwrap() == name)
            .unwrap();
        let source = (super::super::FORMAT.source)(binding).unwrap();
        let events = super::super::tests::intake::export_restart(
            bytes,
            super::super::driver(binding.input.clone(), binding.settings.host.clone()),
            source.as_ref(),
            count,
            0,
        );
        assert!(events.iter().all(|e| e.event_type == "journal.imported"
            && e.payload["format"] == format
            && e.ts_orig.at.is_none()));
        assert_eq!(
            events
                .iter()
                .map(|e| e.payload["text"].as_str().unwrap())
                .collect::<String>(),
            std::str::from_utf8(bytes).unwrap()
        );
    }
}

#[devkit::test]
fn declared_home_is_used_and_an_unknown_home_stays_unavailable() {
    let dir = tempfile::tempdir().unwrap();
    let parent = dir.path().join("main.journal");
    std::fs::write(&parent, "").unwrap();
    let mut settings = Settings::new("example-host");
    settings.options = json!({"include_home":dir.path()});
    assert_eq!(
        resolve(&parent, "~/child.journal", &settings).unwrap(),
        vec![dir.path().join("child.journal")]
    );
    settings.options = json!({"include_home":null});
    assert!(
        resolve(&parent, "~/child.journal", &settings)
            .unwrap_err()
            .contains("unavailable")
    );
}

#[devkit::test]
fn glob_dot_names_are_matched_at_their_own_path_component() {
    let dir = tempfile::tempdir().unwrap();
    let parent = dir.path().join("main.journal");
    std::fs::write(&parent, "").unwrap();
    for name in ["visible/.child", ".hidden/.child"] {
        std::fs::create_dir_all(dir.path().join(name)).unwrap();
        std::fs::write(dir.path().join(name).join("leaf.journal"), "; leaf\n").unwrap();
    }
    assert_eq!(
        resolve(
            &parent,
            "*/.child/*.journal",
            &Settings::new("example-host")
        )
        .unwrap(),
        vec![dir.path().join("visible/.child/leaf.journal")]
    );
    assert_eq!(
        resolve(&parent, "**.journal", &Settings::new("example-host")).unwrap(),
        vec![dir.path().join("visible/.child/leaf.journal")]
    );
}

#[devkit::test]
fn parsed_include_records_keep_one_resolution_list_per_duplicate() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("main.journal");
    let raw = b"include child.journal\ninclude child.journal\ninclude missing.journal\n";
    std::fs::write(&root, raw).unwrap();
    std::fs::write(dir.path().join("child.journal"), "; child\n").unwrap();
    let bindings = plan(&root, &Settings::new("example-host")).unwrap();
    let binding = bindings.iter().find(|b| b.input == root).unwrap();
    let source = (super::super::FORMAT.source)(binding).unwrap();
    let events = super::super::tests::intake::export_restart(
        raw,
        super::super::driver(root, "example-host".into()),
        source.as_ref(),
        3,
        0,
    );
    assert!(
        events
            .iter()
            .all(|e| e.payload["includes"].as_array().unwrap().len() == 1)
    );
    assert!(events[0].payload["includes"][0]["reason"].is_null());
    assert_eq!(events[1].payload["duplicate"], 1);
    assert!(events[2].payload["includes"][0]["reason"].is_string());
}
