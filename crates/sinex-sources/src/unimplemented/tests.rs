use super::*;

#[devkit::test]
fn unimplemented_census_lists_unbuilt_sources_whose_data_the_host_has() {
    let root = tempfile::tempdir().unwrap();
    let context = DiscoveryContext {
        home: Some(root.path().into()),
        data_home: Some(root.path().join("data")),
        config_home: Some(root.path().join("config")),
        ..Default::default()
    };
    assert!(census(&context).is_empty());
    let profiles = root.path().join(".mozilla/firefox");
    std::fs::create_dir_all(&profiles).unwrap();
    let history = root.path().join("config/nushell/history.txt");
    std::fs::create_dir_all(history.parent().unwrap()).unwrap();
    std::fs::write(&history, b"ls\n").unwrap();
    assert_eq!(
        census(&context),
        vec![
            Applicable {
                name: "terminal.nushell-history",
                purpose: "shell recall",
                found: vec![history],
            },
            Applicable {
                name: "browser.history.firefox",
                purpose: "browsing history and bookmarks",
                found: vec![profiles],
            },
        ]
    );
}

#[devkit::test]
fn unimplemented_census_never_names_a_built_source() {
    for unbuilt in UNIMPLEMENTED {
        assert!(
            crate::sources::spec(unbuilt.name).is_none(),
            "{} is built; remove it from the census",
            unbuilt.name
        );
        assert!(!unbuilt.locations.is_empty(), "{}", unbuilt.name);
    }
}
