#[devkit::test]
fn committed_catalog_is_current() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../nix/catalog.json");
    let committed = std::fs::read_to_string(path).unwrap_or_default();
    assert!(
        committed == super::render(),
        "nix/catalog.json is stale; regenerate it with \
         `cargo run -p sinexd -- admin catalog > nix/catalog.json`"
    );
}

#[devkit::test]
fn every_source_has_discovery_and_shared_formats_keep_their_descriptions() {
    let catalog = super::catalog();
    let sources = catalog["sources"].as_array().unwrap();
    for source in sources {
        let descriptions = source["discovery"].as_array().unwrap();
        assert_eq!(
            descriptions.len(),
            source["formats"].as_array().unwrap().len()
        );
        for discovery in descriptions {
            let description = &discovery["description"];
            assert!(
                description["request_only"] == true
                    || !description["streams"].as_array().unwrap().is_empty()
                    || !description["locations"].as_array().unwrap().is_empty(),
                "{}",
                source["name"]
            );
            assert!(description["owner"] == "system" || description["owner"] == "session");
        }
    }
    let clipboard = sources
        .iter()
        .find(|source| source["name"] == "desktop.clipboard")
        .unwrap();
    assert!(
        clipboard["discovery"]
            .as_array()
            .unwrap()
            .iter()
            .any(|description| description["format"] == "clipboard")
    );
    assert!(
        clipboard["discovery"]
            .as_array()
            .unwrap()
            .iter()
            .any(|description| description["format"] == "sinnix-lake")
    );
}
