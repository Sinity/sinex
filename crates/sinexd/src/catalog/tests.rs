#[devkit::test]
fn committed_catalog_is_current() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../nix/catalog.json");
    let committed = std::fs::read_to_string(path).unwrap_or_default();
    assert!(
        committed == super::render(),
        "nix/catalog.json is stale; regenerate it with `cargo xtask db exec -- \
         bash -c 'cargo run --locked --quiet -p sinexd -- admin catalog > nix/catalog.json'`"
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
                source["expect"]["kind"] == "on_demand"
                    || !description["streams"].as_array().unwrap().is_empty()
                    || !description["locations"].as_array().unwrap().is_empty(),
                "{}",
                source["name"]
            );
            assert!(
                description["captured_by"] == "system" || description["captured_by"] == "session"
            );
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

/// A deployed database keeps every registered kind version's definition and
/// every source version's declaration unchanged, and refuses a build that
/// declares one differently. `registered.txt` lists their hashes as the
/// deployed database stored them; a test database is created fresh from this
/// build, so this list is what shows such a change before deployment. A doc
/// comment on a payload type is part of its kind's schema: rewording one, or
/// renaming a type its schema names, needs a new kind version.
#[devkit::test]
fn built_in_kinds_and_sources_match_their_registered_versions() {
    let kinds: std::collections::BTreeMap<(String, u64), String> =
        sinex_model::event_types::declarations()
            .iter()
            .map(|d| ((d.kind.name.clone(), d.kind.version), d.definition_hash()))
            .collect();
    let sources: std::collections::BTreeMap<(String, u64), String> = super::source_declarations()
        .iter()
        .map(|d| {
            (
                (d.source.name.clone(), u64::from(d.source.version)),
                d.fingerprint(),
            )
        })
        .collect();
    let mut differing = Vec::new();
    for line in include_str!("registered.txt").lines() {
        let [what, name, version, hash] = line.split(' ').collect::<Vec<_>>()[..] else {
            panic!("malformed line {line:?}");
        };
        let key = (name.to_owned(), version.parse::<u64>().unwrap());
        let declared = match what {
            "kind" => kinds.get(&key),
            "source" => sources.get(&key),
            _ => panic!("malformed line {line:?}"),
        };
        if declared.is_some_and(|declared| declared != hash) {
            differing.push(format!("{what} {name} version {version}"));
        }
    }
    assert!(
        differing.is_empty(),
        "declared differently from the registered version: {differing:?}"
    );
}
