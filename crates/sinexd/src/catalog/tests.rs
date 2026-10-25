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
