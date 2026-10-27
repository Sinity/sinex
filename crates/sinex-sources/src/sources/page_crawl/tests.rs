use super::*;

#[devkit::test]
fn allow_list_matches_hosts_and_subdomains_only() {
    let allow = vec!["wikipedia.org".to_owned()];
    assert!(allowed("en.wikipedia.org", &allow));
    assert!(allowed("wikipedia.org", &allow));
    assert!(!allowed("notwikipedia.org", &allow));
    assert!(!allowed("example.com", &allow));
}

#[devkit::test]
fn urls_split_into_scheme_host_and_path() {
    assert_eq!(
        split("https://user@en.wikipedia.org:443/wiki/X?a=1#frag"),
        Some((
            "https".into(),
            "en.wikipedia.org".into(),
            "/wiki/X?a=1".into()
        ))
    );
    assert_eq!(
        split("https://docs.rs"),
        Some(("https".into(), "docs.rs".into(), "/".into()))
    );
    assert_eq!(split("chrome://settings"), None);
}

#[devkit::test]
fn robots_longest_match_wins_and_other_agents_are_ignored() {
    let r = Robots::parse(
        "User-agent: Googlebot\nDisallow: /\n\nUser-agent: *\nDisallow: /private\nAllow: /private/ok\n",
    );
    assert!(r.permits("/wiki"));
    assert!(!r.permits("/private/x"));
    assert!(r.permits("/private/ok/y"));
    assert!(Robots::parse("").permits("/anything"));
}

#[devkit::test]
fn titles_are_read_from_html() {
    assert_eq!(
        title(b"<html><head><TITLE>\n A &amp; B </title></head>"),
        Some("A & B".into())
    );
    assert_eq!(title(b"no title"), None);
}

#[devkit::test]
fn crawl_format_obeys_the_limit_and_requires_an_allow_list() {
    let mut settings = sinex_source_core::Settings::new("synthetic");
    settings.options = serde_json::json!({"urls":["https://example.test/allowed"], "allow":"example.test", "limit":0});
    let mut binding =
        sinex_source_core::Binding::of(&SPEC, std::path::Path::new("crawl"), settings);
    let mut reader = (FORMAT.reader.unwrap())(&binding)
        .unwrap()
        .into_polled()
        .unwrap();
    assert!(
        reader
            .poll(&serde_json::Value::Null)
            .unwrap()
            .material
            .is_none()
    );
    binding
        .settings
        .options
        .as_object_mut()
        .unwrap()
        .remove("allow");
    assert!((FORMAT.reader.unwrap())(&binding).is_err());
}
