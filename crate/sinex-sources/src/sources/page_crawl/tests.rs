use super::*;

#[test]
fn allow_list_matches_hosts_and_subdomains_only() {
    let allow = vec!["wikipedia.org".to_owned()];
    assert!(allowed("en.wikipedia.org", &allow));
    assert!(allowed("wikipedia.org", &allow));
    assert!(!allowed("notwikipedia.org", &allow));
    assert!(!allowed("example.com", &allow));
}

#[test]
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

#[test]
fn robots_longest_match_wins_and_other_agents_are_ignored() {
    let r = Robots::parse(
        "User-agent: Googlebot\nDisallow: /\n\nUser-agent: *\nDisallow: /private\nAllow: /private/ok\n",
    );
    assert!(r.permits("/wiki"));
    assert!(!r.permits("/private/x"));
    assert!(r.permits("/private/ok/y"));
    assert!(Robots::parse("").permits("/anything"));
}

#[test]
fn titles_are_read_from_html() {
    assert_eq!(
        title(b"<html><head><TITLE>\n A &amp; B </title></head>"),
        Some("A & B".into())
    );
    assert_eq!(title(b"no title"), None);
}
