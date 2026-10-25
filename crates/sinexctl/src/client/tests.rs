use super::*;

#[devkit::test]
fn endpoints() {
    assert!(matches!(
        Endpoint::parse("unix:/run/sinex/s.sock").unwrap(),
        Endpoint::Unix(_)
    ));
    assert!(matches!(
        Endpoint::parse("/run/sinex/s.sock").unwrap(),
        Endpoint::Unix(_)
    ));
    assert!(
        matches!(Endpoint::parse("http://127.0.0.1:7171/").unwrap(), Endpoint::Tcp(h) if h == "127.0.0.1:7171")
    );
    assert!(Endpoint::parse("relative").is_err());
    assert_eq!(percent_encode("a b&c"), "a%20b%26c");
}
