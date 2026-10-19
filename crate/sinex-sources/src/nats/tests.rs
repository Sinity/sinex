use super::*;

#[test]
fn credentials_are_taken_out_and_decoded() {
    let (server, creds) =
        split("nats://sinexd:daemon%20secret%3A%20with%20%40%20and%20%2F@127.0.0.1:4222");
    assert_eq!(server, "nats://127.0.0.1:4222");
    assert_eq!(
        creds,
        Some(("sinexd".into(), "daemon secret: with @ and /".into()))
    );
}

#[test]
fn a_url_without_credentials_is_unchanged() {
    assert_eq!(
        split("nats://127.0.0.1:4222"),
        ("nats://127.0.0.1:4222".into(), None)
    );
    assert_eq!(split("127.0.0.1:4222"), ("127.0.0.1:4222".into(), None));
}

#[test]
fn messages_never_show_the_password() {
    let shown = redacted("nats://session-x:s3cret@host:4222");
    assert_eq!(shown, "nats://session-x:***@host:4222");
    assert!(!shown.contains("s3cret"));
}

#[test]
fn a_bad_escape_is_kept() {
    assert_eq!(decode("a%zzb%4"), "a%zzb%4");
    assert_eq!(decode("%41"), "A");
}
