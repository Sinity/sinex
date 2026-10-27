use super::*;

#[devkit::test]
fn the_user_is_taken_out_and_decoded() {
    assert_eq!(
        split("nats://session%2Dx@127.0.0.1:4222").unwrap(),
        ("nats://127.0.0.1:4222".into(), Some("session-x".into()))
    );
}

#[devkit::test]
fn a_url_without_a_user_is_unchanged() {
    assert_eq!(
        split("nats://127.0.0.1:4222").unwrap(),
        ("nats://127.0.0.1:4222".into(), None)
    );
    assert_eq!(
        split("127.0.0.1:4222").unwrap(),
        ("127.0.0.1:4222".into(), None)
    );
}

#[devkit::test]
fn a_password_in_the_url_is_refused_without_showing_it() {
    let error = split("nats://sinexd:s3cret@127.0.0.1:4222").unwrap_err();
    assert!(error.contains("--nats-password-file"), "{error}");
    assert!(!error.contains("s3cret"));
}

#[devkit::test]
fn a_user_needs_its_password_file_and_a_file_needs_a_user() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("password");
    std::fs::write(&file, "daemon secret: with @ and /\n").unwrap();
    assert!(options("nats://sinexd@host:4222", Some(&file)).is_ok());
    assert!(options("nats://sinexd@host:4222", None).is_err());
    assert!(options("nats://host:4222", Some(&file)).is_err());
    assert!(options("nats://host:4222", None).is_ok());
    let missing = dir.path().join("missing");
    assert!(options("nats://sinexd@host:4222", Some(&missing)).is_err());
}

#[devkit::test]
fn a_bad_escape_is_kept() {
    assert_eq!(decode("a%zzb%4"), "a%zzb%4");
    assert_eq!(decode("%41"), "A");
}
