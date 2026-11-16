use super::*;
use std::io::Write;

#[devkit::test]
fn library_media_write_is_synced_before_rename() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("media");
    std::fs::write(&path, b"old media").unwrap();
    let error = write_replacing(&path, 0o600, |file| {
        file.write_all(b"partial media")?;
        Err(io::Error::other("incomplete media"))
    })
    .unwrap_err();
    assert_eq!(error.0, ReplaceStep::Write);
    assert_eq!(std::fs::read(&path).unwrap(), b"old media");
    write_replacing(&path, 0o600, |file| file.write_all(b"new media")).unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), b"new media");
    let staged = directory.path().join("recording");
    std::fs::write(&staged, b"read only media").unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o400)).unwrap();
    place(&staged, &path).unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), b"read only media");
}
