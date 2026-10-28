use super::*;
use std::os::unix::fs::PermissionsExt;

#[devkit::test]
async fn notifier_receives_the_binding_and_exact_refusal_as_one_argument() {
    let dir = tempfile::tempdir().unwrap();
    let program = dir.path().join("busctl");
    std::fs::write(
        &program,
        "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$(dirname \"$0\")/arguments\"\n",
    )
    .unwrap();
    std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(send(program.as_os_str(), "synthetic", "unknown type; $(false)").await);
    let arguments = std::fs::read_to_string(dir.path().join("arguments")).unwrap();
    assert!(arguments.contains("Notify\nsusssasa{sv}i\n"));
    assert!(arguments.contains("synthetic: unknown type; $(false)\n"));
    assert!(
        !send(
            dir.path().join("missing").as_os_str(),
            "synthetic",
            "refused"
        )
        .await
    );
}
