use super::*;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::process::Command;

const BODY: &[u8] = b"a tiny receiver-installed plain blob\n";
const PRIVATE: &[u8] = b"existing private bytes stay private\n";
const CLIENT: u32 = 60001;
const RECEIVER: u32 = 60002;
const OUTSIDER: u32 = 60003;
const GROUP: u32 = 60010;

fn name(bytes: &[u8]) -> String {
    format!("blake3:{}", blake3::hash(bytes).to_hex())
}
fn stored(root: &Path, name: &str) -> PathBuf {
    let hex = parse(name).unwrap().hex();
    root.join("blobs")
        .join(&hex[..2])
        .join(format!("{hex}.zst"))
}
fn read(root: &Path, bytes: &[u8]) -> hashed_blobs::Result<Vec<u8>> {
    let reader = hashed_blobs::Reader::new(root, vec![]);
    let mut body = reader.read_plain(
        &parse(&name(bytes)).unwrap(),
        Some(bytes.len() as u64),
        || Ok(()),
    )?;
    let mut out = Vec::new();
    body.read_to_end(&mut out)?;
    Ok(out)
}

#[devkit::test]
fn protected_group_mode_keeps_plain_encoding_and_sealed_key_checks() {
    let shared = tempfile::tempdir().unwrap();
    let private = tempfile::tempdir().unwrap();
    std::fs::set_permissions(shared.path(), std::fs::Permissions::from_mode(0o2770)).unwrap();
    let blobs = Blobs::open(shared.path()).unwrap();
    let hash = name(BODY);
    assert!(!blobs.contains(&hash).unwrap());
    assert_eq!(blobs.put(BODY).unwrap(), hash);
    assert_eq!(
        std::fs::metadata(stored(shared.path(), &hash))
            .unwrap()
            .mode()
            & 0o777,
        0o640
    );
    assert_eq!(read(shared.path(), BODY).unwrap(), BODY);
    let other = Blobs::open(private.path()).unwrap();
    assert_eq!(other.put(BODY).unwrap(), hash);
    assert_eq!(
        std::fs::metadata(stored(private.path(), &hash))
            .unwrap()
            .mode()
            & 0o777,
        0o600
    );
    assert_eq!(
        std::fs::read(stored(shared.path(), &hash)).unwrap(),
        std::fs::read(stored(private.path(), &hash)).unwrap()
    );
    let key = crate::sealed::DataKey {
        key_ref: "synthetic".into(),
        key: [7; 32],
    };
    let sealed = blobs.put_sealed(BODY, &key).unwrap();
    let reader = hashed_blobs::Reader::new(shared.path(), vec![]);
    assert!(
        reader
            .read_plain(&parse(&sealed).unwrap(), None, || Ok(()))
            .is_err()
    );
    assert!(reader.read(&parse(&sealed).unwrap()).is_err());
    let mut opened = blobs
        .read_sealed(
            &sealed,
            Some(BODY.len() as u64),
            &key,
            SealedReadLimits {
                frame_bytes: BLOB_FRAME_BYTES,
                index_bytes: 65536,
            },
            || Ok(()),
        )
        .unwrap();
    let mut bytes = Vec::new();
    opened.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, BODY);
}

#[devkit::test]
fn existing_private_dedup_destination_is_never_chmodded_or_rewritten() {
    let root = tempfile::tempdir().unwrap();
    let before = Blobs::open(root.path()).unwrap().put(BODY).unwrap();
    let path = stored(root.path(), &before);
    let original = std::fs::metadata(&path).unwrap();
    let encoded = std::fs::read(&path).unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o2770)).unwrap();
    let shared = Blobs::open(root.path()).unwrap();
    assert_eq!(shared.put(BODY).unwrap(), before);
    assert_eq!(shared.put(BODY).unwrap(), before);
    let after = std::fs::metadata(&path).unwrap();
    assert_eq!(after.mode() & 0o777, 0o600);
    assert_eq!(after.ino(), original.ino());
    assert_eq!(std::fs::read(path).unwrap(), encoded);
}

fn actor(operation: &str) {
    let uid: u32 = std::env::var("SINEX_FIXTURE_UID").unwrap().parse().unwrap();
    assert_eq!(
        String::from_utf8(Command::new("id").arg("-u").output().unwrap().stdout)
            .unwrap()
            .trim(),
        uid.to_string()
    );
    let groups = Command::new("id").arg("-G").output().unwrap();
    let mut groups: Vec<u32> = String::from_utf8(groups.stdout)
        .unwrap()
        .split_whitespace()
        .map(|n| n.parse().unwrap())
        .collect();
    groups.sort_unstable();
    groups.dedup();
    assert_eq!(
        groups,
        if uid == CLIENT {
            vec![CLIENT, GROUP]
        } else if uid == RECEIVER {
            vec![GROUP]
        } else {
            vec![OUTSIDER]
        }
    );
    let shared = Path::new("/proc/self/fd/0/shared");
    let private = Path::new("/proc/self/fd/0/private");
    match operation {
        "install" => {
            let blobs = Blobs::open(shared).unwrap();
            assert_eq!(blobs.put(BODY).unwrap(), name(BODY));
            // A file already in the store, written under the private configuration.
            let private_store = hashed_blobs::Store::open(shared, vec![], 0o600).unwrap();
            private_store.put(PRIVATE, 3).unwrap();
            let old = std::fs::metadata(stored(shared, &name(PRIVATE))).unwrap();
            assert_eq!(blobs.put(PRIVATE).unwrap(), name(PRIVATE));
            let after = std::fs::metadata(stored(shared, &name(PRIVATE))).unwrap();
            assert_eq!((after.ino(), after.mode() & 0o777), (old.ino(), 0o600));
            Blobs::open(private).unwrap().put(BODY).unwrap();
            assert_eq!(read(shared, BODY).unwrap(), BODY);
            assert_eq!(read(shared, PRIVATE).unwrap(), PRIVATE);
            assert_eq!(read(private, BODY).unwrap(), BODY);
        }
        "read" => {
            assert_eq!(read(shared, BODY).unwrap(), BODY);
            assert_eq!(
                std::fs::metadata(stored(shared, &name(BODY)))
                    .unwrap()
                    .mode()
                    & 0o777,
                0o640
            );
            assert!(
                read(shared, PRIVATE).is_err(),
                "existing private files were not repaired"
            );
            assert!(read(private, BODY).is_err());
        }
        "denied" => {
            assert!(read(shared, BODY).is_err());
            assert!(read(shared, PRIVATE).is_err());
            assert!(read(private, BODY).is_err());
        }
        _ => panic!("unknown synthetic readback operation"),
    }
}

fn run_actor(root: &Path, operation: &str, uid: u32, gid: u32) {
    let groups = if uid == CLIENT {
        format!("--groups={GROUP}")
    } else {
        "--clear-groups".into()
    };
    let test = format!(
        "{}::configured_group_plain_readback_between_distinct_users",
        module_path!().split_once("::").unwrap().1
    );
    let output = Command::new("/run/current-system/sw/bin/setpriv")
        .args([
            "--clear-groups",
            "--",
            "/run/current-system/sw/bin/setpriv",
            "--reuid",
            &uid.to_string(),
            "--regid",
            &gid.to_string(),
            &groups,
            "--",
            "./actor",
            "--exact",
            &test,
            "--ignored",
            "--nocapture",
        ])
        .current_dir(root)
        .stdin(std::fs::File::open(root).unwrap())
        .env("SINEX_FIXTURE_ACTOR", operation)
        .env("SINEX_FIXTURE_UID", uid.to_string())
        .output()
        .unwrap();
    assert!(
        output.status.success()
            && String::from_utf8_lossy(&output.stdout)
                .contains("test result: ok. 1 passed; 0 failed"),
        "{operation} as {uid}: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[devkit::test]
#[ignore = "requires root for isolated numeric-UID and group filesystem checks"]
fn configured_group_plain_readback_between_distinct_users() {
    if let Ok(operation) = std::env::var("SINEX_FIXTURE_ACTOR") {
        return actor(&operation);
    }
    assert_eq!(
        String::from_utf8(Command::new("id").arg("-u").output().unwrap().stdout)
            .unwrap()
            .trim(),
        "0"
    );
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o711)).unwrap();
    let binary = root.path().join("actor");
    std::fs::copy(std::env::current_exe().unwrap(), &binary).unwrap();
    std::fs::set_permissions(binary, std::fs::Permissions::from_mode(0o755)).unwrap();
    for (name, mode) in [("shared", 0o2770), ("private", 0o700)] {
        let path = root.path().join(name);
        std::fs::create_dir(&path).unwrap();
        std::os::unix::fs::chown(&path, Some(RECEIVER), Some(GROUP)).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    }
    run_actor(root.path(), "install", RECEIVER, GROUP);
    run_actor(root.path(), "read", CLIENT, CLIENT);
    run_actor(root.path(), "denied", OUTSIDER, OUTSIDER);
}
