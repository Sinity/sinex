use super::declarations::{atuin_payload_schema_at, fingerprint_at, first_release_shell_payload};
use super::upgrade::dump_environment;

#[test]
fn dump_gets_decoded_connection_fields_through_its_environment() {
    let fields:std::collections::BTreeMap<_,_>=dump_environment("postgresql://synthetic:p%40ss%2Bword@localhost/synthetic%20db?host=%2Fsynthetic%2Fsocket&port=55431&ssl-mode=verify-full&ssl-ca=%2Fsynthetic%2Fca").unwrap().into_iter().collect();
    assert_eq!(fields["PGDATABASE"], "synthetic db");
    assert_eq!(fields["PGHOST"], "/synthetic/socket");
    assert_eq!(fields["PGPORT"], "55431");
    assert_eq!(fields["PGPASSWORD"], "p@ss+word");
    assert_eq!(fields["PGSSLMODE"], "verify-full");
    assert_eq!(fields["PGSSLROOTCERT"], "/synthetic/ca");
}
#[test]
fn versions_five_and_six_keep_their_fingerprints() {
    assert_eq!(
        fingerprint_at(5).unwrap(),
        "6ccf124f6bc31bb4c86522d1585e98a173b3b35c2db2205ff786dbf2909e17d7"
    );
    assert_eq!(
        fingerprint_at(6).unwrap(),
        "2f7b2a8789b6b7aadb0f2d6b06b1272d614d37e23cb480ce1455c1beeadee773"
    );
    assert_eq!(
        atuin_payload_schema_at(5)["properties"]["exit_code"]["minimum"],
        0
    );
    assert!(
        atuin_payload_schema_at(6)["properties"]["exit_code"]
            .get("minimum")
            .is_none()
    );
    assert_eq!(
        first_release_shell_payload()["properties"]["exit_code"]["minimum"],
        0
    );
}
#[test]
fn version_four_keeps_its_fingerprint() {
    assert_eq!(
        fingerprint_at(4).unwrap(),
        "5e611ef17edcc8ced01df352bf993ad3c79f2fb0c3560f2a0bfe5a1a6e277c45"
    );
}
#[test]
fn first_release_keeps_its_fingerprint() {
    // Computed by the published 4b5bf0619 package.
    assert_eq!(
        fingerprint_at(0).unwrap(),
        "2e82b6514b2a7104dbed73a52dfac34e89fbec9e99e9adb0af10e05604c41a9b"
    );
}
#[test]
fn current_version_keeps_its_fingerprint() {
    // Update only together with a new schema change in `upgrade.rs`.
    assert_eq!(
        fingerprint_at(7).unwrap(),
        "2eae36cf4db542714cd68d2082d0bec30acb99487cbf94ac4873371cdf990d3f"
    );
}
