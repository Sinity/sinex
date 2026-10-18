use super::{Member, merge_group, rule};
use serde_json::{Value, json};
use uuid::Uuid;

fn member(source: i16, quality: &str, at_ns: Option<i128>, digest: &str, anchor: i64) -> Member {
    Member {
        id: Uuid::from_u128(u128::from(anchor as u64) + 1),
        source,
        ekey: format!("key-{source}-{anchor}"),
        digest: digest.to_owned(),
        anchor,
        at_ns,
        count2: 1,
        count60: 1,
        payload: Value::Null,
        quality: quality.to_owned(),
    }
}

#[test]
fn time_source_prefers_known_time_then_quality_source_digest_and_anchor() {
    let rank = |m: Member| {
        let (unknown, quality, source, digest, anchor) = m.time_rank();
        (unknown, quality, source, digest.to_owned(), anchor)
    };
    let known = Some(0);
    assert!(
        rank(member(0, "intrinsic", known, "ff", 80))
            < rank(member(1, "intrinsic", known, "00", 0))
    );
    assert!(
        rank(member(1, "captured", known, "ff", 80)) < rank(member(0, "intrinsic", known, "00", 0))
    );
    assert!(
        rank(member(1, "intrinsic", known, "01", 80))
            < rank(member(1, "intrinsic", known, "02", 0))
    );
    assert!(
        rank(member(1, "intrinsic", known, "01", 9))
            < rank(member(1, "intrinsic", known, "01", 10))
    );
    assert!(
        rank(member(1, "staged", known, "ff", 80)) < rank(member(0, "captured", None, "00", 0))
    );
}

#[test]
fn merged_pair_takes_preferred_fields_and_records_conflicts() {
    let mut atuin = member(0, "intrinsic", Some(7_200_000_000_000), "aa", 1);
    atuin.payload =
        json!({"command": "git status", "cwd": "/w", "timestamp_ns": 1, "hostname": "h"});
    let mut zsh = member(1, "captured", Some(3_600_000_000_000), "bb", 2);
    zsh.payload = json!({"command": "git  status", "cwd": null, "timestamp_ns": 2, "hostname": "h",
                         "continuity": "gap"});
    let merged = merge_group(&rule(), &[atuin, zsh], 2).unwrap();
    let (a, z) = (Uuid::from_u128(2), Uuid::from_u128(3));
    let p = &merged.payload;
    assert_eq!(p["command"], "git status");
    assert_eq!(p["cwd"], "/w");
    assert_eq!(p["canonical"], "git status");
    assert_eq!(p["timestamp_ns"], 2, "captured time beats intrinsic");
    assert_eq!(merged.time_parent, z);
    assert_eq!(p["owner_hour"], 1);
    assert_eq!(p["field_evidence"]["command"], json!(a));
    assert_eq!(p["field_evidence"]["time"], json!(z));
    assert!(p["field_evidence"].get("all").is_none());
    assert_eq!(
        p["conflicting_fields"],
        json!({"command": [{"parent": z, "value": "git  status"}]})
    );
    assert_eq!(p["association"]["strategy"], "window-2s");
    assert_eq!(p["association"]["basis"], "rule");
    assert_eq!(
        p["source_continuity"],
        json!([{"parent": z, "continuity": "gap"}])
    );
    assert_eq!(merged.key, r#"["shell-command",["key-0-1","key-1-2"]]"#);
}

#[test]
fn merge_refuses_two_members_from_one_source() {
    let result = merge_group(
        &rule(),
        &[
            member(0, "intrinsic", None, "aa", 1),
            member(0, "intrinsic", None, "bb", 2),
        ],
        0,
    );
    assert!(matches!(
        result,
        Err(crate::Error::Invalid("shell group repeats one source"))
    ));
}
