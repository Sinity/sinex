use super::*;
use crate::PrivacyRule;

#[test]
fn parse_keeps_and_inside_quoted_values_and_rejects_unknown_syntax() {
    assert!(
        parse(r#"events where payload.command = "echo a and b" and payload.exit_code = 0"#).is_ok()
    );
    assert!(parse("events where kind = x;DROP").is_ok()); // A literal value is bound data.
    for bad in [
        "commands",
        "events where typo = 0",
        "events where payload.exit_code = \"0\"",
        "events where when = no",
        "events where source = x or kind = x",
    ] {
        assert!(parse(bad).is_err(), "{bad}");
    }
}

#[test]
fn query_digest_ignores_order_and_quoting_but_not_values() {
    let digest = |text| query_digest(&parse(text).unwrap()).unwrap();
    assert_eq!(
        digest("events where source = x and kind = y"),
        digest(" events where kind = \"y\" and source = \"x\" and source = x ")
    );
    assert_eq!(
        digest("events where ts_orig = 2023-11-14T22:13:20.123456789Z"),
        digest("events where when = 2023-11-14T23:13:20.123456789+01:00")
    );
    assert_ne!(
        digest("events where when = 2023-11-14T22:13:20.123456789Z"),
        digest("events where when = 2023-11-14T22:13:20.123456788Z")
    );
    assert_ne!(
        digest("events where payload.command ~ x"),
        digest("events where payload.command = x")
    );
    assert_ne!(
        digest("events where source = x"),
        digest("events where source = X")
    );
}

#[test]
fn cursor_rejects_other_queries_expiry_and_edited_fields() {
    let clock = OffsetDateTime::parse("2026-10-15T12:00:00Z", &Rfc3339).unwrap();
    let cursor = Cursor {
        version: CURSOR_VERSION,
        order: CURSOR_ORDER.into(),
        query: "question".into(),
        now: clock.format(&Rfc3339).unwrap(),
        zone: "UTC".into(),
        logical_start: "04:00".into(),
        first_commit_seq: 5,
        expires_at: (clock + CURSOR_LIFETIME).format(&Rfc3339).unwrap(),
        last_when: None,
        last_id: "019a54a0-0000-7000-8000-000000000001".into(),
    };
    let raw = serde_json::to_string(&cursor).unwrap();
    assert!(Cursor::decode(&raw, "question", 6, clock).is_ok());
    assert!(Cursor::decode(&raw, "different", 6, clock).is_err());
    assert!(Cursor::decode(&raw, "question", 4, clock).is_err());
    assert!(Cursor::decode(&raw, "question", 6, clock + CURSOR_LIFETIME).is_err());
    assert!(Cursor::decode(&raw, "question", 6, clock - Duration::seconds(1)).is_err());
    let valid = serde_json::to_value(&cursor).unwrap();
    for (field, value) in [
        ("version", json!(CURSOR_VERSION + 1)),
        ("zone", json!("Europe/Warsaw")),
        ("logical_start", json!("00:00")),
        ("first_commit_seq", json!(-1)),
        ("order", json!("other")),
        ("last_id", json!("bad")),
        ("last_when", json!("2023-11-14T22:13:20Z")),
        ("expires_at", json!("2026-10-18T12:00:00Z")),
    ] {
        let mut changed = valid.clone();
        changed[field] = value;
        assert!(
            Cursor::decode(&changed.to_string(), "question", 6, clock).is_err(),
            "{field}"
        );
    }
    // last_when is required even when null.
    let mut missing = valid.clone();
    missing.as_object_mut().unwrap().remove("last_when");
    assert!(Cursor::decode(&missing.to_string(), "question", 6, clock).is_err());
    let mut known = valid.clone();
    known["last_when"] = json!("2023-11-14T22:13:20.123456789Z");
    assert!(Cursor::decode(&known.to_string(), "question", 6, clock).is_ok());
    assert!(Cursor::decode(r#"{"query":"question","last_when":null,"last_id":"019a54a0-0000-7000-8000-000000000001"}"#, "question", 6, clock).is_err());
}

fn rule(pattern: &str, replacement: &str) -> PrivacyRule {
    PrivacyRule {
        pattern: pattern.into(),
        replacement: replacement.into(),
        created_by: "operator".into(),
    }
}

#[test]
fn record_masking_applies_stored_rules_then_new_current_rules_in_order() {
    let once = rule("amber", "amber!");
    let mut record = json!({"id":"fixture", "command":"echo amber"});
    apply_record_privacy(
        &mut record,
        std::slice::from_ref(&once),
        std::slice::from_ref(&once),
    )
    .unwrap();
    assert_eq!(record["command"], "echo amber!");
    // A rule repeated in the stored list applies twice.
    let mut record = json!({"command":"echo amber"});
    apply_record_privacy(
        &mut record,
        &[once.clone(), once.clone()],
        std::slice::from_ref(&once),
    )
    .unwrap();
    assert_eq!(record["command"], "echo amber!!");
    // Different replacements on the same pattern are distinct rules, in order.
    let mut record = json!({"command":"echo amber"});
    apply_record_privacy(
        &mut record,
        std::slice::from_ref(&once),
        &[rule("amber", "azure")],
    )
    .unwrap();
    assert_eq!(record["command"], "echo azure!");
    // Duplicates among the current rules also apply each time.
    let double = rule("!", "!!");
    let mut record = json!({"command":"echo amber"});
    apply_record_privacy(
        &mut record,
        std::slice::from_ref(&once),
        &[once.clone(), double.clone(), double],
    )
    .unwrap();
    assert_eq!(record["command"], "echo amber!!!!");
}

#[test]
fn record_masking_treats_replacement_literally_and_copies_the_record() {
    let original = json!({"id":"fixture", "command":"echo amber", "_from":{"table":"history"}});
    let mut record = original.clone();
    let replacement = rule("(amber)", "$1amber");
    replacement.validate().unwrap();
    apply_record_privacy(&mut record, &[], &[replacement]).unwrap();
    assert_eq!(record["command"], "echo $1amber");
    assert_eq!(record["id"], original["id"]);
    assert_eq!(record["_from"], original["_from"]);
    assert_eq!(original["command"], "echo amber");
}

#[test]
fn record_masking_refuses_to_change_identifiers_and_hashes() {
    for record in [
        json!({"id":"amber", "command":"echo plain"}),
        json!({"session":"amber", "command":"echo plain"}),
        json!({"original_blake3":"amber", "unmetafied_record":"echo plain"}),
        json!({"_from":{"table":"amber"}, "command":"echo plain"}),
    ] {
        for stored in [true, false] {
            let rules = [rule("amber", "amber!")];
            let result = if stored {
                apply_record_privacy(&mut record.clone(), &rules, &[])
            } else {
                apply_record_privacy(&mut record.clone(), &[], &rules)
            };
            assert!(matches!(result, Err(Error::ReplayRefused(_))));
        }
    }
}

#[test]
fn chromium_privacy_rules_mask_content_but_keep_keys() {
    let original = json!({"table":"downloads","coordinate":1,"download":{"id":1,"guid":"native-key","target_path":"/downloads/amber.pdf","hash":{"$blob":"deadbeef"},"nonfinite":{"sqlite_real_bits":"7ff0000000000000"}},"url_chain":[{"id":1,"chain_index":0,"url":"https://amber.invalid/"}],"_from":{"observation":"receipt","table":"downloads","rowid":1}});
    let mut shown = original.clone();
    apply_record_privacy(&mut shown, &[rule("amber", "protected")], &[]).unwrap();
    assert_eq!(shown["download"]["target_path"], "/downloads/protected.pdf");
    assert_eq!(shown["_from"], original["_from"]);
    for opaque in ["native-key", "receipt", "deadbeef", "7ff0000000000000"] {
        assert!(matches!(
            apply_record_privacy(&mut original.clone(), &[rule(opaque, "changed")], &[]),
            Err(Error::ReplayRefused(_))
        ));
    }
}

#[test]
fn zsh_record_masking_leaves_hash_and_label_alone() {
    let bytes = b": 1700000000:0;echo utf8 0a\\\nnext\n: 1700000001:0;later\n";
    let provenance = json!({"source": crate::ZSH_SOURCE, "privacy_rules": []});
    // These rules match the label and hash hex digits.
    let rules = [rule("utf8", "x"), rule("[0-9a-f]", "#")];
    let (record, length) = decode_record(bytes, &provenance, Some(&rules)).unwrap();
    assert_eq!(length, bytes.len() - ": 1700000001:0;later\n".len());
    assert_eq!(record["representation"], "zsh-unmetafied-utf8");
    assert_eq!(
        record["original_blake3"],
        blake3::hash(&bytes[..length]).to_hex().to_string()
    );
    assert!(
        !record["unmetafied_record"]
            .as_str()
            .unwrap()
            .contains("utf8")
    );
}
