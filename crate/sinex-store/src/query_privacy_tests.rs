use super::apply_record_privacy;
use crate::{Error, PrivacyRule};
use serde_json::json;

fn rule(pattern: &str, replacement: &str) -> PrivacyRule {
    PrivacyRule {
        pattern: pattern.into(),
        replacement: replacement.into(),
        created_by: "operator".into(),
    }
}

#[test]
fn raw_projection_keeps_attributed_order_and_skips_only_exact_attributed_tuples() {
    let once = rule("amber", "amber!");
    let mut record = json!({"id":"fixture", "command":"echo amber"});
    apply_record_privacy(
        &mut record,
        std::slice::from_ref(&once),
        std::slice::from_ref(&once),
    )
    .unwrap();
    assert_eq!(record["command"], "echo amber!");
    // Repetition inside the attributed sequence is authored and stays meaningful.
    let mut record = json!({"command":"echo amber"});
    apply_record_privacy(
        &mut record,
        &[once.clone(), once.clone()],
        std::slice::from_ref(&once),
    )
    .unwrap();
    assert_eq!(record["command"], "echo amber!!");
    // Different replacements on the same pattern are distinct tuples, in order.
    let mut record = json!({"command":"echo amber"});
    apply_record_privacy(
        &mut record,
        std::slice::from_ref(&once),
        &[rule("amber", "azure")],
    )
    .unwrap();
    assert_eq!(record["command"], "echo azure!");
    // Additional current duplicates remain a sequence, not a normalized set.
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
fn raw_projection_protects_old_untransformed_evidence_without_rewriting_source() {
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
fn raw_projection_refuses_changed_opaque_source_selectors_and_hashes() {
    for record in [
        json!({"id":"amber", "command":"echo plain"}),
        json!({"session":"amber", "command":"echo plain"}),
        json!({"original_blake3":"amber", "unmetafied_record":"echo plain"}),
        json!({"_from":{"table":"amber"}, "command":"echo plain"}),
    ] {
        for attributed in [true, false] {
            let rules = [rule("amber", "amber!")];
            let result = if attributed {
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
