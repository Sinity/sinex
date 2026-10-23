use super::*;
use serde_json::json;

#[test]
fn masks_every_string_and_keeps_the_rest() {
    let secret = format!("ghp_{}", "a".repeat(36));
    let v = mask_json(
        json!({"cmd": format!("git push https://x:{secret}@github.com"), "n": 3, "ok": true}),
    )
    .unwrap();
    let cmd = v["cmd"].as_str().unwrap();
    assert!(!cmd.contains(&secret), "{cmd}");
    assert!(cmd.starts_with("git push"));
    assert_eq!(v["n"], json!(3));
    assert_eq!(mask_str("ls -la"), "ls -la");
}

#[test]
fn crossing_quoted_values_are_masked_and_nested_credential_keys_refuse() {
    for quote in ['"', '\''] {
        let text =
            format!("--secret={quote}unfinished --token={quote}synthetic-later-value{quote} tail");
        let raw = json!({"nested": [{"value": text}], "empty": "", "unknown": null});
        let output = mask_json(raw.clone()).unwrap();
        assert_eq!(
            output["nested"][0]["value"],
            "--secret=[credential: CLI secret, masked] tail"
        );
        assert_eq!(output["empty"], "");
        assert_eq!(output["unknown"], Value::Null);
        assert_eq!(raw["nested"][0]["value"], text);
        let raw = json!({"nested": [{text.clone(): 1, "ordinary": 2}]});
        let reason = mask_json(raw.clone()).unwrap_err();
        assert_eq!(reason, "result contains a credential-shaped object key");
        assert!(!reason.contains("synthetic-later-value"));
        assert_eq!(raw["nested"][0][&text], 1);
    }
}
