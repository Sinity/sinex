use super::*;
use serde_json::json;

#[test]
fn masks_every_string_and_keeps_the_rest() {
    let secret = format!("ghp_{}", "a".repeat(36));
    let v = mask_json(
        json!({"cmd": format!("git push https://x:{secret}@github.com"), "n": 3, "ok": true}),
    );
    let cmd = v["cmd"].as_str().unwrap();
    assert!(!cmd.contains(&secret), "{cmd}");
    assert!(cmd.starts_with("git push"));
    assert_eq!(v["n"], json!(3));
    assert_eq!(mask_str("ls -la"), "ls -la");
}
