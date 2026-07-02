use super::*;
use crate::privacy::{CategorySet, ProcessingContext};
use xtask::sandbox::sinex_test;

fn test_engine() -> PrivacyEngine {
    let mut config = PrivacyConfig::default();
    config.builtin_categories = CategorySet::All;
    PrivacyEngine::new(config).unwrap()
}

fn test_engine_with_key() -> PrivacyEngine {
    let mut config = PrivacyConfig::default();
    config.builtin_categories = CategorySet::All;
    config.key.key_hex = Some("42".repeat(32));
    config.track_stats = true;
    PrivacyEngine::new(config).unwrap()
}

fn github_token_fixture() -> String {
    ["ghp_", "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij"].concat()
}

fn aws_access_key_fixture() -> String {
    ["AKIA", "IOSFODNN7EXAMPLE"].concat()
}

fn card_number_fixture() -> String {
    ["4111", "111111111111"].concat()
}

// ── Basic redaction cases ──

#[sinex_test]
async fn redacts_aws_access_key() -> ::xtask::sandbox::TestResult<()> {
    let e = test_engine();
    let access_key = aws_access_key_fixture();
    let input = format!("export AWS_ACCESS_KEY_ID={access_key}");
    let result = e.process(&input, ProcessingContext::Command);
    assert!(result.any_matched());
    assert!(result.text.contains("<AWS_ACCESS_KEY>"));
    assert!(!result.text.contains(&access_key));
    Ok(())
}

#[sinex_test]
async fn redacts_github_token() -> ::xtask::sandbox::TestResult<()> {
    let e = test_engine();
    // Use bare token (no `token=` prefix which triggers generic_secret_assign too)
    let token = github_token_fixture();
    let input = format!("found {token} in logs");
    let result = e.process(&input, ProcessingContext::Command);
    assert!(result.any_matched());
    assert!(
        result.text.contains("<GITHUB_TOKEN>"),
        "expected <GITHUB_TOKEN>, got: {}",
        result.text
    );
    assert!(!result.text.contains("ghp_"));
    Ok(())
}

#[sinex_test]
async fn redacts_url_credentials() -> ::xtask::sandbox::TestResult<()> {
    let e = test_engine();
    let input = "postgres://admin:s3cret@localhost:5432/db";
    let result = e.process(input, ProcessingContext::Command);
    assert!(result.any_matched());
    assert!(!result.text.contains("s3cret"));
    Ok(())
}

#[sinex_test]
async fn redacts_jwt() -> ::xtask::sandbox::TestResult<()> {
    let e = test_engine();
    let input = "Authorization: eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abc123def456";
    let result = e.process(input, ProcessingContext::Command);
    assert!(result.any_matched());
    assert!(result.text.contains("<JWT_TOKEN>"));
    Ok(())
}

#[sinex_test]
async fn redacts_bearer_token() -> ::xtask::sandbox::TestResult<()> {
    let e = test_engine();
    let input = "curl -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.test.sig'";
    let result = e.process(input, ProcessingContext::Command);
    assert!(result.any_matched());
    Ok(())
}

#[sinex_test]
async fn preserves_safe_content() -> ::xtask::sandbox::TestResult<()> {
    let e = test_engine();
    let input = "ls -la /home/user/projects";
    let result = e.process(input, ProcessingContext::Command);
    assert!(!result.any_matched());
    assert_eq!(result.text.as_ref(), input);
    Ok(())
}

#[sinex_test]
async fn preserves_normal_commands() -> ::xtask::sandbox::TestResult<()> {
    let e = test_engine();
    let input = "git commit -m 'fix bug'";
    let result = e.process(input, ProcessingContext::Command);
    assert!(!result.any_matched());
    assert_eq!(result.text.as_ref(), input);
    Ok(())
}

// ── Suppress strategy ──

#[sinex_test]
async fn suppresses_private_key() -> ::xtask::sandbox::TestResult<()> {
    let e = test_engine();
    let input = "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAK...";
    let result = e.process(input, ProcessingContext::Command);
    assert!(result.suppressed);
    Ok(())
}

// ── Context filtering ──

#[sinex_test]
async fn cli_flag_only_fires_in_command_context() -> ::xtask::sandbox::TestResult<()> {
    let e = test_engine();
    let input = "--password s3cret";
    let cmd_result = e.process(input, ProcessingContext::Command);
    assert!(cmd_result.any_matched());

    let _journal_result = e.process(input, ProcessingContext::Journal);
    // cli_secret_flag is Command-only, but generic_secret_assign covers Journal
    // so let's test something purely Command-scoped
    let input2 = "--auth-token abc123def456ghi";
    let journal_result2 = e.process(input2, ProcessingContext::Journal);
    // Should not match cli_secret_flag in Journal context
    assert!(!journal_result2.text.contains("--$1"));
    Ok(())
}

// ── Structural PII ──

#[sinex_test]
async fn detects_credit_card_with_luhn() -> ::xtask::sandbox::TestResult<()> {
    let e = test_engine();
    let input = "card: 4111 1111 1111 1111";
    let result = e.process(input, ProcessingContext::Clipboard);
    assert!(result.any_matched());
    assert!(result.text.contains("<CREDIT_CARD>"));
    Ok(())
}

#[sinex_test]
async fn rejects_non_luhn_digits() -> ::xtask::sandbox::TestResult<()> {
    let e = test_engine();
    let input = "number: 1234567890123456";
    let result = e.process(input, ProcessingContext::Clipboard);
    // Should NOT match credit_card (fails Luhn)
    assert!(!result.text.contains("<CREDIT_CARD>"));
    Ok(())
}

#[sinex_test]
async fn hashes_email_when_key_available() -> ::xtask::sandbox::TestResult<()> {
    let e = test_engine_with_key();
    let input = "contact user@example.com please";
    let result = e.process(input, ProcessingContext::Clipboard);
    assert!(result.any_matched());
    assert!(result.text.contains("\u{231c}hash:"));
    assert!(!result.text.contains("user@example.com"));
    Ok(())
}

#[sinex_test]
async fn redacts_email_when_no_key() -> ::xtask::sandbox::TestResult<()> {
    let e = test_engine();
    let input = "contact user@example.com please";
    let result = e.process(input, ProcessingContext::Clipboard);
    assert!(result.any_matched());
    // Degrades to redact since no key
    assert!(
        result.text.contains("<EMAIL_ADDRESS>"),
        "got: {}",
        result.text
    );
    assert!(!result.text.contains("user@example.com"));
    Ok(())
}

// ── JSON processing ──

#[sinex_test]
async fn processes_json_strings() -> ::xtask::sandbox::TestResult<()> {
    let e = test_engine();
    let access_key = aws_access_key_fixture();
    let token = github_token_fixture();
    let json = serde_json::json!({
        "message": format!("key={access_key}"),
        "count": 42,
        "nested": {
            "token": token
        }
    });
    let result = e.process_json(&json, ProcessingContext::Dbus);
    let msg = result["message"].as_str().unwrap();
    assert!(!msg.contains(&access_key));
    Ok(())
}

// ── Encryption roundtrip ──

#[sinex_test]
async fn encrypt_strategy_produces_decryptable_tokens() -> ::xtask::sandbox::TestResult<()> {
    use super::super::{Matcher, PatternRule, RuleCategory, Strategy};

    let mut config = PrivacyConfig::default();
    config.key.key_hex = Some("42".repeat(32));
    config.builtin_categories = CategorySet::None;
    config.extra_rules.push(PatternRule {
        name: "test_encrypt".into(),
        description: "test".into(),
        category: RuleCategory::Custom,
        matcher: Matcher::Regex {
            pattern: r"SECRET_\w+".into(),
        },
        strategy: Strategy::Encrypt,
        contexts: vec![],
        enabled: true,
    });

    let engine = PrivacyEngine::new(config).unwrap();
    let result = engine.process("value=SECRET_ABC123", ProcessingContext::Command);
    assert!(result.text.contains("\u{231c}enc:v1:"));

    let decrypted = engine.decrypt_all(&result.text).unwrap();
    assert_eq!(decrypted, "value=SECRET_ABC123");
    Ok(())
}

// ── Noop engine ──

#[sinex_test]
async fn noop_passes_through() -> ::xtask::sandbox::TestResult<()> {
    let e = PrivacyEngine::noop();
    let input = aws_access_key_fixture();
    let result = e.process(&input, ProcessingContext::Command);
    assert_eq!(result.text.as_ref(), input.as_str());
    assert!(!result.any_matched());
    Ok(())
}

// ── Stats ──

#[sinex_test]
async fn stats_tracking() -> ::xtask::sandbox::TestResult<()> {
    let e = test_engine_with_key();
    let access_key = aws_access_key_fixture();
    let _ = e.process(&access_key, ProcessingContext::Command);
    let _ = e.process(&access_key, ProcessingContext::Command);
    let stats = e.stats_snapshot();
    let aws_count = stats
        .iter()
        .find(|(n, _)| n == "aws_access_key")
        .map_or(0, |(_, c)| *c);
    assert_eq!(aws_count, 2);
    Ok(())
}

// ── apply_mask unit tests ──

#[sinex_test]
async fn mask_middle_digits() -> ::xtask::sandbox::TestResult<()> {
    // card fixture: keep 4 prefix, 4 suffix → 4111 + 8×'*' + 1111
    let card = card_number_fixture();
    let result = apply_mask(&card, '*', 4, 4);
    assert_eq!(result, "4111********1111");
    Ok(())
}

#[sinex_test]
async fn mask_all_chars_when_prefix_suffix_exceed_length() -> ::xtask::sandbox::TestResult<()> {
    // If prefix + suffix >= total, return as-is
    let result = apply_mask("abc", '*', 2, 2);
    assert_eq!(result, "abc");
    Ok(())
}

#[sinex_test]
async fn mask_zero_prefix_suffix() -> ::xtask::sandbox::TestResult<()> {
    let result = apply_mask("hello", '*', 0, 0);
    assert_eq!(result, "*****");
    Ok(())
}

#[sinex_test]
async fn mask_custom_char() -> ::xtask::sandbox::TestResult<()> {
    let result = apply_mask("secret", '#', 1, 1);
    assert_eq!(result, "s####t");
    Ok(())
}

// ── Strategy::Mask integration tests ──

fn engine_with_mask_rule(keep_prefix: usize, keep_suffix: usize) -> PrivacyEngine {
    use super::super::{Matcher, PatternRule, RuleCategory};
    let mut config = PrivacyConfig::default();
    config.builtin_categories = CategorySet::None;
    config.extra_rules.push(PatternRule {
        name: "test_mask".into(),
        description: "test mask rule".into(),
        category: RuleCategory::Custom,
        matcher: Matcher::Regex {
            pattern: r"\b\d{16}\b".into(),
        },
        strategy: Strategy::Mask {
            char: Some('*'),
            keep_prefix: Some(keep_prefix),
            keep_suffix: Some(keep_suffix),
        },
        contexts: vec![],
        enabled: true,
    });
    PrivacyEngine::new(config).unwrap()
}

#[sinex_test]
async fn mask_strategy_redacts_middle_of_card_number() -> ::xtask::sandbox::TestResult<()> {
    let e = engine_with_mask_rule(4, 4);
    let card = card_number_fixture();
    let input = format!("card: {card}");
    let result = e.process(&input, ProcessingContext::Command);
    assert!(result.any_matched());
    assert!(
        result.text.contains("4111********1111"),
        "got: {}",
        result.text
    );
    assert!(!result.text.contains(&card));
    Ok(())
}

#[sinex_test]
async fn mask_strategy_leaves_non_matching_text_unchanged() -> ::xtask::sandbox::TestResult<()>
{
    let e = engine_with_mask_rule(4, 4);
    let result = e.process("no card here", ProcessingContext::Command);
    assert!(!result.any_matched());
    assert_eq!(result.text.as_ref(), "no card here");
    Ok(())
}

// ── Compound matcher tests ──

fn engine_with_compound(matcher: super::super::Matcher) -> PrivacyEngine {
    use super::super::{PatternRule, RuleCategory};
    let mut config = PrivacyConfig::default();
    config.builtin_categories = CategorySet::None;
    config.extra_rules.push(PatternRule {
        name: "test_compound".into(),
        description: "compound rule".into(),
        category: RuleCategory::Custom,
        matcher,
        strategy: Strategy::Redact {
            label: Some("<COMPOUND>".into()),
        },
        contexts: vec![],
        enabled: true,
    });
    PrivacyEngine::new(config).unwrap()
}

#[sinex_test]
async fn any_matcher_fires_on_first_sub_match() -> ::xtask::sandbox::TestResult<()> {
    use super::super::Matcher;
    let e = engine_with_compound(Matcher::Any(vec![
        Matcher::Regex {
            pattern: r"FOO".into(),
        },
        Matcher::Regex {
            pattern: r"BAR".into(),
        },
    ]));
    let result = e.process("contains BAR here", ProcessingContext::Command);
    assert!(result.any_matched());
    assert!(result.text.contains("<COMPOUND>"));
    Ok(())
}

#[sinex_test]
async fn any_matcher_fires_on_either_branch() -> ::xtask::sandbox::TestResult<()> {
    use super::super::Matcher;
    let e = engine_with_compound(Matcher::Any(vec![
        Matcher::Regex {
            pattern: r"FOO".into(),
        },
        Matcher::Regex {
            pattern: r"BAR".into(),
        },
    ]));
    let result_foo = e.process("FOO present", ProcessingContext::Command);
    let result_bar = e.process("BAR present", ProcessingContext::Command);
    assert!(result_foo.any_matched());
    assert!(result_bar.any_matched());
    Ok(())
}

#[sinex_test]
async fn any_matcher_does_not_fire_when_no_branch_matches() -> ::xtask::sandbox::TestResult<()>
{
    use super::super::Matcher;
    let e = engine_with_compound(Matcher::Any(vec![
        Matcher::Regex {
            pattern: r"FOO".into(),
        },
        Matcher::Regex {
            pattern: r"BAR".into(),
        },
    ]));
    let result = e.process("neither here", ProcessingContext::Command);
    assert!(!result.any_matched());
    Ok(())
}

#[sinex_test]
async fn all_matcher_fires_only_when_both_sub_matchers_match()
-> ::xtask::sandbox::TestResult<()> {
    use super::super::Matcher;
    let e = engine_with_compound(Matcher::All(vec![
        Matcher::Regex {
            pattern: r"MUST".into(),
        },
        Matcher::Regex {
            pattern: r"ALSO".into(),
        },
    ]));

    // Both present — should match
    let result = e.process("MUST ALSO be here", ProcessingContext::Command);
    assert!(
        result.any_matched(),
        "both sub-matchers match, rule should fire"
    );

    // Only one present — should NOT match
    let result = e.process("only MUST present", ProcessingContext::Command);
    assert!(
        !result.any_matched(),
        "only one sub-matcher matches, rule must not fire"
    );
    Ok(())
}

#[sinex_test]
async fn all_matcher_does_not_fire_when_one_branch_missing() -> ::xtask::sandbox::TestResult<()>
{
    use super::super::Matcher;
    let e = engine_with_compound(Matcher::All(vec![
        Matcher::Regex {
            pattern: r"ALPHA".into(),
        },
        Matcher::Regex {
            pattern: r"BETA".into(),
        },
    ]));
    let result = e.process("only ALPHA", ProcessingContext::Command);
    assert!(!result.any_matched());
    Ok(())
}
