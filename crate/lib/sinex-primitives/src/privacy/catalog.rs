//! Built-in privacy rule catalog.

use super::{
    Matcher, PatternRule, ProcessingContext, RuleCategory, Strategy, StructuralDetector,
};

/// All built-in privacy rules.
pub fn builtin_rules() -> Vec<PatternRule> {
    let mut rules = Vec::with_capacity(40);
    rules.extend(secret_rules());
    rules.extend(pii_rules());
    rules.extend(infrastructure_rules());
    rules.extend(privacy_title_rules());
    rules
}

// ─── Secrets ─────────────────────────────────────────────────

fn secret_rules() -> Vec<PatternRule> {
    vec![
        PatternRule {
            name: "aws_access_key".into(),
            description: "AWS access key IDs (AKIA/ASIA/ABIA/ACCA prefix)".into(),
            category: RuleCategory::Secret,
            matcher: Matcher::Regex {
                pattern: r"(?i)\b(AKIA|ASIA|ABIA|ACCA)[0-9A-Z]{16}\b".into(),
            },
            strategy: Strategy::Redact {
                label: Some("<AWS_ACCESS_KEY>".into()),
            },
            contexts: vec![],
            enabled: true,
        },
        PatternRule {
            name: "aws_secret_key".into(),
            description: "AWS secret access key assignments".into(),
            category: RuleCategory::Secret,
            matcher: Matcher::Regex {
                pattern: r"(?i)(aws_secret_access_key|secret_access_key|aws_secret)\s*[:=]\s*([A-Za-z0-9/+=]{40})".into(),
            },
            strategy: Strategy::Redact {
                label: Some("$1=<AWS_SECRET_KEY>".into()),
            },
            contexts: vec![],
            enabled: true,
        },
        PatternRule {
            name: "url_credentials".into(),
            description: "Credentials embedded in URLs (proto://user:pass@host)".into(),
            category: RuleCategory::Secret,
            matcher: Matcher::Regex {
                pattern: r"([a-z]+://)([^:@\s]+):([^@\s]+)@".into(),
            },
            strategy: Strategy::Redact {
                label: Some("$1<USER>:<REDACTED>@".into()),
            },
            contexts: vec![],
            enabled: true,
        },
        PatternRule {
            name: "private_key_header".into(),
            description: "PEM private key headers".into(),
            category: RuleCategory::Secret,
            matcher: Matcher::Regex {
                pattern: r"-----BEGIN [A-Z ]*PRIVATE KEY-----".into(),
            },
            strategy: Strategy::Suppress,
            contexts: vec![],
            enabled: true,
        },
        PatternRule {
            name: "github_token".into(),
            description: "GitHub personal access tokens and fine-grained tokens".into(),
            category: RuleCategory::Secret,
            matcher: Matcher::Regex {
                pattern: r"gh(?:[pousr]_|ithub_pat_)[A-Za-z0-9_]{36,255}".into(),
            },
            strategy: Strategy::Redact {
                label: Some("<GITHUB_TOKEN>".into()),
            },
            contexts: vec![],
            enabled: true,
        },
        PatternRule {
            name: "gitlab_token".into(),
            description: "GitLab personal access tokens".into(),
            category: RuleCategory::Secret,
            matcher: Matcher::Regex {
                pattern: r"glpat-[A-Za-z0-9_\-]{20,}".into(),
            },
            strategy: Strategy::Redact {
                label: Some("<GITLAB_TOKEN>".into()),
            },
            contexts: vec![],
            enabled: true,
        },
        PatternRule {
            name: "npm_token".into(),
            description: "npm authentication tokens".into(),
            category: RuleCategory::Secret,
            matcher: Matcher::Regex {
                pattern: r"npm_[A-Za-z0-9]{36,}".into(),
            },
            strategy: Strategy::Redact {
                label: Some("<NPM_TOKEN>".into()),
            },
            contexts: vec![],
            enabled: true,
        },
        PatternRule {
            name: "stripe_key".into(),
            description: "Stripe API keys (sk_live, pk_test, etc.)".into(),
            category: RuleCategory::Secret,
            matcher: Matcher::Regex {
                pattern: r"(?:sk|pk|rk)_(?:test|live)_[A-Za-z0-9]{24,}".into(),
            },
            strategy: Strategy::Redact {
                label: Some("<STRIPE_KEY>".into()),
            },
            contexts: vec![],
            enabled: true,
        },
        PatternRule {
            name: "slack_token".into(),
            description: "Slack API tokens (xoxb, xoxp, xoxa, etc.)".into(),
            category: RuleCategory::Secret,
            matcher: Matcher::Regex {
                pattern: r"xox[bpsar]-[A-Za-z0-9\-]+".into(),
            },
            strategy: Strategy::Redact {
                label: Some("<SLACK_TOKEN>".into()),
            },
            contexts: vec![],
            enabled: true,
        },
        PatternRule {
            name: "jwt".into(),
            description: "JSON Web Tokens (3-segment base64url)".into(),
            category: RuleCategory::Secret,
            matcher: Matcher::Regex {
                pattern: r"eyJ[A-Za-z0-9_\-]+\.eyJ[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+".into(),
            },
            strategy: Strategy::Redact {
                label: Some("<JWT_TOKEN>".into()),
            },
            contexts: vec![],
            enabled: true,
        },
        PatternRule {
            name: "google_api_key".into(),
            description: "Google API keys".into(),
            category: RuleCategory::Secret,
            matcher: Matcher::Regex {
                pattern: r"AIza[A-Za-z0-9_\-]{35}".into(),
            },
            strategy: Strategy::Redact {
                label: Some("<GOOGLE_API_KEY>".into()),
            },
            contexts: vec![],
            enabled: true,
        },
        PatternRule {
            name: "azure_connection".into(),
            description: "Azure storage account keys".into(),
            category: RuleCategory::Secret,
            matcher: Matcher::Regex {
                pattern: r"AccountKey=[A-Za-z0-9+/=]{44,}".into(),
            },
            strategy: Strategy::Redact {
                label: Some("AccountKey=<AZURE_KEY>".into()),
            },
            contexts: vec![],
            enabled: true,
        },
        PatternRule {
            name: "generic_secret_assign".into(),
            description: "Assignments to secret-looking variable names".into(),
            category: RuleCategory::Secret,
            matcher: Matcher::Regex {
                // Exclude values that are already redaction placeholders (<...>)
                pattern: r"(?i)(password|passwd|secret|token|api_key|apikey|api-key|access_key|auth_token|credentials)\s*[:=]\s*([^<\s]\S*)".into(),
            },
            strategy: Strategy::Redact {
                label: Some("$1=<REDACTED>".into()),
            },
            contexts: vec![ProcessingContext::Command, ProcessingContext::Journal],
            enabled: true,
        },
        PatternRule {
            name: "cli_secret_flag".into(),
            description: "Command-line flags for secrets (--password, --token, etc.)".into(),
            category: RuleCategory::Secret,
            matcher: Matcher::Regex {
                pattern: r"(?i)--(password|token|secret|key|api-key|auth-token)[\s=]+(\S+)".into(),
            },
            strategy: Strategy::Redact {
                label: Some("--$1 <REDACTED>".into()),
            },
            contexts: vec![ProcessingContext::Command],
            enabled: true,
        },
        PatternRule {
            name: "bearer_token".into(),
            description: "HTTP Bearer authentication tokens".into(),
            category: RuleCategory::Secret,
            matcher: Matcher::Regex {
                pattern: r"(?i)Bearer\s+[A-Za-z0-9._~+/=\-]{20,}".into(),
            },
            strategy: Strategy::Redact {
                label: Some("Bearer <REDACTED>".into()),
            },
            contexts: vec![],
            enabled: true,
        },
        PatternRule {
            name: "database_url".into(),
            description: "Database connection strings with credentials".into(),
            category: RuleCategory::Secret,
            matcher: Matcher::Regex {
                pattern: r"(?i)(postgres|mysql|redis|mongodb)://\S+".into(),
            },
            strategy: Strategy::Redact {
                label: Some("<DATABASE_URL>".into()),
            },
            contexts: vec![],
            enabled: true,
        },
        PatternRule {
            name: "ssh_public_key".into(),
            description: "SSH public keys".into(),
            category: RuleCategory::Secret,
            matcher: Matcher::Regex {
                pattern: r"ssh-(?:rsa|ed25519|ecdsa)\s+AAAA[A-Za-z0-9+/]+".into(),
            },
            strategy: Strategy::Redact {
                label: Some("<SSH_PUBLIC_KEY>".into()),
            },
            contexts: vec![],
            enabled: true,
        },
        PatternRule {
            name: "anthropic_api_key".into(),
            description: "Anthropic API keys (sk-ant-api03- prefix)".into(),
            category: RuleCategory::Secret,
            matcher: Matcher::Regex {
                // Length 93–108 chars after prefix; allow tolerance for future extension.
                pattern: r"sk-ant-api\d{2}-[A-Za-z0-9_\-]{80,120}".into(),
            },
            strategy: Strategy::Suppress,
            contexts: vec![],
            enabled: true,
        },
        PatternRule {
            name: "openai_api_key".into(),
            description: "OpenAI API keys (sk- prefix, legacy 48-char and project sk-proj-)".into(),
            category: RuleCategory::Secret,
            matcher: Matcher::Regex {
                // Legacy format: sk- followed by exactly 48 alphanum chars (word boundary).
                // Project keys: sk-proj- followed by 20+ URL-safe base64 chars.
                // Note: sk-ant- (Anthropic) handled separately; exclude via negative lookahead
                // is unavailable in the `regex` crate so we rely on rule ordering (anthropic
                // rule fires first) and non-overlapping patterns.
                pattern: r"sk-proj-[A-Za-z0-9_\-]{20,}|(?:sk-)(?!ant-)[A-Za-z0-9]{48}\b".into(),
            },
            strategy: Strategy::Suppress,
            contexts: vec![],
            enabled: true,
        },
        PatternRule {
            name: "huggingface_token".into(),
            description: "HuggingFace API tokens (hf_ prefix)".into(),
            category: RuleCategory::Secret,
            matcher: Matcher::Regex {
                pattern: r"hf_[A-Za-z0-9]{34,}".into(),
            },
            strategy: Strategy::Redact {
                label: Some("<HUGGINGFACE_TOKEN>".into()),
            },
            contexts: vec![],
            enabled: true,
        },
    ]
}

// ─── PII ─────────────────────────────────────────────────────

fn pii_rules() -> Vec<PatternRule> {
    vec![
        PatternRule {
            name: "credit_card".into(),
            description: "Payment card numbers (Luhn-validated)".into(),
            category: RuleCategory::Pii,
            matcher: Matcher::Structural {
                detector: StructuralDetector::CreditCard,
            },
            strategy: Strategy::Redact {
                label: Some("<CREDIT_CARD>".into()),
            },
            contexts: vec![],
            enabled: true,
        },
        PatternRule {
            name: "ssn".into(),
            description: "US Social Security Numbers (structurally validated: excludes invalid area/group/serial)".into(),
            category: RuleCategory::Pii,
            matcher: Matcher::Structural {
                detector: StructuralDetector::Ssn,
            },
            strategy: Strategy::Redact {
                label: Some("<SSN>".into()),
            },
            // Only in contexts where SSNs would appear (not in journal/dbus chatter)
            contexts: vec![
                ProcessingContext::Command,
                ProcessingContext::Clipboard,
                ProcessingContext::Document,
                ProcessingContext::Notification,
            ],
            enabled: true,
        },
        PatternRule {
            name: "email_address".into(),
            description: "Email addresses (structurally validated)".into(),
            category: RuleCategory::Pii,
            matcher: Matcher::Structural {
                detector: StructuralDetector::Email,
            },
            strategy: Strategy::Hash,
            contexts: vec![],
            enabled: true,
        },
        PatternRule {
            name: "phone_number".into(),
            description: "Phone numbers with country/area code".into(),
            category: RuleCategory::Pii,
            matcher: Matcher::Structural {
                detector: StructuralDetector::PhoneNumber,
            },
            strategy: Strategy::Hash,
            contexts: vec![
                ProcessingContext::Clipboard,
                ProcessingContext::Document,
                ProcessingContext::Notification,
            ],
            enabled: true,
        },
        PatternRule {
            name: "iban".into(),
            description: "International Bank Account Numbers (mod-97 validated)".into(),
            category: RuleCategory::Pii,
            matcher: Matcher::Structural {
                detector: StructuralDetector::Iban,
            },
            strategy: Strategy::Redact {
                label: Some("<IBAN>".into()),
            },
            contexts: vec![],
            enabled: true,
        },
        PatternRule {
            name: "pesel".into(),
            description: "Polish national identification number — PESEL (checksum-validated)".into(),
            category: RuleCategory::Pii,
            matcher: Matcher::Structural {
                detector: StructuralDetector::Pesel,
            },
            // Hash preserves identity for analytics without exposing the literal value.
            strategy: Strategy::Hash,
            contexts: vec![
                ProcessingContext::Command,
                ProcessingContext::Clipboard,
                ProcessingContext::Document,
                ProcessingContext::Notification,
            ],
            enabled: true,
        },
        PatternRule {
            name: "nip".into(),
            description: "Polish tax identification number — NIP (checksum-validated)".into(),
            category: RuleCategory::Pii,
            matcher: Matcher::Structural {
                detector: StructuralDetector::Nip,
            },
            strategy: Strategy::Hash,
            contexts: vec![
                ProcessingContext::Command,
                ProcessingContext::Clipboard,
                ProcessingContext::Document,
                ProcessingContext::Notification,
            ],
            enabled: true,
        },
        PatternRule {
            name: "regon".into(),
            description: "Polish business registry number — REGON (checksum-validated)".into(),
            category: RuleCategory::Pii,
            matcher: Matcher::Structural {
                detector: StructuralDetector::Regon,
            },
            strategy: Strategy::Hash,
            contexts: vec![
                ProcessingContext::Command,
                ProcessingContext::Clipboard,
                ProcessingContext::Document,
                ProcessingContext::Notification,
            ],
            enabled: true,
        },
    ]
}

// ─── Infrastructure / metadata ───────────────────────────────

fn infrastructure_rules() -> Vec<PatternRule> {
    vec![
        PatternRule {
            name: "ipv4_address".into(),
            description: "IPv4 addresses".into(),
            category: RuleCategory::Pii,
            matcher: Matcher::Structural {
                detector: StructuralDetector::Ipv4,
            },
            strategy: Strategy::Redact {
                label: Some("<IPV4>".into()),
            },
            contexts: vec![],
            enabled: true,
        },
        PatternRule {
            name: "ipv6_address".into(),
            description: "IPv6 addresses".into(),
            category: RuleCategory::Pii,
            matcher: Matcher::Structural {
                detector: StructuralDetector::Ipv6,
            },
            strategy: Strategy::Redact {
                label: Some("<IPV6>".into()),
            },
            contexts: vec![],
            enabled: true,
        },
        PatternRule {
            name: "mac_address".into(),
            description: "Hardware MAC addresses".into(),
            category: RuleCategory::Pii,
            matcher: Matcher::Structural {
                detector: StructuralDetector::MacAddress,
            },
            strategy: Strategy::Redact {
                label: Some("<MAC_ADDRESS>".into()),
            },
            contexts: vec![],
            enabled: true,
        },
        PatternRule {
            name: "local_hostname".into(),
            description: "Local machine hostname".into(),
            category: RuleCategory::Privacy,
            matcher: Matcher::Structural {
                detector: StructuralDetector::LocalHostname,
            },
            strategy: Strategy::Redact {
                label: Some("<HOSTNAME>".into()),
            },
            contexts: vec![],
            enabled: true,
        },
        PatternRule {
            name: "user_home_path".into(),
            description: "Paths under the user's home directory".into(),
            category: RuleCategory::Privacy,
            matcher: Matcher::Structural {
                detector: StructuralDetector::UserHomePath,
            },
            strategy: Strategy::Redact {
                label: Some("<HOME>/...".into()),
            },
            contexts: vec![],
            enabled: true,
        },
        // Opt-in aggressive variant. Hashes the full home path instead of
        // collapsing to `<HOME>/...`, which preserves uniqueness under analysis
        // (so two events touching different files inside $HOME stay
        // distinguishable) without leaking the literal path. Disabled by
        // default; enable via override:
        //
        //   [overrides.user_home_path_aggressive]
        //   enabled = true
        //
        //   # and typically also disable the soft variant to avoid both
        //   # firing on the same input:
        //   [overrides.user_home_path]
        //   enabled = false
        //
        // Or via env: SINEX_PRIVACY_OVERRIDES='{"user_home_path_aggressive":{"enabled":true},"user_home_path":{"enabled":false}}'
        PatternRule {
            name: "user_home_path_aggressive".into(),
            description: "Aggressive variant: hash full home paths instead of collapsing to <HOME>/..."
                .into(),
            category: RuleCategory::Privacy,
            matcher: Matcher::Structural {
                detector: StructuralDetector::UserHomePath,
            },
            strategy: Strategy::Hash,
            contexts: vec![],
            enabled: false,
        },
    ]
}

// ─── Privacy / Window titles ─────────────────────────────────

fn privacy_title_rules() -> Vec<PatternRule> {
    vec![
        PatternRule {
            name: "password_entry_title".into(),
            description: "Window titles related to password entry".into(),
            category: RuleCategory::Privacy,
            matcher: Matcher::Regex {
                pattern: r"(?i)(password|passwort|mot de passe|contraseña|密码|パスワード)".into(),
            },
            strategy: Strategy::Redact {
                label: Some("<PASSWORD_ENTRY>".into()),
            },
            contexts: vec![ProcessingContext::WindowTitle],
            enabled: true,
        },
        PatternRule {
            name: "login_window_title".into(),
            description: "Login / sign-in window titles".into(),
            category: RuleCategory::Privacy,
            matcher: Matcher::Regex {
                pattern: r"(?i)(sign.?in|log.?in|auth(?:entication)?|verify your identity)".into(),
            },
            strategy: Strategy::Redact {
                label: Some("<LOGIN_WINDOW>".into()),
            },
            contexts: vec![ProcessingContext::WindowTitle],
            enabled: true,
        },
        PatternRule {
            name: "password_manager_title".into(),
            description: "Password manager window titles".into(),
            category: RuleCategory::Privacy,
            matcher: Matcher::Regex {
                pattern: r"(?i)(keepass|1password|bitwarden|lastpass|dashlane|password.?safe)"
                    .into(),
            },
            strategy: Strategy::Redact {
                label: Some("<PASSWORD_MANAGER>".into()),
            },
            contexts: vec![ProcessingContext::WindowTitle],
            enabled: true,
        },
        PatternRule {
            name: "sensitive_file_title".into(),
            description: "Window titles showing sensitive file types".into(),
            category: RuleCategory::Privacy,
            matcher: Matcher::Regex {
                pattern: r"(?i)\.(env|pem|key|crt|pfx|p12|jks|keystore)\b".into(),
            },
            strategy: Strategy::Redact {
                label: Some("<SENSITIVE_FILE>".into()),
            },
            contexts: vec![ProcessingContext::WindowTitle],
            enabled: true,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn catalog_has_expected_count() -> ::xtask::sandbox::TestResult<()> {
        let rules = builtin_rules();
        // 20 secrets + 8 PII + 5 infrastructure + 4 privacy = 37
        // (added: anthropic_api_key, openai_api_key, huggingface_token,
        //         pesel, nip, regon)
        assert!(
            rules.len() >= 37,
            "expected at least 37 rules, got {}",
            rules.len()
        );
        Ok(())
    }

    #[sinex_test]
    async fn all_rules_have_unique_names() -> ::xtask::sandbox::TestResult<()> {
        let rules = builtin_rules();
        let mut names: Vec<&str> = rules.iter().map(|r| r.name.as_str()).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), rules.len(), "duplicate rule names found");
        Ok(())
    }

    /// Names of rules that ship disabled-by-default. These are opt-in
    /// aggressive variants the operator must explicitly enable via overrides.
    const OPT_IN_RULES: &[&str] = &["user_home_path_aggressive"];

    #[sinex_test]
    async fn default_enablement_matches_opt_in_list() -> ::xtask::sandbox::TestResult<()> {
        let rules = builtin_rules();
        for rule in &rules {
            let expected = !OPT_IN_RULES.contains(&rule.name.as_str());
            assert_eq!(
                rule.enabled, expected,
                "rule '{}' enablement disagrees with OPT_IN_RULES list",
                rule.name
            );
        }
        Ok(())
    }

    #[sinex_test]
    async fn opt_in_rules_actually_exist() -> ::xtask::sandbox::TestResult<()> {
        let rules = builtin_rules();
        for name in OPT_IN_RULES {
            assert!(
                rules.iter().any(|r| r.name == *name),
                "OPT_IN_RULES references '{name}' but no such rule is in the catalog"
            );
        }
        Ok(())
    }

    #[sinex_test]
    async fn aggressive_path_rule_replaces_collapse_when_opted_in()
    -> ::xtask::sandbox::TestResult<()> {
        // Document the operator-facing pattern: turning on the aggressive
        // variant and turning off the soft variant produces hashed output
        // for $HOME paths instead of `<HOME>/...` collapse.
        use crate::privacy::{PrivacyConfig, PrivacyEngine, ProcessingContext, RuleOverride};
        use std::collections::HashMap;

        unsafe {
            std::env::set_var("HOME", "/home/sinity-test-aggressive-redact");
        }

        let mut overrides = HashMap::new();
        overrides.insert(
            "user_home_path_aggressive".to_string(),
            RuleOverride {
                enabled: Some(true),
                ..Default::default()
            },
        );
        overrides.insert(
            "user_home_path".to_string(),
            RuleOverride {
                enabled: Some(false),
                ..Default::default()
            },
        );

        let config = PrivacyConfig {
            overrides,
            ..PrivacyConfig::default()
        };
        let engine = PrivacyEngine::new(config).expect("engine builds");
        let result = engine.process(
            "/home/sinity-test-aggressive-redact/projects/sinex/Cargo.toml",
            ProcessingContext::Metadata,
        );

        // Without a key, Hash degrades to a generic redact label rather than
        // a real hash. Either way the literal home prefix must be gone, AND
        // the soft `<HOME>/...` collapse must NOT be the output (proving the
        // override flipped which rule fired).
        assert!(
            !result.text.contains("/home/sinity-test-aggressive-redact/"),
            "aggressive variant must redact the home prefix, got {:?}",
            result.text
        );
        assert!(
            !result.text.contains("<HOME>"),
            "aggressive variant must not emit the soft <HOME>/... label, got {:?}",
            result.text
        );
        Ok(())
    }

    // ── New API keys ──────────────────────────────────────────

    fn rule_exists(name: &str) -> bool {
        builtin_rules().iter().any(|r| r.name == name)
    }

    #[sinex_test]
    async fn anthropic_api_key_rule_exists() -> ::xtask::sandbox::TestResult<()> {
        assert!(rule_exists("anthropic_api_key"));
        Ok(())
    }

    #[sinex_test]
    async fn openai_api_key_rule_exists() -> ::xtask::sandbox::TestResult<()> {
        assert!(rule_exists("openai_api_key"));
        Ok(())
    }

    #[sinex_test]
    async fn huggingface_token_rule_exists() -> ::xtask::sandbox::TestResult<()> {
        assert!(rule_exists("huggingface_token"));
        Ok(())
    }

    // ── New Polish PII rules ──────────────────────────────────

    #[sinex_test]
    async fn pesel_rule_exists() -> ::xtask::sandbox::TestResult<()> {
        assert!(rule_exists("pesel"));
        Ok(())
    }

    #[sinex_test]
    async fn nip_rule_exists() -> ::xtask::sandbox::TestResult<()> {
        assert!(rule_exists("nip"));
        Ok(())
    }

    #[sinex_test]
    async fn regon_rule_exists() -> ::xtask::sandbox::TestResult<()> {
        assert!(rule_exists("regon"));
        Ok(())
    }

    #[sinex_test]
    async fn pesel_rule_uses_hash_strategy() -> ::xtask::sandbox::TestResult<()> {
        let rules = builtin_rules();
        let rule = rules.iter().find(|r| r.name == "pesel").expect("pesel rule");
        assert!(
            matches!(rule.strategy, Strategy::Hash),
            "PESEL should use Hash strategy, got {:?}",
            rule.strategy
        );
        Ok(())
    }

    #[sinex_test]
    async fn nip_rule_uses_hash_strategy() -> ::xtask::sandbox::TestResult<()> {
        let rules = builtin_rules();
        let rule = rules.iter().find(|r| r.name == "nip").expect("nip rule");
        assert!(
            matches!(rule.strategy, Strategy::Hash),
            "NIP should use Hash strategy, got {:?}",
            rule.strategy
        );
        Ok(())
    }

    #[sinex_test]
    async fn regon_rule_uses_hash_strategy() -> ::xtask::sandbox::TestResult<()> {
        let rules = builtin_rules();
        let rule = rules.iter().find(|r| r.name == "regon").expect("regon rule");
        assert!(
            matches!(rule.strategy, Strategy::Hash),
            "REGON should use Hash strategy, got {:?}",
            rule.strategy
        );
        Ok(())
    }

    #[sinex_test]
    async fn anthropic_key_uses_suppress_strategy() -> ::xtask::sandbox::TestResult<()> {
        let rules = builtin_rules();
        let rule = rules
            .iter()
            .find(|r| r.name == "anthropic_api_key")
            .expect("anthropic_api_key rule");
        assert!(
            matches!(rule.strategy, Strategy::Suppress),
            "Anthropic API key should be suppressed, got {:?}",
            rule.strategy
        );
        Ok(())
    }

    #[sinex_test]
    async fn openai_key_uses_suppress_strategy() -> ::xtask::sandbox::TestResult<()> {
        let rules = builtin_rules();
        let rule = rules
            .iter()
            .find(|r| r.name == "openai_api_key")
            .expect("openai_api_key rule");
        assert!(
            matches!(rule.strategy, Strategy::Suppress),
            "OpenAI API key should be suppressed, got {:?}",
            rule.strategy
        );
        Ok(())
    }
}
