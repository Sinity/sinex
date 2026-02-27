//! Built-in privacy rule catalog.

use super::{Matcher, PatternRule, ProcessingContext, RuleCategory, Strategy, StructuralDetector};

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
                pattern: r"gh[pousr]_[A-Za-z0-9_]{36,255}".into(),
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
                pattern: r"(?i)(password|passwd|secret|token|api_key|apikey|access_key|auth_token|credentials)\s*[:=]\s*(\S+)".into(),
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
                pattern: r"(?i)--(password|token|secret|key|api-key|auth-token)\s+(\S+)".into(),
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
            description: "US Social Security Numbers (tightened: excludes invalid area/group)"
                .into(),
            category: RuleCategory::Pii,
            matcher: Matcher::Regex {
                // Excludes: area 000, 666, 900-999; group 00; serial 0000.
                pattern: r"\b(?!000|666|9\d{2})\d{3}[-\s]?(?!00)\d{2}[-\s]?(?!0000)\d{4}\b".into(),
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

    #[test]
    fn catalog_has_expected_count() {
        let rules = builtin_rules();
        // 17 secrets + 5 PII + 5 infrastructure + 4 privacy = 31
        assert!(
            rules.len() >= 31,
            "expected at least 31 rules, got {}",
            rules.len()
        );
    }

    #[test]
    fn all_rules_have_unique_names() {
        let rules = builtin_rules();
        let mut names: Vec<&str> = rules.iter().map(|r| r.name.as_str()).collect();
        names.sort();
        names.dedup();
        assert_eq!(names.len(), rules.len(), "duplicate rule names found");
    }

    #[test]
    fn all_rules_are_enabled() {
        let rules = builtin_rules();
        for rule in &rules {
            assert!(
                rule.enabled,
                "rule '{}' should be enabled by default",
                rule.name
            );
        }
    }
}
