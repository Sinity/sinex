//! Built-in privacy rule catalog.

use super::{
    Matcher, PatternRule, PrivacyPolicySeedRule, ProcessingContext, RuleCategory, Strategy,
    StructuralDetector,
};
use serde_json::json;

/// All built-in privacy rules.
pub fn builtin_rules() -> Vec<PatternRule> {
    let mut rules = Vec::with_capacity(36);
    rules.extend(secret_rules());
    rules.extend(pii_rules());
    rules.extend(infrastructure_rules());
    rules
}

/// Project built-in catalog entries into DB-policy seed rows.
///
/// The projection preserves the old catalog category/context as metadata in
/// `matcher_config`; it does not translate those contexts into runtime scopes.
/// Actual runtime authority comes only from rows inserted into the policy DB.
#[must_use]
pub fn builtin_policy_seed_rules(enabled: bool) -> Vec<PrivacyPolicySeedRule> {
    builtin_rules()
        .into_iter()
        .filter_map(|rule| policy_seed_rule(rule, enabled))
        .collect()
}

fn policy_seed_rule(rule: PatternRule, enabled: bool) -> Option<PrivacyPolicySeedRule> {
    let (matcher_type, matcher_value, mut matcher_config, case_sensitive) = match rule.matcher {
        Matcher::Regex { pattern } => (
            "regex".to_string(),
            pattern,
            json!({ "seed_source": "builtin_catalog" }),
            false,
        ),
        Matcher::Literal {
            text,
            case_sensitive,
        } => (
            "literal".to_string(),
            text,
            json!({ "seed_source": "builtin_catalog" }),
            case_sensitive,
        ),
        Matcher::Structural { detector } => {
            let detector_name = serde_json::to_value(detector)
                .ok()
                .and_then(|value| value.as_str().map(str::to_string))?;
            (
                "structural".to_string(),
                detector_name,
                json!({
                    "seed_source": "builtin_catalog",
                    "detector": detector,
                }),
                false,
            )
        }
        Matcher::All(_) | Matcher::Any(_) => return None,
    };
    matcher_config["category"] = json!(rule.category);
    matcher_config["catalog_contexts"] = json!(rule.contexts);

    let (action, action_label) = match rule.strategy {
        Strategy::Redact { label } => ("redact".to_string(), label),
        Strategy::Encrypt => ("encrypt".to_string(), None),
        Strategy::Hash => ("hash".to_string(), None),
        Strategy::Suppress => ("suppress".to_string(), None),
        Strategy::Mask { .. } => ("mask".to_string(), None),
    };

    Some(PrivacyPolicySeedRule {
        name: rule.name,
        description: rule.description,
        matcher_type,
        matcher_value,
        matcher_config,
        recognizer_kind: "local_pattern".to_string(),
        case_sensitive,
        action,
        action_label,
        key_namespace: "default".to_string(),
        enabled: enabled && rule.enabled,
    })
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
        // NOTE: Two URL-credential redactors exist by design:
        //
        // 1. This regex rule — operates on free-form text (command lines, log lines,
        //    notifications) where the surrounding context means the input cannot be
        //    handed to `url::Url::parse`. It must capture credentials embedded inside
        //    longer strings (e.g. `git clone https://user:pass@host/repo.git`).
        //
        // 2. `sinex_primitives::utils::url_redaction` — uses `url::Url::parse` for
        //    structured inputs (operator-facing config/diagnostic display). More robust
        //    for edge cases (IPv6 hosts, multiple `@` in password, non-ASCII schemes).
        //
        // Keeping both is legitimate; this rule cannot delegate to the structured path
        // because the privacy engine processes arbitrary free text, not parsed URLs.
        //
        // Sentinel alignment: both use `***` so redacted output is consistent regardless
        // of which path handled the URL. The structured path produces `user:***@host`;
        // this regex produces `proto://user:***@` (same `***` sentinel).
        PatternRule {
            name: "url_credentials".into(),
            description: "Credentials embedded in URLs (proto://user:pass@host)".into(),
            category: RuleCategory::Secret,
            matcher: Matcher::Regex {
                pattern: r"([a-z]+://)([^:@\s]+):([^@\s]+)@".into(),
            },
            strategy: Strategy::Redact {
                // Keep scheme and username; replace password with `***` to match the
                // sentinel used by `url_redaction::redact_url_password_for_diagnostics`.
                label: Some("$1$2:***@".into()),
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
                // Anthropic keys contain additional hyphen-delimited segments
                // and are handled by the dedicated rule above; the legacy
                // OpenAI shape is plain alphanumeric after `sk-`.
                pattern: r"sk-proj-[A-Za-z0-9_\-]{20,}|sk-[A-Za-z0-9]{48}\b".into(),
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
            description:
                "Aggressive variant: hash full home paths instead of collapsing to <HOME>/..."
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

#[cfg(test)]
#[path = "catalog_test.rs"]
mod tests;
