# Unified Privacy Engine

> **Status**: RFC  
> **Date**: 2026-02-22  
> **Scope**: Replace all fragmented scrubbing/redaction with a single privacy engine

---

## 1. Current State: What Exists and What's Wrong

### 1.1 Inventory

| Component | Location | Mechanism | What it does |
|-----------|----------|-----------|--------------|
| `RedactionConfig` | `sinex-primitives/src/redaction_config.rs` | Serde struct | `RedactionPattern { name, regex, replacement }`, env loading, reference patterns |
| `ConfigurableRedactor` | `sinex-primitives/src/secret_redaction.rs` | Compiled regex set | `redact_content()`, `redact_title()`, `is_highly_sensitive()`, stats tracking |
| `GLOBAL_REDACTOR` | `sinex-primitives/src/secret_redaction.rs` | `lazy_static` singleton | Process-wide default; ignores `SINEX_REDACTION_*` env vars |
| `PrivacyFilter` | `sinex-desktop-ingestor/src/privacy_filter.rs` | `OnceLock` wrapper | **Separate instance** that reads env vars, unlike `GLOBAL_REDACTOR` |
| Terminal re-export | `sinex-terminal-ingestor/src/secret_redaction.rs` | Re-export + tests | ~200 lines of nearly identical test code |
| `redact_json_strings()` | `sinex-system-ingestor/src/dbus_watcher.rs` | Recursive JSON walk | Local reimplementation — should be in core |
| Journal watcher | `sinex-system-ingestor/src/unified_journal_watcher.rs` | Inline calls | `GLOBAL_REDACTOR.redact_content()` on MESSAGE, _CMDLINE, extra fields |
| Terminal node | `sinex-terminal-ingestor/src/unified_node.rs` | Inline call | `GLOBAL_REDACTOR.redact_content_with_stats()` on command text |
| Clipboard watcher | `sinex-desktop-ingestor/src/clipboard.rs` | Via PrivacyFilter | `redact_content()` + randomized hash for redacted content |
| Window manager | `sinex-desktop-ingestor/src/window_manager.rs` | Via PrivacyFilter | `redact_title()` on window+input titles |
| Sandbox sanitizer | `xtask/src/sandbox/context.rs` | String cleaning | NUL + control char stripping (test infrastructure, orthogonal) |
| `SanitizedPath` | `sinex-primitives/src/domain.rs` | Validated newtype | Path traversal prevention (keep as-is) |

### 1.2 Architectural Flaws

**F1 · Split brain singleton.** `GLOBAL_REDACTOR` uses `lazy_static` with hardcoded defaults. `PrivacyFilter` uses `OnceLock` and reads `SINEX_REDACTION_*` env vars. Terminal and system ingestors import `GLOBAL_REDACTOR` directly → they **never see user-configured overrides**. Two competing singletons, zero consistency.

**F2 · Replace-not-merge semantics.** Setting `SINEX_REDACTION_PATTERNS` replaces the entire 14-pattern reference set. To add one custom pattern, users must copy-paste all defaults into the env var. This is hostile UX.

**F3 · Lossy-only strategy.** Everything becomes `<REDACTED>` or `<AWS_ACCESS_KEY>`. The original is permanently destroyed. No authorized recovery path for debugging, auditing, or incident investigation.

**F4 · No composable strategies.** Every call site hard-codes one approach (regex replace). There's no way to choose encryption, hashing, or suppression per-pattern or per-context.

**F5 · Narrow detection surface.** Only 14 content regexes + 4 title regexes. PII coverage is limited to credit cards (with an over-broad regex — no Luhn check) and SSNs (also over-broad — matches many valid 9-digit numbers). Missing: emails, phones, IPs, IBANs, MAC addresses, home directory paths, hostnames.

**F6 · `is_highly_sensitive()` uses raw `contains()`.** It checks for `"-----BEGIN"` and `"password"` with string matching, bypassing the entire pattern engine. Should flow through the same system.

**F7 · `redact_json_strings()` is local.** The D-Bus watcher reimplements recursive JSON traversal. Other JSON-carrying contexts (journal extra fields, clipboard metadata) don't share this.

**F8 · Dead code.** `ConfigurableRedactor::new()` allows programmatic per-node config but no call site uses it. The terminal ingestor's `secret_redaction.rs` is entirely re-exports and duplicate tests.

**F9 · No context awareness.** The same patterns fire on command text, window titles, journal messages, and D-Bus payloads. Different contexts have different false-positive profiles (e.g., a window title like "AKIA Corporation" shouldn't trigger `aws_access_key`; a journal message containing an internal IP shouldn't be redacted for a single-user local system).

### 1.3 What Works Well (Keep)

- **Pattern-based approach**: The `RedactionPattern { name, regex, replacement }` structure is sound.
- **`Cow<'a, str>` return**: Zero-allocation fast path when nothing is redacted.
- **Stats tracking**: Useful for diagnostics.
- **Randomized hash on redacted clipboard**: Prevents correlation of repeated sensitive content.
- **`SanitizedPath` / `RecordedPath`**: Orthogonal, well-designed. Leave as-is.

---

## 2. Design

### 2.1 Module Layout

```
sinex-primitives/src/privacy/
├── mod.rs              // Re-exports, PRIVACY_ENGINE global
├── config.rs           // PrivacyConfig, from_env(), from_file()
├── engine.rs           // PrivacyEngine — the core processing type
├── pattern.rs          // PatternRule, Matcher, built-in catalog
├── strategy.rs         // Strategy enum + implementations
├── detector.rs         // Structural detectors (Luhn, email, etc.)
├── context.rs          // ProcessingContext enum, field-level sensitivity annotations
├── envelope.rs         // Encrypted/hashed token format, encode/decode
├── stats.rs            // ProcessingStats (promoted from OwnedRedactionStats)
├── json.rs             // JSON tree processing (replaces dbus_watcher::redact_json_strings)
└── error.rs            // PrivacyError
```

**Delete:**

- `sinex-primitives/src/secret_redaction.rs`
- `sinex-primitives/src/redaction_config.rs`
- `sinex-terminal-ingestor/src/secret_redaction.rs`
- `sinex-desktop-ingestor/src/privacy_filter.rs`

### 2.2 Processing Context

The engine needs to know *what kind of data* it's looking at. Different contexts have different sensitivity profiles, false-positive tolerances, and appropriate strategies.

```rust
/// What kind of content is being processed.
/// This determines which rules activate and how aggressively they fire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessingContext {
    /// Shell commands, command-line arguments.
    /// High sensitivity — commands routinely contain secrets inline.
    Command,
    /// Clipboard text content.
    /// High sensitivity — users copy passwords, tokens, connection strings.
    Clipboard,
    /// Window and tab titles.
    /// Medium sensitivity — may reveal what apps/sites are open.
    /// Lower false-positive tolerance (titles are short, user-visible).
    WindowTitle,
    /// Systemd journal messages and fields.
    /// Medium sensitivity — may contain leaked secrets in error output.
    Journal,
    /// D-Bus method arguments, signal payloads.
    /// Variable — notification bodies are sensitive; bus name changes are not.
    Dbus,
    /// Notification body text (e.g., desktop notifications).
    /// High sensitivity — may contain OTP codes, message previews, auth prompts.
    Notification,
    /// File content or document body text.
    /// High sensitivity but high volume — needs to be fast.
    Document,
    /// Structured metadata fields (hostnames, PIDs, paths).
    /// Low sensitivity for most fields; targeted rules only.
    Metadata,
}
```

### 2.3 Strategy

```rust
/// What to do when a rule matches.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum Strategy {
    /// Replace matched text with a fixed label. Lossy, non-reversible.
    /// Example output: `<AWS_ACCESS_KEY>`
    Redact {
        /// The replacement label. If omitted, uses `<{rule_name}>`.
        label: Option<String>,
    },

    /// Encrypt matched text using XChaCha20-Poly1305 with the system privacy key.
    /// Output: `⌜enc:v1:<b64url(nonce ‖ ciphertext ‖ tag)>⌝`
    /// Reversible with the correct key.
    Encrypt,

    /// Replace with a keyed BLAKE3 MAC. Deterministic for the same input+key.
    /// Output: `⌜hash:<hex[0..16]>⌝`
    /// Allows correlation analysis without exposing plaintext.
    /// Not reversible, but consistent — same input always yields same output.
    Hash,

    /// Drop the containing field entirely. Use for extremely sensitive data
    /// (private key bodies, credential JSON blobs, full auth headers).
    Suppress,

    /// Replace with a format-preserving mask that maintains string length/structure.
    /// Example: `4111-2222-3333-4444` → `4111-XXXX-XXXX-4444`
    /// Useful when downstream systems validate format.
    Mask {
        /// Character to use for masking. Default: 'X'.
        char: Option<char>,
        /// How many characters to preserve at start.
        keep_prefix: Option<usize>,
        /// How many characters to preserve at end.
        keep_suffix: Option<usize>,
    },
}

impl Default for Strategy {
    fn default() -> Self {
        Self::Redact { label: None }
    }
}
```

### 2.4 Matcher

```rust
/// How a rule identifies sensitive content.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Matcher {
    /// Regular expression with capture groups.
    Regex {
        pattern: String,
    },

    /// Structural validator that uses checksums, format rules, or
    /// domain knowledge to reduce false positives vs. regex alone.
    Structural {
        detector: StructuralDetector,
    },

    /// Exact literal match (for known-bad strings, token prefixes, etc.).
    Literal {
        /// The literal text to find (case-insensitive by default).
        text: String,
        case_sensitive: Option<bool>,
    },

    /// Compound: ALL sub-matchers must match (logical AND).
    /// Useful for reducing false positives: "matches credit card regex AND passes Luhn".
    All {
        matchers: Vec<Matcher>,
    },

    /// Compound: ANY sub-matcher suffices (logical OR).
    Any {
        matchers: Vec<Matcher>,
    },
}

/// Structural detectors that use domain knowledge, not just regex.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StructuralDetector {
    /// Payment card numbers with Luhn check-digit validation.
    /// Handles space/dash/dot separators. Rejects non-card numbers.
    CreditCard,
    /// Email addresses. RFC 5322 simplified (no comments/folding).
    /// Catches bare addresses and `mailto:` URIs.
    Email,
    /// Phone numbers in E.164, North American, and European formats.
    /// Requires country code or area code to reduce false positives.
    PhoneNumber,
    /// IPv4 addresses. Excludes loopback (127.*), link-local (169.254.*),
    /// and private ranges (10.*, 172.16-31.*, 192.168.*) by default.
    /// Private range exclusion is configurable.
    Ipv4,
    /// IPv6 addresses. Excludes ::1 and fe80::*.
    Ipv6,
    /// International Bank Account Numbers with country-specific
    /// length validation and mod-97 check digits.
    Iban,
    /// MAC addresses in colon, hyphen, and Cisco dot notation.
    MacAddress,
    /// File paths containing a username component.
    /// Detects /home/<user>/, /Users/<user>/, C:\Users\<user>\.
    /// The username is auto-detected from $USER/$USERNAME or configurable.
    UserHomePath,
    /// Hostname/FQDN of the local machine (from gethostname).
    /// Replaces with `<HOSTNAME>`.
    LocalHostname,
}
```

### 2.5 Pattern Rule

```rust
/// A single privacy rule: how to find sensitive content and what to do with it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternRule {
    /// Unique identifier for this rule. Used in overrides, stats, diagnostics.
    pub name: String,

    /// Human-readable description, shown in `xtask privacy catalog`.
    pub description: String,

    /// Functional category.
    pub category: RuleCategory,

    /// How to find matches.
    pub matcher: Matcher,

    /// What to do with matches.
    pub strategy: Strategy,

    /// Which contexts this rule applies to.
    /// Empty means "all contexts".
    pub contexts: Vec<ProcessingContext>,

    /// Priority for ordering when multiple rules match overlapping ranges.
    /// Higher priority wins. Default: 100.
    #[serde(default = "default_priority")]
    pub priority: u16,

    /// Whether this rule is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_priority() -> u16 { 100 }
fn default_true() -> bool { true }

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuleCategory {
    /// Authentication secrets: API keys, tokens, passwords, private keys.
    Secret,
    /// Personally identifiable information: names, emails, phones, addresses.
    Pii,
    /// Privacy-relevant metadata: window titles revealing activity, notifications.
    ActivityPrivacy,
    /// Infrastructure identifiers: hostnames, IPs, MACs, database URLs.
    Infrastructure,
    /// User-defined rules.
    Custom,
}
```

### 2.6 Built-in Pattern Catalog

Organized by category. Each rule has a sensible default strategy and context binding.

#### Secrets (high confidence, aggressive strategies)

| Rule | Matcher | Default Strategy | Contexts |
|------|---------|-----------------|----------|
| `aws_access_key` | Regex `\b(AKIA\|ASIA\|ABIA\|ACCA)[0-9A-Z]{16}\b` | Redact `<AWS_KEY>` | All |
| `aws_secret_key` | Regex `(?i)(aws_secret_access_key\|secret_access_key)\s*[:=]\s*\S{40}` | Redact | All |
| `private_key_header` | Regex `-----BEGIN .* PRIVATE KEY-----` | Suppress | All |
| `private_key_block` | Regex (multiline PEM body through END marker) | Suppress | All |
| `github_token` | Regex `gh[pousr]_[A-Za-z0-9_]{36,}` | Encrypt | All |
| `gitlab_token` | Regex `glpat-[A-Za-z0-9_-]{20,}` | Encrypt | All |
| `npm_token` | Regex `npm_[A-Za-z0-9]{36,}` | Encrypt | All |
| `slack_token` | Regex `xox[bpsar]-[A-Za-z0-9-]+` | Encrypt | All |
| `stripe_key` | Regex `(sk\|pk\|rk)_(test\|live)_[A-Za-z0-9]{24,}` | Encrypt | All |
| `jwt` | Regex `eyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+` | Redact | All |
| `bearer_token` | Regex `(?i)Bearer\s+[A-Za-z0-9._~+/=-]{20,}` | Encrypt | All |
| `google_api_key` | Regex `AIza[A-Za-z0-9_-]{35}` | Encrypt | All |
| `azure_connection` | Regex `AccountKey=[A-Za-z0-9+/=]{44,}` | Encrypt | All |
| `generic_secret_assign` | Regex `(?i)(password\|secret\|token\|api_key\|apikey)\s*[:=]\s*\S+` | Redact | Command, Journal |
| `cli_secret_flag` | Regex `(?i)--(password\|token\|secret\|key)\s+\S+` | Redact | Command |
| `url_credentials` | Regex `[a-z]+://[^:@\s]+:[^@\s]+@` | Redact | All |
| `database_url` | Regex `(postgres\|mysql\|redis\|mongodb)://\S+` | Encrypt | All |
| `ssh_public_key` | Regex `ssh-(rsa\|ed25519\|ecdsa)\s+AAAA[A-Za-z0-9+/]+` | Redact | All |
| `docker_auth` | Regex `"auth"\s*:\s*"[A-Za-z0-9+/=]+"` | Encrypt | All |

#### PII (needs structural validation to reduce false positives)

| Rule | Matcher | Default Strategy | Contexts |
|------|---------|-----------------|----------|
| `credit_card` | Structural(CreditCard) — Luhn-validated | Mask(keep_prefix=4, keep_suffix=4) | All |
| `ssn` | Regex (tightened: excludes 000, 666, 900-999 area, 00 group, 0000 serial) | Redact | All |
| `email_address` | Structural(Email) | Hash | All |
| `phone_number` | Structural(PhoneNumber) — requires area/country code | Hash | Clipboard, Document, Notification |
| `iban` | Structural(Iban) — mod-97 validated | Mask(keep_prefix=4, keep_suffix=4) | All |

#### Infrastructure (identity leakage for a single-user system)

| Rule | Matcher | Default Strategy | Contexts |
|------|---------|-----------------|----------|
| `ipv4_external` | Structural(Ipv4) — public ranges only | Hash | Journal, Dbus, Metadata |
| `ipv6_external` | Structural(Ipv6) — non-local | Hash | Journal, Dbus, Metadata |
| `mac_address` | Structural(MacAddress) | Hash | All |
| `local_hostname` | Structural(LocalHostname) | Redact `<HOSTNAME>` | Journal, Metadata |
| `user_home_path` | Structural(UserHomePath) | Redact `/home/<USER>/...` | All |

#### Activity Privacy (what you're doing, not authentication)

| Rule | Matcher | Default Strategy | Contexts |
|------|---------|-----------------|----------|
| `password_entry_title` | Regex `(?i)(password\|passwort\|mot de passe\|contraseña)` | Redact `<PASSWORD_ENTRY>` | WindowTitle |
| `login_window_title` | Regex `(?i)(sign.?in\|log.?in\|auth)` | Redact `<LOGIN_WINDOW>` | WindowTitle |
| `password_manager_title` | Regex `(?i)(keepass\|1password\|bitwarden\|lastpass)` | Redact `<PASSWORD_MANAGER>` | WindowTitle |
| `sensitive_file_title` | Regex `(?i)(\.env\|\.pem\|\.key\|id_rsa\|\.gpg)` | Redact `<SENSITIVE_FILE>` | WindowTitle |
| `incognito_title` | Regex `(?i)(incognito\|private.?brows\|inprivate)` | Redact `<PRIVATE_BROWSING>` | WindowTitle |
| `banking_title` | Regex `(?i)(bank\|paypal\|venmo\|wise\|revolut)` | Redact `<FINANCIAL_APP>` | WindowTitle |

### 2.7 PrivacyEngine

```rust
/// Single process-wide privacy engine.
///
/// Constructed once from configuration, then used immutably by all threads.
/// Thread-safe (`Send + Sync`).
pub struct PrivacyEngine {
    rules: Vec<CompiledRule>,
    key: Option<PrivacyKey>,
    stats_enabled: bool,
    stats: DashMap<String, AtomicU64>,  // rule_name → match_count
}

/// A rule with its matcher compiled for fast execution.
struct CompiledRule {
    definition: PatternRule,
    compiled_matcher: CompiledMatcher,
}

/// Result of processing a string.
pub struct Processed<'a> {
    /// The output string. Borrowed if unchanged, owned if modified.
    pub text: Cow<'a, str>,
    /// Which rules matched, in order.
    pub matched_rules: Vec<String>,
    /// Whether any rule triggered a Suppress strategy.
    pub suppressed: bool,
}

impl PrivacyEngine {
    /// Build from configuration.
    pub fn new(config: PrivacyConfig) -> Result<Self, PrivacyError>;

    /// Process a string in the given context.
    pub fn process<'a>(&self, input: &'a str, ctx: ProcessingContext) -> Processed<'a>;

    /// Process all string values in a JSON tree.
    pub fn process_json(
        &self,
        value: &serde_json::Value,
        ctx: ProcessingContext,
    ) -> serde_json::Value;

    /// True if any active rule with a Suppress strategy matches.
    pub fn should_suppress(&self, input: &str, ctx: ProcessingContext) -> bool;

    /// Decrypt an encrypted token `⌜enc:v1:...⌝` back to plaintext.
    pub fn decrypt(&self, token: &str) -> Result<String, PrivacyError>;

    /// Snapshot of per-rule match statistics.
    pub fn stats_snapshot(&self) -> BTreeMap<String, u64>;

    /// Dump the rule catalog as a table (for diagnostics/xtask).
    pub fn catalog(&self) -> Vec<RuleSummary>;
}
```

### 2.8 Global Instance

```rust
use std::sync::OnceLock;

static ENGINE: OnceLock<PrivacyEngine> = OnceLock::new();

/// Get the process-wide privacy engine.
///
/// On first call, initializes from `PrivacyConfig::from_env()`.
/// Panics only if pattern compilation fails (indicates broken built-in patterns — 
/// this would be a build-time bug caught by tests).
pub fn engine() -> &'static PrivacyEngine {
    ENGINE.get_or_init(|| {
        let config = PrivacyConfig::from_env()
            .expect("privacy config loading should not fail with defaults");
        PrivacyEngine::new(config)
            .expect("built-in privacy patterns should compile")
    })
}
```

Only one global. No `lazy_static`. No competing singletons.

---

## 3. Configuration

### 3.1 Config Structure

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyConfig {
    /// Master switch. When false, engine becomes a passthrough.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Which built-in rule categories to activate.
    #[serde(default = "default_all")]
    pub builtin_categories: CategorySet,

    /// Additional user-defined rules.
    /// These are MERGED with (not replacing) built-in rules.
    #[serde(default)]
    pub extra_rules: Vec<PatternRule>,

    /// Overrides for built-in rules by name.
    /// Can disable rules, change their strategy, or restrict their contexts.
    #[serde(default)]
    pub overrides: HashMap<String, RuleOverride>,

    /// Default strategy applied when a rule doesn't specify one.
    #[serde(default)]
    pub default_strategy: Strategy,

    /// Default strategy specifically for the Secret category
    /// (overrides default_strategy for secrets).
    pub secret_strategy: Option<Strategy>,

    /// Key configuration for Encrypt and Hash strategies.
    #[serde(default)]
    pub key: KeyConfig,

    /// Enable per-rule match statistics.
    #[serde(default)]
    pub track_stats: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleOverride {
    /// Set to false to disable a built-in rule.
    pub enabled: Option<bool>,
    /// Override the strategy.
    pub strategy: Option<Strategy>,
    /// Override the context list.
    pub contexts: Option<Vec<ProcessingContext>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CategorySet {
    /// All built-in categories.
    All,
    /// Only these categories.
    Only(Vec<RuleCategory>),
    /// All categories except these.
    Except(Vec<RuleCategory>),
    /// No built-in rules (user defines everything).
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct KeyConfig {
    /// Path to a file containing a 256-bit key (32 bytes, raw or hex-encoded).
    /// Production path: agenix-managed secret file.
    pub key_file: Option<PathBuf>,
    /// Hex-encoded key. Convenience for development. **Not for production.**
    pub key_hex: Option<String>,
}
```

### 3.2 Environment Variables

| Variable | Type | Default | Purpose |
|----------|------|---------|---------|
| `SINEX_PRIVACY_ENABLED` | bool | `true` | Master switch |
| `SINEX_PRIVACY_BUILTIN` | string | `all` | `all`, `none`, or comma-separated categories: `secret,pii,infra,activity` |
| `SINEX_PRIVACY_EXTRA_RULES` | JSON | `[]` | Array of `PatternRule` objects to add |
| `SINEX_PRIVACY_OVERRIDES` | JSON | `{}` | Map of `{ "rule_name": RuleOverride }` |
| `SINEX_PRIVACY_DEFAULT_STRATEGY` | string | `redact` | `redact`, `encrypt`, `hash`, `suppress` |
| `SINEX_PRIVACY_SECRET_STRATEGY` | string | — | Override default for Secret category |
| `SINEX_PRIVACY_KEY_FILE` | path | — | 256-bit key file (agenix) |
| `SINEX_PRIVACY_KEY` | hex | — | Hex key (development only) |
| `SINEX_PRIVACY_STATS` | bool | `false` | Per-rule match counting |

### 3.3 File-based Configuration

For complex setups, a TOML config file at `$SINEX_STATE_DIR/privacy.toml`:

```toml
enabled = true
track_stats = true
default_strategy = { action = "redact" }
secret_strategy = { action = "encrypt" }

[key]
key_file = "/run/agenix/sinex-privacy-key"

[overrides.ssn]
enabled = false

[overrides.email_address]
strategy = { action = "redact", label = "<EMAIL>" }

[[extra_rules]]
name = "internal_project_name"
description = "Redact codename references in external-facing output"
category = "custom"
strategy = { action = "redact", label = "<PROJECT>" }
contexts = ["notification", "clipboard"]
[extra_rules.matcher]
type = "literal"
text = "Project Xenomorph"
```

Load order: built-in defaults → privacy.toml → env vars (env wins).

---

## 4. Encrypted Token Format

```
⌜enc:v1:<base64url(nonce ‖ ciphertext ‖ tag)>⌝
```

- **Delimiters**: `⌜` (U+231C) and `⌝` (U+231D) — visually distinctive, won't appear in natural text, trivially grepable.
- **v1**: Version tag for future algorithm changes.
- **Algorithm**: XChaCha20-Poly1305 (24-byte nonce, AEAD, no nonce reuse risk with random nonces).
- **Encoding**: base64url (no padding) for the nonce+ciphertext+tag blob.
- **Properties**:
  - Surrounding context is preserved: `export KEY=⌜enc:v1:abc123⌝`
  - Different nonces each time → same plaintext produces different tokens (no correlation).
  - Token carries its own nonce — no state needed beyond the key.

### 4.1 Hash Token Format

```
⌜hash:<hex[0..32]>⌝
```

- Keyed BLAKE3 MAC (truncated to 128 bits for readability).
- Deterministic: same input + same key = same output, enabling correlation analysis.
- Not reversible.

### 4.2 Key Absence

If no key is configured:

- `Strategy::Encrypt` degrades to `Strategy::Redact` with a label like `<ENCRYPTED_UNAVAILABLE:rule_name>`.
- `Strategy::Hash` degrades to `Strategy::Redact` similarly.
- A **startup warning** is emitted via `tracing::warn!` once.
- This ensures the system never leaks plaintext due to misconfiguration.

---

## 5. Detection Approaches Beyond Regex

### 5.1 Structural Detectors

These use domain knowledge to dramatically reduce false positives vs. pure regex:

**Credit Card (Luhn):**

- Regex pre-filter extracts candidate digit sequences (13-19 digits).
- Luhn algorithm validates the check digit.
- Rejects IIN ranges that aren't payment cards (e.g., 0000, 1111).
- Handles space, dash, and dot separators.

**Email:**

- Regex captures `local@domain.tld` candidates.
- Validates TLD against a small set of known TLDs (not the full IANA list — just enough to reject `user@localhost`, `build@step.3`).
- Rejects addresses where the domain part looks like a file path or version string.

**Phone Number:**

- Regex captures numeric sequences with optional `+`, parens, dashes, spaces.
- Requires minimum length (7 digits) and an area code or country code prefix.
- This deliberately avoids matching PIDs, UIDs, port numbers, etc.

**IPv4:**

- Regex captures dotted-quad notation.
- Validates each octet is 0-255.
- By default, skips private RFC 1918 ranges (10.*, 172.16-31.*, 192.168.*), loopback (127.*), and APIPA (169.254.*).
- Configurable: `include_private_ranges: bool` in the Ipv4 detector config.
- Rationale: on a local-first single-user system, internal IPs are not sensitive.

**IBAN:**

- Regex captures 2-letter country code + 2 check digits + up to 30 alphanumeric.
- Validates mod-97 check digits per ISO 13616.
- Country-specific length validation.

**UserHomePath:**

- Detects `/home/<user>/`, `/Users/<user>/`, `C:\Users\<user>\` patterns.
- Auto-detects `<user>` from `$USER` / `$USERNAME` environment variable at startup.
- Replaces with `/home/<USER>/...` preserving the relative path suffix.

**LocalHostname:**

- Reads hostname from `gethostname()` at startup.
- Replaces exact matches and FQDN variants with `<HOSTNAME>`.
- Useful for journal entries that embed the machine name.

### 5.2 Compound Matchers for Precision

The `All` and `Any` combinators enable high-precision rules:

```toml
# Credit card that must pass BOTH regex and Luhn
[[extra_rules]]
name = "precise_credit_card"
category = "pii"
strategy = { action = "mask", keep_prefix = 4, keep_suffix = 4 }
[extra_rules.matcher]
type = "all"
matchers = [
  { type = "regex", pattern = "\\b(?:\\d[ -]?){13,19}\\b" },
  { type = "structural", detector = "credit_card" },
]
```

### 5.3 Non-PII Patterns Worth Handling

Beyond PII and secrets, Sinex captures enough context to warrant these categories:

**Activity privacy:**

- Password manager windows (KeePass, Bitwarden, 1Password).
- Banking/financial app titles.
- Private browsing tabs.
- Dating app notifications.
- Medical portal windows.
- Tax preparation software.

**Infrastructure identifiers:**

- Internal hostnames and FQDNs.
- MAC addresses (hardware fingerprinting).
- Docker container IDs.
- Kubernetes pod names containing deployment info.
- Process arguments containing internal service URLs.

**Code/work context:**

- Internal project codenames.
- JIRA/Linear ticket IDs (if the org considers these sensitive).
- Internal wiki URLs.
- Enterprise SSO/SAML assertion fragments.

**Temporal patterns (for future consideration):**

- OTP codes (6-8 digit sequences in notification context).
- MFA challenge/response tokens.
- Session IDs in URLs.

---

## 6. Call Site Migration

Every call site becomes a single-expression change. No wrappers, no re-exports.

### 6.1 Terminal Ingestor (`unified_processor.rs`)

```rust
use sinex_primitives::privacy::{self, ProcessingContext};

let processed = privacy::engine().process(command, ProcessingContext::Command);
if !processed.matched_rules.is_empty() {
    tracing::info!(rules = ?processed.matched_rules, "Privacy rules matched in command");
}
let final_command = processed.text.as_ref();
```

### 6.2 System Ingestor — D-Bus (`dbus_watcher.rs`)

```rust
use sinex_primitives::privacy::{self, ProcessingContext};

// Delete the local `redact_json_strings()` function entirely.
let processed_args = privacy::engine().process_json(&args, ProcessingContext::Dbus);
```

### 6.3 System Ingestor — Journal (`unified_journal_watcher.rs`)

```rust
use sinex_primitives::privacy::{self, ProcessingContext};

let message = obj.get("MESSAGE")
    .and_then(|v| v.as_str())
    .map(|s| privacy::engine().process(s, ProcessingContext::Journal).text.into_owned())
    .unwrap_or_default();

let cmdline = obj.get("_CMDLINE")
    .and_then(|v| v.as_str())
    .map(|s| privacy::engine().process(s, ProcessingContext::Command).text.into_owned());

// Extra fields — use process_json for the whole map
```

### 6.4 Desktop Ingestor — Clipboard (`clipboard.rs`)

```rust
use sinex_primitives::privacy::{self, ProcessingContext};

let processed = privacy::engine().process(&content.text, ProcessingContext::Clipboard);
let privacy_filtered = processed.text.as_ref() != content.text;
let data_bytes = processed.text.as_bytes();
```

### 6.5 Desktop Ingestor — Window Manager (`window_manager.rs`)

```rust
use sinex_primitives::privacy::{self, ProcessingContext};

let title = privacy::engine().process(raw_title, ProcessingContext::WindowTitle).text;
```

---

## 7. xtask Integration

The privacy engine gets first-class tooling via `xtask privacy`. This follows the project's established patterns: clap-derived Args struct, `XtaskCommand` trait impl, structured output via `CommandContext`, and `CommandMetadata::diagnostics()` for the category.

### 7.1 CLI Design

```
xtask privacy <subcommand>

Subcommands:
    catalog     Show all active privacy rules with their configuration
    test        Test privacy processing against sample input
    stats       Show per-rule match statistics from a running instance
    decrypt     Decrypt encrypted privacy tokens
    key         Key management utilities
```

### 7.2 `xtask privacy catalog`

Lists all active rules after config resolution (builtins + extra + overrides). Essential for operators to understand what's active.

```
$ xtask privacy catalog

Privacy Engine Configuration
────────────────────────────
Status:     enabled
Key:        loaded (from /run/agenix/sinex-privacy-key)
Categories: all

Active Rules (32)

  Secrets (19)
  ──────────────
  ✓ aws_access_key       Regex     Redact    All contexts
  ✓ github_token         Regex     Encrypt   All contexts
  ✓ private_key_header   Regex     Suppress  All contexts
  ✗ docker_auth          Regex     —         (disabled via override)
  ...

  PII (5)
  ────────
  ✓ credit_card          Luhn      Mask      All contexts
  ✓ email_address         Email     Hash      All contexts
  ...

  Infrastructure (5)
  ──────────────────
  ✓ local_hostname       Hostname  Redact    Journal, Metadata
  ...
  
  Activity Privacy (6)
  ────────────────────
  ✓ password_entry_title  Regex     Redact    WindowTitle
  ...

$ xtask privacy catalog --json  # structured output for scripting
```

### 7.3 `xtask privacy test`

Interactive testing tool. Feed sample content and see what the engine does — invaluable when writing custom rules or debugging false positives.

```
$ xtask privacy test --context command "export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE"
Input:   export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
Output:  export AWS_ACCESS_KEY_ID=<AWS_KEY>
Rules:   aws_access_key (Secret/Redact)
Context: Command

$ xtask privacy test --context clipboard "Call me at +1-555-867-5309"
Input:   Call me at +1-555-867-5309
Output:  Call me at ⌜hash:a7b3c9d1e5f28a04⌝
Rules:   phone_number (PII/Hash)
Context: Clipboard

$ xtask privacy test --context command --stdin < suspicious_commands.txt
# Process each line, show summary of matched rules

$ echo "multi line\ninput" | xtask privacy test --context journal --stdin
```

**Flags:**

- `--context <ctx>`: Required. One of: command, clipboard, window_title, journal, dbus, notification, document, metadata.
- `--stdin`: Read from stdin (one entry per line).
- `--show-key`: Also show the encryption key ID (last 8 hex chars of key hash).
- `--verbose`: Show all rules evaluated, not just those that matched.

### 7.4 `xtask privacy decrypt`

Decrypts `⌜enc:v1:...⌝` tokens back to plaintext. Requires the privacy key.

```
$ xtask privacy decrypt '⌜enc:v1:abc123def456...⌝'
Decrypted: ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

$ xtask privacy decrypt --stdin < event_dump.json
# Decrypts all tokens found in the input, outputs the decrypted version

$ xtask privacy decrypt --file /path/to/events.jsonl
# Same but from file
```

**Flags:**

- `--key-file <path>`: Override key file (default: from config).
- `--stdin`: Read from stdin.
- `--file <path>`: Read from file.
- `--in-place`: Dangerous — decrypt tokens in the file in-place. Requires `--confirm`.

### 7.5 `xtask privacy key`

Key management utilities:

```
$ xtask privacy key generate
Generated 256-bit key: a4b3c2d1...  (hex)
Write to file:         xtask privacy key generate --out /path/to/key

$ xtask privacy key info
Key source: /run/agenix/sinex-privacy-key
Key ID:     a4b3c2d1 (BLAKE3 hash of key, first 8 hex chars)
Loaded:     yes
Algorithm:  XChaCha20-Poly1305

$ xtask privacy key verify
Verifying key can encrypt and decrypt... OK
```

### 7.6 `xtask privacy stats`

Shows per-rule match counts, useful for understanding what's firing in production. Stats are tracked in-process; this reads from a shared state file or the engine's stats API.

```
$ xtask privacy stats

Rule Match Statistics (since 2026-02-22 03:00:00)
──────────────────────────────────────────────────
  generic_secret_assign    1,247 matches
  url_credentials            834 matches
  user_home_path             612 matches  
  local_hostname             445 matches
  email_address              289 matches
  jwt                         73 matches
  ...
  credit_card                  0 matches

Total: 3,851 matches across 15 rules
```

### 7.7 Implementation Sketch

```rust
// xtask/src/commands/privacy.rs

use clap::Subcommand;
use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};

#[derive(Debug, Clone, clap::Args)]
pub struct PrivacyCommand {
    #[command(subcommand)]
    pub action: PrivacyAction,
}

#[derive(Debug, Clone, Subcommand)]
pub enum PrivacyAction {
    /// Show all active privacy rules
    Catalog {
        /// Filter by category
        #[arg(long)]
        category: Option<String>,
    },
    /// Test privacy processing against sample input
    Test {
        /// Processing context
        #[arg(long)]
        context: String,
        /// Input text (omit for --stdin)
        input: Option<String>,
        /// Read from stdin
        #[arg(long)]
        stdin: bool,
        /// Show all rules evaluated
        #[arg(long)]
        verbose: bool,
    },
    /// Decrypt privacy tokens
    Decrypt {
        /// Token to decrypt (omit for --stdin/--file)
        token: Option<String>,
        #[arg(long)]
        stdin: bool,
        #[arg(long)]
        file: Option<PathBuf>,
        #[arg(long)]
        key_file: Option<PathBuf>,
    },
    /// Key management
    Key {
        #[command(subcommand)]
        action: KeyAction,
    },
    /// Show per-rule match statistics
    Stats,
}

#[derive(Debug, Clone, Subcommand)]
pub enum KeyAction {
    /// Generate a new privacy key
    Generate {
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Show info about the current key
    Info,
    /// Verify the key works correctly
    Verify,
}

#[async_trait::async_trait]
impl XtaskCommand for PrivacyCommand {
    fn name(&self) -> &'static str { "privacy" }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::diagnostics()
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        match &self.action {
            PrivacyAction::Catalog { category } => exec_catalog(ctx, category.as_deref()).await,
            PrivacyAction::Test { context, input, stdin, verbose } =>
                exec_test(ctx, context, input.as_deref(), *stdin, *verbose).await,
            PrivacyAction::Decrypt { token, stdin, file, key_file } =>
                exec_decrypt(ctx, token.as_deref(), *stdin, file.as_deref(), key_file.as_deref()).await,
            PrivacyAction::Key { action } => exec_key(ctx, action).await,
            PrivacyAction::Stats => exec_stats(ctx).await,
        }
    }
}
```

---

## 8. Dependencies

New crate dependencies for `sinex-primitives`:

```toml
[dependencies]
chacha20poly1305 = "0.10"   # XChaCha20-Poly1305, ~40KB, pure Rust, constant-time
base64 = "0.22"             # Already in workspace dep tree
blake3 = { workspace = true }  # Already in workspace — used for Hash strategy + key derivation
dashmap = "6"               # Lock-free concurrent map for stats (already in dep tree via node-sdk)
```

No new system dependencies. No FFI. No OpenSSL. No GPG.

---

## 9. Security Properties

**Key compromise:** If `SINEX_PRIVACY_KEY` leaks, all encrypted tokens are decryptable. Mitigated by:

- Key file permissions (0400, owned by sinex user).
- agenix integration for NixOS deployments.
- Key rotation via `xtask privacy key generate` + re-encrypt.
- Encrypted tokens are meaningless without the key, so database dumps are safe.

**Nonce reuse:** XChaCha20's 24-byte nonce space (2^192) makes random nonce collision negligible even at billions of tokens per year.

**Side channels:** The `chacha20poly1305` crate provides constant-time implementations.

**Configuration failure:** If no key is configured, Encrypt/Hash degrade to Redact. The system never leaks plaintext due to missing configuration.

**Test isolation:** `PrivacyEngine::new()` takes explicit config. Tests never touch the global instance. No `lazy_static` means no test pollution.

**Performance target:** <0.5ms per event for a typical payload. The engine pre-compiles all regex patterns into a `RegexSet` for single-pass matching, and structural detectors short-circuit on format pre-checks.

---

## 10. Implementation Plan

### Phase 1: Core Engine (foundation)

1. Create `sinex-primitives/src/privacy/` module tree.
2. Implement `PrivacyConfig`, `PatternRule`, `Strategy`, `ProcessingContext`, `Matcher`.
3. Implement `PrivacyEngine` with `process()` and `process_json()`.
4. Port all 14 existing content patterns + 4 title patterns as built-in rules.
5. Implement `engine()` global via `OnceLock`.
6. Add `from_env()` config loading with merge semantics.
7. **Migrate all call sites** to `privacy::engine().process(...)`.
8. Delete `secret_redaction.rs`, `redaction_config.rs`, `privacy_filter.rs`, terminal re-export.
9. Run full test suite, ensure no regressions.

### Phase 2: Structural Detectors + Extended Patterns

1. Implement `StructuralDetector` for: CreditCard (Luhn), Email, PhoneNumber, Ipv4, Ipv6.
2. Add Mask strategy.
3. Add compound matchers (All, Any).
4. Add new rule categories: Infrastructure, Activity Privacy.
5. Tighten SSN pattern.
6. Add UserHomePath and LocalHostname detectors.
7. Property tests for all structural detectors (proptest).

### Phase 3: Encryption

1. Add `chacha20poly1305` dependency.
2. Implement `⌜enc:v1:...⌝` token format (encode/decode).
3. Implement `⌜hash:...⌝` token format.
4. Implement `KeyConfig` loading (file → env var → absent → degrade).
5. Implement `Strategy::Encrypt` and `Strategy::Hash` in the engine.
6. Implement `PrivacyEngine::decrypt()`.
7. Update default strategy for Secret category to Encrypt (when key is available).

### Phase 4: xtask Tooling

1. Implement `xtask privacy catalog`.
2. Implement `xtask privacy test`.
3. Implement `xtask privacy decrypt`.
4. Implement `xtask privacy key {generate, info, verify}`.
5. Implement `xtask privacy stats`.
6. Register in `commands/mod.rs`, wire into clap dispatch.

### Phase 5: Polish

1. TOML file-based configuration support.
2. `from_file()` config loader.
3. IBAN and MAC address structural detectors.
4. Documentation: update `docs/current/security.md`.
5. Integration tests for privacy engine in sandbox environment.
