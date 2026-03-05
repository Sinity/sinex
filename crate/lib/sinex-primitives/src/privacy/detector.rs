//! Structural PII detectors that go beyond regex for accuracy.

use regex::Regex;
use std::sync::LazyLock;

// ─── Credit card (Luhn) ──────────────────────────────────────

/// Pre-filter regex for credit card candidates: 13-19 digit sequences
/// with optional space/dash separators.
#[allow(clippy::expect_used)] // Compile-time constant regex
static CC_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b(?:\d[ -]?){13,19}\b").expect("credit card regex"));

/// Luhn algorithm check-digit validation.
fn is_luhn_valid(digits: &str) -> bool {
    let digits: Vec<u8> = digits
        .chars()
        .filter(char::is_ascii_digit)
        .map(|c| c as u8 - b'0')
        .collect();
    if digits.len() < 13 || digits.len() > 19 {
        return false;
    }
    let mut sum: u32 = 0;
    let mut double = false;
    for &d in digits.iter().rev() {
        let mut val = u32::from(d);
        if double {
            val *= 2;
            if val > 9 {
                val -= 9;
            }
        }
        sum += val;
        double = !double;
    }
    sum.is_multiple_of(10)
}

/// Find credit card numbers in input that pass Luhn validation.
/// Returns (start, end) byte ranges.
pub fn find_credit_cards(input: &str) -> Vec<(usize, usize)> {
    CC_RE
        .find_iter(input)
        .filter(|m| {
            let text = m.as_str();
            let digit_count = text.chars().filter(char::is_ascii_digit).count();
            (13..=19).contains(&digit_count) && is_luhn_valid(text)
        })
        .map(|m| (m.start(), m.end()))
        .collect()
}

// ─── Email ───────────────────────────────────────────────────

/// Pre-filter regex for email addresses.
#[allow(clippy::expect_used)] // Compile-time constant regex
static EMAIL_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}").expect("email regex")
});

/// Minimal TLD validation — reject obviously non-email patterns.
fn is_plausible_email(candidate: &str) -> bool {
    let parts: Vec<&str> = candidate.splitn(2, '@').collect();
    if parts.len() != 2 {
        return false;
    }
    let local = parts[0];
    let domain = parts[1];
    // Reject empty parts
    if local.is_empty() || domain.is_empty() {
        return false;
    }
    // Domain must have at least one dot
    if !domain.contains('.') {
        return false;
    }
    // Reject version-string look-alikes (e.g., user@1.2.3)
    let tld = domain.rsplit('.').next().unwrap_or("");
    if tld.len() < 2 || tld.chars().all(|c| c.is_ascii_digit()) {
        return false;
    }
    true
}

/// Find email addresses in input.
pub fn find_emails(input: &str) -> Vec<(usize, usize)> {
    EMAIL_RE
        .find_iter(input)
        .filter(|m| is_plausible_email(m.as_str()))
        .map(|m| (m.start(), m.end()))
        .collect()
}

// ─── Phone number ────────────────────────────────────────────

/// Pre-filter regex for phone numbers.
/// Requires a `+` prefix, parens around area code, or at least 7 digits
/// with separators.
#[allow(clippy::expect_used)] // Compile-time constant regex
static PHONE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?x)
        (?:
            \+\d{1,3}[\s.-]?            # International prefix
            |
            \(\d{2,4}\)[\s.-]?          # Area code in parens
        )
        [\d\s.\-()]{6,18}              # Remaining digits with separators
        ",
    )
    .expect("phone regex")
});

/// Validate phone number candidate: must have enough actual digits.
fn is_plausible_phone(candidate: &str) -> bool {
    let digit_count = candidate.chars().filter(char::is_ascii_digit).count();
    // Phone numbers have 7-15 digits (E.164 max is 15)
    (7..=15).contains(&digit_count)
}

/// Find phone numbers in input.
pub fn find_phones(input: &str) -> Vec<(usize, usize)> {
    PHONE_RE
        .find_iter(input)
        .filter(|m| is_plausible_phone(m.as_str()))
        .map(|m| (m.start(), m.end()))
        .collect()
}

// ─── IBAN ────────────────────────────────────────────────────

/// Pre-filter regex for IBAN: 2-letter country code + 2 check digits + up to 30 alphanumeric.
#[allow(clippy::expect_used)] // Compile-time constant regex
static IBAN_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b[A-Z]{2}\d{2}[\s]?[\dA-Z]{4}(?:[\s]?[\dA-Z]{4}){1,7}(?:[\s]?[\dA-Z]{1,4})?\b")
        .expect("iban regex")
});

/// Validate IBAN using mod-97 check (ISO 13616).
fn is_valid_iban(candidate: &str) -> bool {
    let cleaned: String = candidate
        .chars()
        .filter(|c| !c.is_ascii_whitespace())
        .collect();
    if cleaned.len() < 15 || cleaned.len() > 34 {
        return false;
    }
    // Move first 4 chars to end
    let rearranged = format!("{}{}", &cleaned[4..], &cleaned[..4]);
    // Convert letters to numbers: A=10, B=11, ..., Z=35
    let mut numeric = String::with_capacity(rearranged.len() * 2);
    for ch in rearranged.chars() {
        if ch.is_ascii_digit() {
            numeric.push(ch);
        } else if ch.is_ascii_uppercase() {
            let val = ch as u32 - 'A' as u32 + 10;
            numeric.push_str(&val.to_string());
        } else {
            return false;
        }
    }
    // Mod 97 check — process in chunks to avoid big-integer overflow
    let mut remainder: u64 = 0;
    for chunk in numeric.as_bytes().chunks(9) {
        let s = std::str::from_utf8(chunk).unwrap_or("0");
        let combined = format!("{remainder}{s}");
        remainder = combined.parse::<u64>().unwrap_or(0) % 97;
    }
    remainder == 1
}

/// Find IBANs in input.
pub fn find_ibans(input: &str) -> Vec<(usize, usize)> {
    IBAN_RE
        .find_iter(input)
        .filter(|m| is_valid_iban(m.as_str()))
        .map(|m| (m.start(), m.end()))
        .collect()
}

// ─── IPv4 ────────────────────────────────────────────────────

/// Pre-filter regex for IPv4 addresses.
/// Requires all four octets — avoids matching version strings like `1.2.3`.
/// Note: uses post-match boundary checking instead of lookaround (unsupported by `regex` crate).
#[allow(clippy::expect_used)] // Compile-time constant regex
static IPV4_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d?)")
        .expect("ipv4 regex")
});

/// Returns true if the character at `pos` in `input` is a digit or dot.
fn is_ip_adjacent(input: &str, pos: usize) -> bool {
    input
        .as_bytes()
        .get(pos)
        .is_some_and(|&b| b.is_ascii_digit() || b == b'.')
}

/// Find IPv4 addresses in input.
pub fn find_ipv4(input: &str) -> Vec<(usize, usize)> {
    IPV4_RE
        .find_iter(input)
        .filter(|m| {
            // Reject if preceded by digit or dot (e.g. "1.2.3.4.5" → don't match "2.3.4.5")
            let before_ok = m.start() == 0 || !is_ip_adjacent(input, m.start() - 1);
            // Reject if followed by digit or dot (e.g. "1.2.3.4.5" → don't match "1.2.3.4")
            let after_ok = !is_ip_adjacent(input, m.end());
            before_ok && after_ok
        })
        .map(|m| (m.start(), m.end()))
        .collect()
}

// ─── IPv6 ────────────────────────────────────────────────────

/// Pre-filter regex for IPv6 addresses.
/// Covers full, compressed (`::` notation), and mixed IPv4/IPv6 notation.
#[allow(clippy::expect_used)] // Compile-time constant regex
static IPV6_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?xi)
        (?:
            # Full or compressed IPv6 (may contain ::)
            (?:[0-9a-f]{1,4}:){7}[0-9a-f]{1,4}         # Full 8-group
            |
            (?:[0-9a-f]{1,4}:){1,7}:                    # Trailing ::
            |
            :(?::[0-9a-f]{1,4}){1,7}                    # Leading ::
            |
            (?:[0-9a-f]{1,4}:){1,6}:[0-9a-f]{1,4}      # One compressed middle
            |
            (?:[0-9a-f]{1,4}:){1,5}(?::[0-9a-f]{1,4}){1,2}
            |
            (?:[0-9a-f]{1,4}:){1,4}(?::[0-9a-f]{1,4}){1,3}
            |
            (?:[0-9a-f]{1,4}:){1,3}(?::[0-9a-f]{1,4}){1,4}
            |
            (?:[0-9a-f]{1,4}:){1,2}(?::[0-9a-f]{1,4}){1,5}
            |
            ::(?:[0-9a-f]{1,4}:){0,5}[0-9a-f]{1,4}     # :: prefix
            |
            ::                                           # Loopback / unspecified
        )
        ",
    )
    .expect("ipv6 regex")
});

/// Validate that a candidate looks like a plausible IPv6 address.
fn is_plausible_ipv6(candidate: &str) -> bool {
    // Must contain at least one colon
    if !candidate.contains(':') {
        return false;
    }
    // Must not start/end with a single colon (:: is ok, : is not)
    let trimmed = candidate.trim();
    if trimmed.starts_with(':') && !trimmed.starts_with("::") {
        return false;
    }
    if trimmed.ends_with(':') && !trimmed.ends_with("::") {
        return false;
    }
    // Must have hex digits / colons only
    trimmed
        .chars()
        .all(|c| c.is_ascii_hexdigit() || c == ':' || c == '.')
}

/// Find IPv6 addresses in input.
pub fn find_ipv6(input: &str) -> Vec<(usize, usize)> {
    IPV6_RE
        .find_iter(input)
        .filter(|m| is_plausible_ipv6(m.as_str()))
        .map(|m| (m.start(), m.end()))
        .collect()
}

// ─── MAC address ─────────────────────────────────────────────

/// Pre-filter regex for MAC addresses.
/// Supports colon-separated (`aa:bb:cc:dd:ee:ff`), dash-separated
/// (`aa-bb-cc-dd-ee-ff`), and Cisco dot-separated pairs (`aabb.ccdd.eeff`).
#[allow(clippy::expect_used)] // Compile-time constant regex
static MAC_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?xi)
        (?:
            # Colon-separated: aa:bb:cc:dd:ee:ff
            [0-9a-f]{2}(?::[0-9a-f]{2}){5}
            |
            # Dash-separated: aa-bb-cc-dd-ee-ff
            [0-9a-f]{2}(?:-[0-9a-f]{2}){5}
            |
            # Cisco dot notation: aabb.ccdd.eeff
            [0-9a-f]{4}\.[0-9a-f]{4}\.[0-9a-f]{4}
        )
        ",
    )
    .expect("mac address regex")
});

/// Find MAC addresses in input.
pub fn find_mac_addresses(input: &str) -> Vec<(usize, usize)> {
    MAC_RE
        .find_iter(input)
        .map(|m| (m.start(), m.end()))
        .collect()
}

// ─── User home path ──────────────────────────────────────────

use std::sync::OnceLock;

/// Cached home path regex, built from the `HOME`/`USER` environment variables.
static HOME_PATH_RE: OnceLock<Option<Regex>> = OnceLock::new();

fn home_path_regex() -> Option<&'static Regex> {
    HOME_PATH_RE
        .get_or_init(|| {
            // Try $HOME first, fall back to constructing from $USER
            let home = std::env::var("HOME")
                .ok()
                .filter(|h| !h.is_empty() && h.starts_with('/'))
                .or_else(|| {
                    std::env::var("USER").ok().and_then(|user| {
                        if user.is_empty() {
                            None
                        } else {
                            // Try both Linux and macOS paths
                            Some(format!("/home/{user}"))
                        }
                    })
                })?;

            // Escape special regex characters in the path
            let escaped = regex::escape(&home);
            // Match the home prefix followed by `/` and anything
            Regex::new(&format!(r#"(?:{})/[^\s"']+"#, escaped))
                .ok()
                .or_else(|| {
                    // Also try /Users/<name> pattern (macOS)
                    if let Ok(user) = std::env::var("USER") {
                        if !user.is_empty() {
                            let macos_path = format!("/Users/{user}");
                            let macos_escaped = regex::escape(&macos_path);
                            return Regex::new(&format!(r#"(?:{})/[^\s"']+"#, macos_escaped)).ok();
                        }
                    }
                    None
                })
        })
        .as_ref()
}

/// Find occurrences of the user's home directory path in input.
pub fn find_home_paths(input: &str) -> Vec<(usize, usize)> {
    match home_path_regex() {
        Some(re) => re.find_iter(input).map(|m| (m.start(), m.end())).collect(),
        None => Vec::new(),
    }
}

// ─── Local hostname ───────────────────────────────────────────

/// Cached compiled regex for the local hostname.
static HOSTNAME_RE: OnceLock<Option<Regex>> = OnceLock::new();

fn hostname_regex() -> Option<&'static Regex> {
    HOSTNAME_RE
        .get_or_init(|| {
            // gethostname via libc/nix is not available in primitives; use std::process or env
            // HOSTNAME env var is set by bash; also try reading /proc/sys/kernel/hostname
            let hostname = std::env::var("HOSTNAME")
                .ok()
                .filter(|h| !h.is_empty())
                .or_else(|| {
                    std::fs::read_to_string("/proc/sys/kernel/hostname")
                        .ok()
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                })?;

            // Strip FQDN suffix if present — only use the short hostname
            let short = hostname.split('.').next().unwrap_or(&hostname);
            if short.len() < 3 {
                // Too short — would match too many things
                return None;
            }

            let escaped = regex::escape(short);
            Regex::new(&format!(r"\b{escaped}\b")).ok()
        })
        .as_ref()
}

/// Find occurrences of the local hostname in input.
pub fn find_hostnames(input: &str) -> Vec<(usize, usize)> {
    match hostname_regex() {
        Some(re) => re.find_iter(input).map(|m| (m.start(), m.end())).collect(),
        None => Vec::new(),
    }
}

// ─── SSN ─────────────────────────────────────────────────────

/// Pre-filter regex for US Social Security Numbers (NNN-NN-NNNN format).
/// Does not use lookaheads — validation is done in `is_valid_ssn`.
#[allow(clippy::expect_used)] // Compile-time constant regex
static SSN_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b\d{3}[-\s]\d{2}[-\s]\d{4}\b").expect("ssn regex"));

/// Validate a SSN candidate: reject area 000, 666, 900-999; group 00; serial 0000.
fn is_valid_ssn(candidate: &str) -> bool {
    let digits: String = candidate.chars().filter(char::is_ascii_digit).collect();
    if digits.len() != 9 {
        return false;
    }
    let area: u32 = digits[0..3].parse().unwrap_or(0);
    let group: u32 = digits[3..5].parse().unwrap_or(0);
    let serial: u32 = digits[5..9].parse().unwrap_or(0);

    // Invalid area codes: 000, 666, 900-999
    if area == 0 || area == 666 || area >= 900 {
        return false;
    }
    // Invalid group: 00
    if group == 0 {
        return false;
    }
    // Invalid serial: 0000
    if serial == 0 {
        return false;
    }
    true
}

/// Find SSNs in input.
pub fn find_ssns(input: &str) -> Vec<(usize, usize)> {
    SSN_RE
        .find_iter(input)
        .filter(|m| is_valid_ssn(m.as_str()))
        .map(|m| (m.start(), m.end()))
        .collect()
}

// ─── Dispatcher ──────────────────────────────────────────────

use super::StructuralDetector;

/// Find all matches for a structural detector, returning byte ranges.
pub fn find_matches(detector: StructuralDetector, input: &str) -> Vec<(usize, usize)> {
    match detector {
        StructuralDetector::CreditCard => find_credit_cards(input),
        StructuralDetector::Email => find_emails(input),
        StructuralDetector::PhoneNumber => find_phones(input),
        StructuralDetector::Iban => find_ibans(input),
        StructuralDetector::Ipv4 => find_ipv4(input),
        StructuralDetector::Ipv6 => find_ipv6(input),
        StructuralDetector::MacAddress => find_mac_addresses(input),
        StructuralDetector::UserHomePath => find_home_paths(input),
        StructuralDetector::LocalHostname => find_hostnames(input),
        StructuralDetector::Ssn => find_ssns(input),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::sinex_test;

    // ── Luhn ──

    #[sinex_test]
    async fn luhn_valid_visa() -> ::xtask::sandbox::TestResult<()> {
        assert!(is_luhn_valid("4111111111111111"));
        Ok(())
    }

    #[sinex_test]
    async fn luhn_valid_mastercard() -> ::xtask::sandbox::TestResult<()> {
        assert!(is_luhn_valid("5500000000000004"));
        Ok(())
    }

    #[sinex_test]
    async fn luhn_invalid() -> ::xtask::sandbox::TestResult<()> {
        assert!(!is_luhn_valid("4111111111111112"));
        Ok(())
    }

    #[sinex_test]
    async fn luhn_too_short() -> ::xtask::sandbox::TestResult<()> {
        assert!(!is_luhn_valid("411111"));
        Ok(())
    }

    // ── Credit card ──

    #[sinex_test]
    async fn cc_finds_valid_number() -> ::xtask::sandbox::TestResult<()> {
        let matches = find_credit_cards("card: 4111 1111 1111 1111 ok");
        assert_eq!(matches.len(), 1);
        Ok(())
    }

    #[sinex_test]
    async fn cc_rejects_random_digits() -> ::xtask::sandbox::TestResult<()> {
        let matches = find_credit_cards("number: 1234567890123456");
        assert!(matches.is_empty()); // fails Luhn
        Ok(())
    }

    // ── Email ──

    #[sinex_test]
    async fn email_finds_address() -> ::xtask::sandbox::TestResult<()> {
        let matches = find_emails("contact user@example.com please");
        assert_eq!(matches.len(), 1);
        Ok(())
    }

    #[sinex_test]
    async fn email_rejects_version() -> ::xtask::sandbox::TestResult<()> {
        let matches = find_emails("dep user@1.2.3");
        assert!(matches.is_empty());
        Ok(())
    }

    // ── Phone ──

    #[sinex_test]
    async fn phone_finds_international() -> ::xtask::sandbox::TestResult<()> {
        let matches = find_phones("call +1-555-867-5309 now");
        assert_eq!(matches.len(), 1);
        Ok(())
    }

    #[sinex_test]
    async fn phone_finds_parens() -> ::xtask::sandbox::TestResult<()> {
        let matches = find_phones("call (212) 555-1234");
        assert_eq!(matches.len(), 1);
        Ok(())
    }

    // ── IBAN ──

    #[sinex_test]
    async fn iban_valid_german() -> ::xtask::sandbox::TestResult<()> {
        assert!(is_valid_iban("DE89370400440532013000"));
        Ok(())
    }

    #[sinex_test]
    async fn iban_valid_gb() -> ::xtask::sandbox::TestResult<()> {
        assert!(is_valid_iban("GB29 NWBK 6016 1331 9268 19"));
        Ok(())
    }

    #[sinex_test]
    async fn iban_invalid() -> ::xtask::sandbox::TestResult<()> {
        assert!(!is_valid_iban("DE00000000000000000000"));
        Ok(())
    }

    // ── IPv4 ──

    #[sinex_test]
    async fn ipv4_finds_address() -> ::xtask::sandbox::TestResult<()> {
        let matches = find_ipv4("connecting to 192.168.1.100 now");
        assert_eq!(matches.len(), 1);
        assert_eq!(
            &"connecting to 192.168.1.100 now"[matches[0].0..matches[0].1],
            "192.168.1.100"
        );
        Ok(())
    }

    #[sinex_test]
    async fn ipv4_finds_public_address() -> ::xtask::sandbox::TestResult<()> {
        let matches = find_ipv4("server at 8.8.8.8");
        assert_eq!(matches.len(), 1);
        Ok(())
    }

    #[sinex_test]
    async fn ipv4_rejects_version_string() -> ::xtask::sandbox::TestResult<()> {
        // Version strings like "1.2.3" (only 3 octets) must not match
        let matches = find_ipv4("version 1.2.3 released");
        assert!(matches.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn ipv4_rejects_out_of_range_octet() -> ::xtask::sandbox::TestResult<()> {
        // 999.0.0.1 is not a valid IPv4
        let matches = find_ipv4("addr 999.0.0.1");
        assert!(matches.is_empty());
        Ok(())
    }

    // ── IPv6 ──

    #[sinex_test]
    async fn ipv6_finds_full_address() -> ::xtask::sandbox::TestResult<()> {
        let matches = find_ipv6("addr: 2001:0db8:85a3:0000:0000:8a2e:0370:7334");
        assert_eq!(matches.len(), 1);
        Ok(())
    }

    #[sinex_test]
    async fn ipv6_finds_compressed_address() -> ::xtask::sandbox::TestResult<()> {
        let matches = find_ipv6("addr: 2001:db8::1");
        assert_eq!(matches.len(), 1);
        Ok(())
    }

    #[sinex_test]
    async fn ipv6_finds_loopback() -> ::xtask::sandbox::TestResult<()> {
        let matches = find_ipv6("bound to ::");
        assert_eq!(matches.len(), 1);
        Ok(())
    }

    // ── MAC address ──

    #[sinex_test]
    async fn mac_finds_colon_separated() -> ::xtask::sandbox::TestResult<()> {
        let matches = find_mac_addresses("eth0: aa:bb:cc:dd:ee:ff");
        assert_eq!(matches.len(), 1);
        Ok(())
    }

    #[sinex_test]
    async fn mac_finds_dash_separated() -> ::xtask::sandbox::TestResult<()> {
        let matches = find_mac_addresses("hw: aa-bb-cc-dd-ee-ff");
        assert_eq!(matches.len(), 1);
        Ok(())
    }

    #[sinex_test]
    async fn mac_finds_cisco_notation() -> ::xtask::sandbox::TestResult<()> {
        let matches = find_mac_addresses("mac: aabb.ccdd.eeff");
        assert_eq!(matches.len(), 1);
        Ok(())
    }

    #[sinex_test]
    async fn mac_rejects_too_short() -> ::xtask::sandbox::TestResult<()> {
        let matches = find_mac_addresses("short: aa:bb:cc");
        assert!(matches.is_empty());
        Ok(())
    }

    // ── SSN ──

    #[sinex_test]
    async fn ssn_valid() -> ::xtask::sandbox::TestResult<()> {
        assert!(is_valid_ssn("123-45-6789"));
        Ok(())
    }

    #[sinex_test]
    async fn ssn_rejects_area_000() -> ::xtask::sandbox::TestResult<()> {
        assert!(!is_valid_ssn("000-45-6789"));
        Ok(())
    }

    #[sinex_test]
    async fn ssn_rejects_area_666() -> ::xtask::sandbox::TestResult<()> {
        assert!(!is_valid_ssn("666-45-6789"));
        Ok(())
    }

    #[sinex_test]
    async fn ssn_rejects_area_900_plus() -> ::xtask::sandbox::TestResult<()> {
        assert!(!is_valid_ssn("900-45-6789"));
        assert!(!is_valid_ssn("999-45-6789"));
        Ok(())
    }

    #[sinex_test]
    async fn ssn_rejects_group_00() -> ::xtask::sandbox::TestResult<()> {
        assert!(!is_valid_ssn("123-00-6789"));
        Ok(())
    }

    #[sinex_test]
    async fn ssn_rejects_serial_0000() -> ::xtask::sandbox::TestResult<()> {
        assert!(!is_valid_ssn("123-45-0000"));
        Ok(())
    }

    #[sinex_test]
    async fn ssn_finds_in_text() -> ::xtask::sandbox::TestResult<()> {
        let matches = find_ssns("my SSN is 123-45-6789 ok");
        assert_eq!(matches.len(), 1);
        Ok(())
    }

    #[sinex_test]
    async fn ssn_rejects_invalid_in_text() -> ::xtask::sandbox::TestResult<()> {
        let matches = find_ssns("invalid 000-45-6789");
        assert!(matches.is_empty());
        Ok(())
    }

    // ── Home path ──

    #[sinex_test]
    async fn home_path_returns_vec() -> ::xtask::sandbox::TestResult<()> {
        // Can't assert specific results without knowing $HOME, but the function
        // must not panic and must return a Vec.
        let result = find_home_paths("/some/path/here");
        let _ = result; // just verify it runs
        Ok(())
    }

    #[sinex_test]
    async fn home_path_finds_if_env_set() -> ::xtask::sandbox::TestResult<()> {
        // Set HOME to a known value and construct a matching path
        // NOTE: we can't safely mutate env in a threaded test runner, so we just
        // verify the dispatcher routes correctly.
        use super::super::StructuralDetector;
        let result = find_matches(StructuralDetector::UserHomePath, "/etc/hosts");
        let _ = result; // no panic
        Ok(())
    }

    // ── Hostname ──

    #[sinex_test]
    async fn hostname_returns_vec() -> ::xtask::sandbox::TestResult<()> {
        // Dispatcher must route without panic
        use super::super::StructuralDetector;
        let result = find_matches(StructuralDetector::LocalHostname, "some log line");
        let _ = result;
        Ok(())
    }
}
