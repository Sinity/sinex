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

// ─── Dispatcher ──────────────────────────────────────────────

use super::StructuralDetector;

/// Find all matches for a structural detector, returning byte ranges.
pub fn find_matches(detector: StructuralDetector, input: &str) -> Vec<(usize, usize)> {
    match detector {
        StructuralDetector::CreditCard => find_credit_cards(input),
        StructuralDetector::Email => find_emails(input),
        StructuralDetector::PhoneNumber => find_phones(input),
        StructuralDetector::Iban => find_ibans(input),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Luhn ──

    #[test]
    fn luhn_valid_visa() {
        assert!(is_luhn_valid("4111111111111111"));
    }

    #[test]
    fn luhn_valid_mastercard() {
        assert!(is_luhn_valid("5500000000000004"));
    }

    #[test]
    fn luhn_invalid() {
        assert!(!is_luhn_valid("4111111111111112"));
    }

    #[test]
    fn luhn_too_short() {
        assert!(!is_luhn_valid("411111"));
    }

    // ── Credit card ──

    #[test]
    fn cc_finds_valid_number() {
        let matches = find_credit_cards("card: 4111 1111 1111 1111 ok");
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn cc_rejects_random_digits() {
        let matches = find_credit_cards("number: 1234567890123456");
        assert!(matches.is_empty()); // fails Luhn
    }

    // ── Email ──

    #[test]
    fn email_finds_address() {
        let matches = find_emails("contact user@example.com please");
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn email_rejects_version() {
        let matches = find_emails("dep user@1.2.3");
        assert!(matches.is_empty());
    }

    // ── Phone ──

    #[test]
    fn phone_finds_international() {
        let matches = find_phones("call +1-555-867-5309 now");
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn phone_finds_parens() {
        let matches = find_phones("call (212) 555-1234");
        assert_eq!(matches.len(), 1);
    }

    // ── IBAN ──

    #[test]
    fn iban_valid_german() {
        assert!(is_valid_iban("DE89370400440532013000"));
    }

    #[test]
    fn iban_valid_gb() {
        assert!(is_valid_iban("GB29 NWBK 6016 1331 9268 19"));
    }

    #[test]
    fn iban_invalid() {
        assert!(!is_valid_iban("DE00000000000000000000"));
    }
}
