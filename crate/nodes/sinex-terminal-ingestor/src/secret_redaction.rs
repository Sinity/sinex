use lazy_static::lazy_static;
use regex::Regex;
use std::borrow::Cow;

/// Redactor for sensitive information in terminal commands
pub struct SecretRedactor;

struct RedactionPattern {
    _name: &'static str, // Kept for documentation/debugging
    regex: Regex,
    placeholder: &'static str,
}

lazy_static! {
    static ref PATTERNS: Vec<RedactionPattern> = vec![
        // AWS Access Key ID (AKIA/ASIA...)
        RedactionPattern {
            _name: "aws_access_key",
            regex: Regex::new(r"(?i)\b(AKIA|ASIA|ABIA|ACCA)[0-9A-Z]{16}\b").unwrap(),
            placeholder: "<AWS_ACCESS_KEY>",
        },
        // URLs with credentials
        RedactionPattern {
            _name: "url_credentials",
            regex: Regex::new(r"(?i)([a-z]+://)([^:/]+):([^@]+)@").unwrap(),
            placeholder: "${1}${2}:<REDACTED>@",
        },
        // Private Key Headers
        RedactionPattern {
            _name: "private_key_header",
            regex: Regex::new(r"(?i)-----BEGIN[ A-Z]+PRIVATE KEY-----").unwrap(),
            placeholder: "<PRIVATE_KEY_HEADER>",
        },
    ];

    // Improved AWS Secret Key pattern that looks for keys often found in CLI args or env vars
    // Matches 40 char alphanumeric strings only if they look "random" (mixed case, numbers) and are likely an argument
    // Regex is tricky for "randomness", so we rely on length and charset.
    // We'll target specific flags.
    static ref CLI_FLAG_SECRET: Regex = Regex::new(r"(?i)(--password|--secret|--token|--key)\s+([^\s]+)").unwrap();
}

impl SecretRedactor {
    /// Redact sensitive information from the input string
    pub fn redact(input: &str) -> Cow<'_, str> {
        let mut result = Cow::Borrowed(input);

        // Apply global patterns
        for pattern in PATTERNS.iter() {
            if pattern.regex.is_match(&result) {
                let redacted = pattern.regex.replace_all(&result, pattern.placeholder);
                result = Cow::Owned(redacted.into_owned());
            }
        }

        // Apply CLI flag redaction specific logic logic
        if CLI_FLAG_SECRET.is_match(&result) {
            let redacted = CLI_FLAG_SECRET.replace_all(&result, "$1 <REDACTED>");
            result = Cow::Owned(redacted.into_owned());
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redact_aws_keys() {
        let input = "export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE";
        let expected = "export AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY>";
        assert_eq!(SecretRedactor::redact(input), expected);
    }

    #[test]
    fn test_redact_url_credentials() {
        let input = "git clone https://user:password123@github.com/repo.git";
        let expected = "git clone https://user:<REDACTED>@github.com/repo.git";
        assert_eq!(SecretRedactor::redact(input), expected);
    }

    #[test]
    fn test_redact_cli_flags() {
        let input = "./deploy --token abcdef123456 --verbose";
        let expected = "./deploy --token <REDACTED> --verbose";
        assert_eq!(SecretRedactor::redact(input), expected);
    }
}
