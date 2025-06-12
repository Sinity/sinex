# Security Training: Understanding and Preventing Attacks in Sinex

## Module 1: Understanding the Threats

### 1.1 Path Injection Attacks

**What is it?**
Attackers manipulate file paths to access files outside intended directories.

**Real Example:**
```
User provides: "../../../etc/passwd"
System accesses: /etc/passwd (sensitive system file)
```

**How Sinex Protects:**
```rust
// ❌ Vulnerable code
let path = format!("/data/{}", user_input);
std::fs::read(path)?; // Could read any file!

// ✅ Secure code
let validator = PathValidator::default();
let safe_path = validator.validate(user_input)?;
let full_path = format!("/data/{}", safe_path.display());
std::fs::read(full_path)?; // Only validated paths
```

**Exercise 1:** Try these inputs with PathValidator:
- `"normal_file.txt"` → Should pass
- `"../etc/passwd"` → Should fail
- `"file\0.txt"` → Should fail
- `"/etc/passwd"` → Should fail (absolute path)

### 1.2 Null Byte Injection

**What is it?**
Null bytes (`\0`) can truncate strings in C-based systems, bypassing security checks.

**Attack Scenario:**
```
Input: "config.txt\0.jpg"
Filter sees: "config.txt.jpg" (looks like image)
System reads: "config.txt" (stops at \0)
```

**Detection Code:**
```rust
if path.contains('\0') {
    // ATTACK DETECTED!
    log_security_event(SecurityEvent::NullByteRejected { path });
    return Err("Null bytes not allowed");
}
```

### 1.3 Unicode Attacks

**What is it?**
Different Unicode representations bypass security filters.

**Examples:**
```
"admin" vs "аdmin" (Cyrillic 'а')
"file.txt" vs "ﬁle.txt" (ligature 'ﬁ')
"important.doc" vs "important.doc​" (zero-width space)
```

**How to Detect:**
```rust
// Normalize first
let normalized = UnicodeNormalizer::default().normalize(input)?;

// Check for suspicious characters
for ch in normalized.chars() {
    if is_zero_width(ch) || is_rtl_override(ch) {
        return Err("Suspicious Unicode detected");
    }
}
```

## Module 2: JSON Security

### 2.1 Billion Laughs Attack

**What is it?**
Nested structures that expand exponentially when parsed.

**Attack Pattern:**
```json
{
  "lol1": "lol",
  "lol2": ["lol1", "lol1", "lol1", "lol1", "lol1", "lol1", "lol1", "lol1", "lol1", "lol1"],
  "lol3": ["lol2", "lol2", "lol2", "lol2", "lol2", "lol2", "lol2", "lol2", "lol2", "lol2"],
  "lol4": ["lol3", "lol3", "lol3", "lol3", "lol3", "lol3", "lol3", "lol3", "lol3", "lol3"]
}
```

**Impact:**
- Level 1: 10 bytes
- Level 2: 100 bytes
- Level 3: 1,000 bytes
- Level 8: 100,000,000 bytes (100MB from ~1KB input!)

**Protection:**
```rust
let limits = JsonLimits {
    max_depth: 32,     // Prevent deep nesting
    max_size: 10 * 1024 * 1024,  // 10MB max
    ..Default::default()
};
```

### 2.2 Hash Collision DoS

**What is it?**
Crafted keys that hash to the same value, degrading HashMap performance.

**Attack Keys:**
```
"Aa", "BB" → Same hash
"AaAa", "AaBB", "BBAa", "BBBB" → All collide
```

**Solution: SipHash**
```rust
// Uses cryptographic hash with random key
let secure_json = parse_secure_json(input)?;
// HashMap now resistant to collision attacks
```

### 2.3 Circular References

**What is it?**
JSON documents that reference themselves, causing infinite loops.

**Example:**
```json
{
  "user": {
    "manager": {"$ref": "#/user"}
  }
}
```

**Detection:**
```rust
let mut resolver = JsonRefResolver::new();
resolver.validate(&json)?; // Detects cycles
```

## Module 3: Command Injection

### 3.1 Understanding the Risk

**Vulnerable Pattern:**
```rust
// ❌ NEVER DO THIS
let cmd = format!("process_file {}", user_input);
std::process::Command::new("sh").arg("-c").arg(cmd).output()?;

// If user_input = "file.txt; rm -rf /"
// Executes: process_file file.txt; rm -rf /
```

### 3.2 Safe Command Execution

**Secure Pattern:**
```rust
// ✅ ALWAYS DO THIS
let mut cmd = SafeCommand::new("process_file");
cmd.arg(user_input); // Passed as separate argument
cmd.execute()?;

// Shell metacharacters have no effect
```

**Dangerous Characters to Block:**
```
; | & $ ` ( ) { } < > \ \n \r \0 * ? [ ] ! ~ " '
```

## Module 4: Practical Exercises

### Exercise 1: Spot the Vulnerability

```rust
// Code snippet 1
fn read_user_file(username: &str, filename: &str) -> Result<String> {
    let path = format!("/home/{}/documents/{}", username, filename);
    std::fs::read_to_string(path)
}
```

**Question:** What's wrong? How would you fix it?

<details>
<summary>Answer</summary>

Vulnerable to path traversal! User could provide:
- username: "alice"
- filename: "../../bob/secrets.txt"

Fix:
```rust
fn read_user_file(username: &str, filename: &str) -> Result<String> {
    let validator = PathValidator::default();
    let safe_username = validator.validate(username)?;
    let safe_filename = validator.validate(filename)?;
    
    // Additional check: no path separators in filename
    if filename.contains('/') || filename.contains('\\') {
        return Err("Invalid filename");
    }
    
    let path = format!("/home/{}/documents/{}", 
                      safe_username.display(), 
                      safe_filename.display());
    std::fs::read_to_string(path)
}
```
</details>

### Exercise 2: Design a Secure API

Design an API endpoint that accepts JSON and stores it. Consider:
1. Size limits
2. Validation
3. Security monitoring
4. Error handling

<details>
<summary>Solution</summary>

```rust
async fn store_json(body: String) -> Result<Response> {
    // 1. Size check (early rejection)
    if body.len() > 1024 * 1024 { // 1MB
        log_security_event(SecurityEvent::JsonTooLarge { 
            size: body.len() 
        });
        return Err("Payload too large");
    }
    
    // 2. Validate JSON
    let mut validator = Validator::default();
    let validated = validator.validate_json(&body)?;
    
    // 3. Additional business logic validation
    if !validated.get("type").is_some() {
        return Err("Missing required field: type");
    }
    
    // 4. Store safely
    store_to_database(validated).await?;
    
    // 5. Return success
    Ok(Response::success())
}
```
</details>

### Exercise 3: Incident Response

You see these alerts:
```
[ERROR] SECURITY: Null byte injection attempt blocked - path: /etc/passwd\0.txt
[ERROR] SECURITY: Null byte injection attempt blocked - path: /etc/shadow\0.txt
[ERROR] SECURITY: Path traversal attempt blocked - path: ../../../etc/hosts
```

What do you do?

<details>
<summary>Response Plan</summary>

1. **Immediate Actions:**
   - Check source IP/user if available
   - Block source if pattern continues
   - Review recent activity from same source

2. **Investigation:**
   ```rust
   // Export recent events
   let events = DASHBOARD.get_recent_events(1000);
   let attack_pattern = events.iter()
       .filter(|e| e.severity == Severity::Critical)
       .collect::<Vec<_>>();
   ```

3. **Analysis:**
   - Attacker is trying to read system files
   - Testing multiple techniques (null byte, traversal)
   - Likely automated scanner

4. **Response:**
   - Temporarily increase monitoring sensitivity
   - Add source to watchlist
   - Review if any attempts succeeded
   - Check for similar patterns in logs

5. **Follow-up:**
   - Document attack pattern
   - Update security tests to include these attempts
   - Consider rate limiting per source
</details>

## Module 5: Security Checklist

### For Every Input:
- [ ] Is it validated?
- [ ] Is it normalized (Unicode)?
- [ ] Is it logged if rejected?
- [ ] Is the error message safe (no information leakage)?

### For File Operations:
- [ ] Path validated for null bytes?
- [ ] Path traversal prevented?
- [ ] Symbolic links handled safely?
- [ ] Permissions checked?

### For JSON Processing:
- [ ] Size limits enforced?
- [ ] Depth limits enforced?
- [ ] Using secure parsing (SipHash)?
- [ ] Circular references detected?

### For Command Execution:
- [ ] Using SafeCommand wrapper?
- [ ] Arguments passed separately?
- [ ] Environment variables controlled?
- [ ] No shell interpolation?

### For Security Events:
- [ ] Logged with sufficient detail?
- [ ] Monitored in dashboard?
- [ ] Alerts configured?
- [ ] Metrics tracked?

## Module 6: Tools and Commands

### Testing Security Locally

```bash
# Run security tests
cargo test --package sinex-validation

# Test specific validator
cargo test --package sinex-validation path_validator

# Run adversarial tests
cargo test --test adversarial
```

### Monitoring Security

```rust
// Check current stats
let stats = DASHBOARD.get_stats(Duration::from_secs(3600));
println!("Critical events: {}", stats.critical_events);

// Export for analysis
let events_json = DASHBOARD.export_events(ExportFormat::Json)?;
std::fs::write("security_events.json", events_json)?;
```

### Common Patterns

```rust
// Pattern 1: Validate early
pub fn process_request(input: UserInput) -> Result<()> {
    let validated = validate_all_fields(input)?;
    // Now safe to process
}

// Pattern 2: Fail securely
match validator.validate(input) {
    Ok(safe_input) => process(safe_input),
    Err(e) => {
        log_security_event(e);
        Err("Invalid input") // Generic error to user
    }
}

// Pattern 3: Defense in depth
let input = normalize_unicode(raw_input)?;
let validated = validate_format(input)?;
let sanitized = remove_dangerous_chars(validated)?;
let final_check = verify_business_rules(sanitized)?;
```

## Quiz

1. **What's wrong with this code?**
   ```rust
   let file = format!("{}.txt", user_input);
   std::fs::read(file)?;
   ```

2. **How many security issues can you spot?**
   ```rust
   fn process_json(json: &str) {
       let parsed: Value = serde_json::from_str(json).unwrap();
       let cmd = format!("echo '{}'", parsed["message"]);
       std::process::Command::new("sh").arg("-c").arg(cmd).output();
   }
   ```

3. **Which Unicode character is most dangerous?**
   - a) É (accented E)
   - b) 🔒 (lock emoji)
   - c) ‌ (zero-width non-joiner)
   - d) Ω (Greek omega)

<details>
<summary>Answers</summary>

1. No path validation - vulnerable to:
   - Null bytes: `user_input = "/etc/passwd\0"`
   - Path traversal: `user_input = "../../../etc/passwd"`
   - Absolute paths: `user_input = "/etc/passwd"`

2. Multiple issues:
   - No JSON size limits (DoS)
   - Command injection via `parsed["message"]`
   - Using shell execution
   - No error handling (panic on invalid JSON)
   - No validation of message field existence

3. c) Zero-width non-joiner - invisible character that can bypass filters

</details>

## Resources

- [OWASP Top 10](https://owasp.org/Top10/)
- [CWE Database](https://cwe.mitre.org/)
- [Rust Security Guidelines](https://anssi-fr.github.io/rust-guide/)
- Run `cargo test --test adversarial` to see attacks in action

Remember: **Security is everyone's responsibility!**