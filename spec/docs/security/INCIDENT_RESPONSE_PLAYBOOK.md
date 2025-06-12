# Sinex Security Incident Response Playbook

## 🚨 Quick Reference

**Security Hotline:** [Your security team contact]  
**Escalation:** [Escalation path]  
**Dashboard:** Access via `DASHBOARD.get_recent_events(100)`

---

## Incident Classification

### Severity Levels

| Level | Description | Response Time | Examples |
|-------|-------------|---------------|----------|
| **P0 - Critical** | Active exploitation, data breach risk | < 15 minutes | Command injection attempts, mass null byte attacks |
| **P1 - High** | Potential exploitation, service impact | < 1 hour | Hash collision DoS, circular reference attacks |
| **P2 - Medium** | Suspicious activity, limited impact | < 4 hours | Unicode bypass attempts, oversized JSON |
| **P3 - Low** | Anomalous behavior, no immediate risk | < 24 hours | Single validation failures, config probes |

---

## Response Procedures

### 🔴 P0 - Critical Incident Response

#### 1. IMMEDIATE ACTIONS (0-15 minutes)

```bash
# 1. Capture current state
cargo run --bin security-snapshot

# 2. Check active attacks
echo "SELECT * FROM security_events WHERE severity='CRITICAL' AND timestamp > NOW() - INTERVAL '1 hour';" | psql sinex_db

# 3. Export recent events
curl -X POST http://localhost:8080/api/security/export \
  -H "Content-Type: application/json" \
  -d '{"format": "json", "minutes": 60}'
```

#### 2. CONTAIN THE THREAT (15-30 minutes)

**A. Block Attack Source**
```rust
// Add to blocklist
let mut blocklist = BLOCKLIST.write().unwrap();
blocklist.insert(source_ip);

// Update firewall rules
update_firewall_rules(&blocklist);
```

**B. Increase Security Posture**
```rust
// Tighten validation limits
let strict_config = ValidatorConfig {
    json_limits: JsonLimits {
        max_size: 1024 * 1024,    // Reduce to 1MB
        max_depth: 10,            // Reduce depth
        max_keys_per_object: 100, // Fewer keys
        ..Default::default()
    },
    ..Default::default()
};

// Apply globally
set_global_validator_config(strict_config);
```

#### 3. INVESTIGATE (30-60 minutes)

**Attack Pattern Analysis:**
```sql
-- Find attack patterns
SELECT 
    event_type,
    COUNT(*) as attempts,
    MIN(timestamp) as first_seen,
    MAX(timestamp) as last_seen,
    COUNT(DISTINCT source_ip) as unique_sources
FROM security_events
WHERE timestamp > NOW() - INTERVAL '24 hours'
GROUP BY event_type
ORDER BY attempts DESC;

-- Identify targeted resources
SELECT 
    jsonb_extract_path_text(details, 'path') as target_path,
    COUNT(*) as attempts
FROM security_events
WHERE event_type IN ('null_byte_injection', 'path_traversal')
GROUP BY target_path
ORDER BY attempts DESC;
```

**Check for Successful Exploits:**
```bash
# Review application logs for anomalies
grep -E "(ERROR|WARN)" /var/log/sinex/*.log | \
  grep -v "validation failed" | \
  tail -1000

# Check for unusual file access
find /data -type f -mtime -1 -ls

# Verify system integrity
sha256sum -c /etc/sinex/checksums.txt
```

#### 4. ERADICATE (1-2 hours)

**Clean Compromised Data:**
```sql
-- Remove any malicious payloads
DELETE FROM raw.events 
WHERE event_payload::text LIKE '%\x00%'
   OR event_payload::text LIKE '%../..%';

-- Audit remaining data
SELECT COUNT(*), event_type 
FROM raw.events 
WHERE created_at > NOW() - INTERVAL '24 hours'
GROUP BY event_type;
```

#### 5. RECOVER (2-4 hours)

**Restore Normal Operations:**
```rust
// Gradually relax security limits
let normal_config = ValidatorConfig::default();
set_global_validator_config(normal_config);

// Clear IP blocklist (selective)
let mut blocklist = BLOCKLIST.write().unwrap();
blocklist.retain(|ip| is_still_threatening(ip));

// Resume normal processing
resume_event_processing();
```

### 🟠 P1 - High Severity Response

#### 1. ASSESS (0-60 minutes)

```rust
// Get detailed statistics
let stats = DASHBOARD.get_stats(Duration::from_secs(3600));
println!("Attack Summary: {:?}", stats);

// Identify attack vector
let events = DASHBOARD.get_events_by_severity(Severity::High);
let attack_types: HashSet<_> = events.iter()
    .map(|e| &e.event_type)
    .collect();
```

#### 2. MITIGATE

**For Hash Collision DoS:**
```rust
// Switch to more aggressive limits
update_json_limits(JsonLimits {
    max_keys_per_object: 100, // Reduce from 1000
    ..Default::default()
});

// Monitor HashMap performance
enable_hashmap_metrics();
```

**For Circular References:**
```rust
// Enable deep inspection
enable_deep_json_inspection();

// Log all $ref usage
log_json_references();
```

#### 3. MONITOR

Set up enhanced monitoring:
```rust
// Alert on threshold
spawn_monitoring_task(|stats| {
    if stats.high_events > 10 {
        send_alert("High severity events spike detected");
    }
});
```

### 🟡 P2 - Medium Severity Response

#### 1. INVESTIGATE

```bash
# Review patterns
cargo run --bin analyze-security-patterns -- --days 7

# Check for reconnaissance
grep -E "(scan|probe|test)" /var/log/sinex/security.log
```

#### 2. ADJUST

Update security rules if needed:
```rust
// Add new patterns to detection
add_suspicious_pattern(regex!("admin.*test"));

// Update Unicode blocklist
add_blocked_unicode_range(0x200B..=0x200F);
```

### 🟢 P3 - Low Severity Response

Document and monitor:
```rust
// Log for trend analysis
log::info!("Low severity event: {:?}", event);

// Update metrics
METRICS.low_severity_events.increment();
```

---

## Playbook Scenarios

### Scenario 1: Null Byte Attack Campaign

**Indicators:**
- Multiple null byte injection attempts
- Targeting system files (/etc/passwd, /etc/shadow)
- Rapid succession from single source

**Response:**
```bash
# 1. Block source immediately
iptables -A INPUT -s $ATTACKER_IP -j DROP

# 2. Audit file access logs
ausearch -f /etc/passwd -ts recent

# 3. Check for successful reads
SELECT * FROM raw.events 
WHERE event_type = 'file_read' 
  AND event_payload->>'path' LIKE '/etc/%'
  AND created_at > NOW() - INTERVAL '1 hour';
```

### Scenario 2: JSON Billion Laughs Attack

**Indicators:**
- JSON parsing taking >1 second
- Memory usage spikes
- Worker thread timeouts

**Response:**
```rust
// 1. Emergency limits
set_emergency_json_limits(JsonLimits {
    max_size: 100_000,  // 100KB only
    max_depth: 5,       // Very shallow
    ..Default::default()
});

// 2. Identify attack payloads
let attacks = find_slow_json_events();

// 3. Block sources
for event in attacks {
    block_source(event.source_ip);
}
```

### Scenario 3: Distributed Unicode Attack

**Indicators:**
- Unicode normalization errors from multiple IPs
- Homoglyph attempts on usernames
- Mixed script detections

**Response:**
```rust
// 1. Enable strict mode
enable_strict_unicode_mode();

// 2. Collect samples
let samples = collect_unicode_attack_samples();
export_for_analysis(samples);

// 3. Update detection rules
for pattern in analyze_patterns(samples) {
    add_unicode_detection_rule(pattern);
}
```

---

## Post-Incident Actions

### 1. Incident Report Template

```markdown
## Incident Report: [INCIDENT-ID]

**Date:** [DATE]
**Severity:** P[0-3]
**Duration:** [START] - [END]
**Impact:** [DESCRIPTION]

### Timeline
- T+0: Initial detection
- T+X: Containment started
- T+Y: Threat eradicated
- T+Z: Normal operations restored

### Root Cause
[DESCRIPTION]

### Actions Taken
1. [ACTION 1]
2. [ACTION 2]

### Lessons Learned
- [LESSON 1]
- [LESSON 2]

### Follow-up Items
- [ ] Update security rules
- [ ] Add test cases
- [ ] Documentation updates
```

### 2. Update Security Tests

```rust
#[test]
fn test_incident_[INCIDENT_ID]_prevention() {
    // Add test that would have caught this attack
    let payload = "[ATTACK_PAYLOAD]";
    let result = validator.validate(payload);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), 
                    ValidationError::[EXPECTED_ERROR]));
}
```

### 3. Update Monitoring

```rust
// Add specific detection for this attack pattern
impl SecurityMonitor {
    fn detect_incident_[INCIDENT_ID]_pattern(&self, event: &Event) -> bool {
        // Pattern matching logic
    }
}
```

---

## Tools and Commands

### Emergency Commands

```bash
# Stop all processing
systemctl stop sinex-collector

# Enable emergency mode
echo "EMERGENCY_MODE=true" >> /etc/sinex/config

# Dump security state
pg_dump -t security_events sinex_db > security_backup_$(date +%s).sql

# Clear event queue
redis-cli FLUSHDB

# Restart with strict mode
STRICT_MODE=true systemctl start sinex-collector
```

### Analysis Scripts

```bash
# Top attack sources
cat security_events.json | \
  jq -r '.[] | select(.severity=="CRITICAL") | .source_ip' | \
  sort | uniq -c | sort -rn | head -20

# Attack timeline
cat security_events.json | \
  jq -r '.[] | [.timestamp, .event_type] | @csv' | \
  sort | awk -F, '{print strftime("%Y-%m-%d %H:%M:%S", $1), $2}'

# Resource impact
ps aux | grep sinex | awk '{sum+=$6} END {print "Memory:", sum/1024, "MB"}'
```

### Recovery Verification

```rust
// Verify security posture
async fn verify_security_posture() -> Result<()> {
    // Check validators
    let validator = Validator::default();
    assert!(validator.validate_path("/etc/passwd\0").is_err());
    
    // Check monitoring
    let stats = DASHBOARD.get_stats(Duration::from_secs(300));
    assert_eq!(stats.critical_events, 0);
    
    // Check performance
    let start = Instant::now();
    parse_secure_json(r#"{"test": "data"}"#)?;
    assert!(start.elapsed() < Duration::from_millis(10));
    
    Ok(())
}
```

---

## Contact Information

**Security Team:** security@sinex.example.com  
**On-Call Engineer:** +1-XXX-XXX-XXXX  
**Escalation Manager:** escalation@sinex.example.com  

**External Resources:**
- CERT/CC: cert@cert.org
- IC3: www.ic3.gov

---

## Appendix: Quick Reference Card

```
┌─────────────────────────────────────┐
│        SECURITY QUICK REFERENCE      │
├─────────────────────────────────────┤
│ View Events:                        │
│   DASHBOARD.get_recent_events(100)  │
│                                     │
│ Export Events:                      │
│   DASHBOARD.export_events(Json)     │
│                                     │
│ Check Stats:                        │
│   DASHBOARD.get_stats(Duration)     │
│                                     │
│ Emergency Validator:                │
│   ValidatorConfig {                 │
│     validate_paths: true,           │
│     validate_json: true,            │
│     normalize_unicode: true,        │
│     use_secure_json: true,          │
│     detect_circular_refs: true,     │
│   }                                 │
└─────────────────────────────────────┘
```

Remember: **When in doubt, escalate!**