# Sinex Security Guide

## Overview

Sinex captures extensive personal data, making security a critical concern. This guide covers security best practices, threat mitigation, and operational security measures.

## Key Security Principles

1. **Local-First**: Data stays on your machine by default
2. **Defense in Depth**: Multiple layers of security
3. **Least Privilege**: Services run with minimal permissions
4. **Audit Trail**: All access and changes are logged

## Threat Model Summary

### Protected Assets
- Raw event data (`core.events`) containing user activity
- Personal Knowledge Management content
- Large file blobs (git-annex store)
- Configuration secrets (API keys, passwords)
- System integrity and availability

### Primary Threats
1. **Information Disclosure**: Unauthorized access to personal data
2. **Data Tampering**: Modification of historical events
3. **Service Disruption**: DoS attacks on local services
4. **Privilege Escalation**: Compromised services gaining system access

## Security Measures

### 1. Filesystem Security

**Full Disk Encryption (Required)**
```bash
# Verify LUKS encryption
cryptsetup status /dev/mapper/root
```

**Database Permissions**
```bash
# PostgreSQL data directory should be 0700
ls -la /var/lib/postgresql/
# Should show: drwx------ postgres postgres
```

**Git-annex Encryption**
```bash
# For remote storage, use encrypted special remotes
git annex initremote myremote type=s3 encryption=shared
```

### 2. Network Security

**Default: Local-Only Services**
```nix
# All services bind to localhost by default
services.sinex.monitoring.observabilityStack.listenAddress = "127.0.0.1";
```

**Firewall Configuration**
```nix
# If exposing services, use firewall rules
networking.firewall = {
  allowedTCPPorts = [ ];  # Empty by default
  interfaces.lo.allowedTCPPorts = [ 8080 3000 9090 ];  # Localhost only
};
```

### 3. Authentication & Authorization

**NATS Security**
```bash
# Ingestion is NATS-native; configure NATS credentials and local-only access
# Example (nats-server): use authorization blocks and bind to 127.0.0.1
```

**Database Access Control**
```sql
-- Sinex user has minimal required permissions
GRANT CONNECT ON DATABASE sinex TO sinex;
GRANT USAGE ON SCHEMA core, raw TO sinex;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA core TO sinex;
```

### 4. Secret Management

**Using agenix for Secrets**
```nix
age.secrets = {
  postgresPassword = {
    file = ./secrets/postgres-password.age;
    owner = "postgres";
    mode = "0400";
  };
};
```

**Runtime Secret Storage**
- Secrets decrypted to tmpfs (`/run/secrets/`)
- Never stored on disk in plaintext
- Strict file permissions (0400)

### 5. Input Validation

**Path Traversal Protection**
```rust
// All file paths are sanitized
SecurityValidator::sanitize_path(user_input)?;
```

**SQL Injection Prevention**
- Using parameterized queries via SQLX
- No dynamic SQL construction

**JSON Schema Validation**
- All event payloads validated against schemas
- Malformed events rejected at ingestion

### 6. Monitoring & Auditing

**Security Event Monitoring**
```sql
-- Monitor failed authentication attempts
SELECT * FROM core.events 
WHERE event_type = 'security.auth.failed'
AND ts_orig > NOW() - INTERVAL '1 hour';
```

**Resource Monitoring**
```bash
# Check for unusual resource usage
systemctl status sinex-* | grep -E "(Memory|CPU)"
```

## Operational Security

### Daily Security Checklist

1. **Check Service Health**
   ```bash
   systemctl status sinex-*
   journalctl -u sinex-* --since "1 hour ago" | grep -i error
   ```

2. **Monitor Access Logs**
   ```bash
   # Check for unauthorized access attempts
   journalctl | grep -E "(authentication|permission denied)"
   ```

3. **Verify Permissions**
   ```bash
   # Database files
   stat -c "%a %U %G" /var/lib/postgresql/*/main
   # Sinex state
   stat -c "%a %U %G" /var/lib/sinex
   ```

### Incident Response

**Data Breach Response**
1. Stop affected services: `systemctl stop sinex-*`
2. Preserve logs: `journalctl > incident.log`
3. Check access patterns in database
4. Rotate compromised credentials
5. Review and patch vulnerability

**Service Compromise**
1. Isolate service: `systemctl mask sinex-<service>`
2. Check for persistence: Review systemd units, cron jobs
3. Audit recent events from service
4. Rebuild from known-good state

### Security Hardening

**NixOS Security Options**
```nix
services.sinex = {
  security.level = "strict";  # Maximum hardening
  
  # Per-service hardening
  services.satellites.filesystem = {
    serviceConfig = {
      PrivateTmp = true;
      ProtectSystem = "strict";
      ProtectHome = true;
      NoNewPrivileges = true;
      RestrictSUIDSGID = true;
      RemoveIPC = true;
      RestrictNamespaces = true;
      RestrictRealtime = true;
      LockPersonality = true;
      ProtectKernelTunables = true;
      ProtectKernelModules = true;
      ProtectControlGroups = true;
      RestrictAddressFamilies = "AF_UNIX AF_INET AF_INET6";
      SystemCallFilter = "@system-service";
      SystemCallErrorNumber = "EPERM";
    };
  };
};
```

**Database Hardening**
```sql
-- Limit connections
ALTER DATABASE sinex CONNECTION LIMIT 50;

-- Enable SSL for remote connections (if needed)
-- In postgresql.conf:
-- ssl = on
-- ssl_cert_file = 'server.crt'
-- ssl_key_file = 'server.key'
```

## Privacy Considerations

### Data Minimization
- Configure satellites to exclude sensitive paths
- Use event filters to prevent capturing passwords
- Regular data expiration policies

### Sensitive Content Detection
```nix
services.sinex.satellite.eventSources.clipboard = {
  sensitiveContentDetection = true;  # Redact passwords, keys
};
```

### Data Export Controls
- Audit all data exports
- Sanitize exports before sharing
- Use encryption for backups

## Security Updates

**Regular Updates**
```bash
# Update NixOS and all packages
sudo nix-channel --update
sudo nixos-rebuild switch --upgrade

# Check for security advisories
nix-shell -p vulnix --run "vulnix /run/current-system"
```

**Monitoring Security Events**
```sql
-- Create security dashboard view
CREATE VIEW security_events AS
SELECT ts_orig, source, event_type, payload
FROM core.events
WHERE event_type LIKE 'security.%'
   OR event_type LIKE '%.error'
   OR event_type LIKE '%.failed'
ORDER BY ts_orig DESC;
```

## Compliance & Regulations

### GDPR Considerations (if applicable)
- Right to erasure: Implement event deletion
- Data portability: Export tools available
- Access controls: Audit trail maintained

### Local Regulations
- Ensure compliance with local privacy laws
- Consider data retention requirements
- Implement appropriate access controls

## Additional Resources

- [OWASP Security Guidelines](https://owasp.org/)
- [NixOS Security](https://nixos.wiki/wiki/Security)
- [PostgreSQL Security](https://www.postgresql.org/docs/current/security.html)
- [Git-annex Encryption](https://git-annex.branchable.com/encryption/)
