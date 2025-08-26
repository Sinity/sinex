# Security Architecture

## Overview

Sinex implements defense-in-depth security with multiple layers of protection for data at rest, in transit, and during processing. While some security features are planned but not implemented, the architecture provides a foundation for comprehensive data protection.

## Current Security Implementation

### Process Isolation
✅ **Implemented via systemd hardening**:
- `NoNewPrivileges=true` prevents privilege escalation
- `ProtectSystem=strict` makes system directories read-only
- `SystemCallFilter` restricts available system calls
- `PrivateTmp=true` isolates temporary directories
- Resource limits via `MemoryMax` and `CPUQuota`

### Access Control
✅ **Unix permissions and PostgreSQL roles**:
- Satellite services run as dedicated `sinex` user
- Database access via local peer authentication
- Unix socket permissions for IPC security
- Separate PostgreSQL roles for different access levels

### Input Validation
✅ **Multi-layer validation**:
- JSON Schema validation for event payloads
- ULID format validation
- SQL injection prevention via QueryBuilder
- Type-safe database queries

## Planned Security Features

### Database Encryption (pgsodium)
❌ **Not Implemented** - Critical gap in security model

pgsodium would provide field-level encryption for:
- Sensitive event payloads (passwords, API keys)
- Personal information (emails, file paths)
- Knowledge management content
- Configuration secrets

See [Database Encryption Roadmap](/docs/roadmap/features/database-encryption-pgsodium.md) for implementation details.

### Secrets Management (agenix)
⚠️ **Partial Implementation**
- ✅ Implemented in user's main NixOS configuration
- ✅ API keys managed via environment variables
- ❌ Not integrated into Sinex project directly
- ❌ No pgsodium master key management

Current approach may be sufficient as:
- Database uses peer authentication (no passwords)
- Services run under system users
- API keys injected from system configuration

### Network Security
❌ **Not Implemented**:
- No TLS for gRPC communications
- No authentication framework
- No rate limiting
- Gateway exposed without access control

## Security Model

### Trust Boundaries
1. **User ↔ System**: Full trust (single-user system)
2. **Satellites ↔ ingestd**: Unix socket permissions
3. **ingestd ↔ Database**: PostgreSQL role separation
4. **Automata ↔ NATS JetStream**: Durable consumer isolation
5. **External APIs**: API keys from environment

### Data Classification
1. **Public**: System metrics, non-sensitive events
2. **Private**: File paths, window titles, commands
3. **Sensitive**: Passwords, API keys, personal notes
4. **Critical**: Encryption keys, auth tokens

### Threat Model Highlights

**In Scope**:
- Accidental data exposure via logs/exports
- Unauthorized access to database
- Memory disclosure vulnerabilities
- Malicious event injection
- Resource exhaustion attacks

**Out of Scope** (Single-user assumption):
- Multi-user access control
- Network-based attacks (local-only)
- Physical access attacks
- Supply chain attacks

## Implementation Priorities

### 🚨 Critical (Do First)
1. **Enable pgsodium encryption**
   - Protects data at rest
   - Required for compliance
   - Foundation for other security

2. **Implement audit logging**
   - Track all data access
   - Monitor security events
   - Enable forensics

### ⚠️ Important (Do Soon)
3. **Add authentication to gateway**
   - Before any network exposure
   - Token-based or mTLS
   - Rate limiting

4. **Enhanced input sanitization**
   - Redact passwords in events
   - Filter environment variables
   - Scrub command arguments

### 📋 Nice to Have
5. **Implement TLS for IPC**
   - Between satellites and ingestd
   - For NATS connections
   - For PostgreSQL if remote

6. **Security scanning**
   - Dependency audits
   - SAST/DAST integration
   - Penetration testing

## Security Checklist

### Pre-Deployment
- [ ] Enable LUKS full-disk encryption
- [ ] Configure pgsodium with secure key
- [ ] Set up agenix secret management
- [ ] Enable PostgreSQL SSL
- [ ] Configure firewall rules
- [ ] Disable unnecessary services
- [ ] Set up audit logging
- [ ] Create security backups

### Operational Security
- [ ] Regular security updates
- [ ] Monitor audit logs
- [ ] Rotate secrets periodically
- [ ] Review access logs
- [ ] Test backup restoration
- [ ] Update threat model
- [ ] Security training

## Threat Model Summary

### Information Disclosure Threats
1. **Unauthorized filesystem access** → Mitigated by LUKS FDE + permissions
2. **Secrets exposure in `/run`** → Mitigated by tmpfs + strict permissions
3. **Network service exposure** → Mitigate with localhost binding + auth
4. **Keylogging via evdev** → Requires opt-in + privilege separation
5. **Over-privileged SQL access** → Need field encryption + granular roles
6. **LLM data oversharing** → Requires policies + local LLM preference

### Tampering Threats
1. **Database corruption** → Append-only events + backups + checksums
2. **Git-annex tampering** → Content-addressed storage detects changes
3. **Binary tampering** → NixOS immutability + version control

### Denial of Service Threats
1. **Resource exhaustion** → Systemd quotas + monitoring + retention
2. **Database overload** → Connection pooling + optimization
3. **Ingestor flooding** → Rate limiting + backpressure
4. **LLM cost runaway** → Budgeting + throttling + fallbacks

### Privilege Escalation Threats
1. **Agent vulnerabilities** → Sandboxing + least privilege + updates
2. **SQL injection** → Parameterized queries only
3. **Path traversal** → Input validation + canonicalization

See [TIM-SecurityThreatModel](../docs/_todo/archive/TIM-SecurityThreatModel.md) for comprehensive threat analysis.

## References

- [ADR-006: NixOS Secrets Management Tool](../docs/_todo/archive/ADR-006-NixOSSecretsManagementTool.md)
- [Database Encryption with pgsodium](../docs/roadmap/features/database-encryption-pgsodium.md)
- [TIM-SecurityThreatModel](../docs/_todo/archive/TIM-SecurityThreatModel.md)
- Original Vision Document security requirements
