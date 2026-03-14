# Database Encryption with pgsodium

> Status: exploratory only, explicitly not a current system requirement.

## Overview (Tentative)

pgsodium is a PostgreSQL extension that provides encryption functions using the libsodium crypto library. It can be used for targeted field-level encryption of especially sensitive data (e.g., auth tokens or redacted PII fields) but is **not** currently planned as blanket encryption for all event payloads. This remains an optional, exploratory feature; user-configurable redaction at capture time is the primary privacy control.

## What is pgsodium?

pgsodium provides:
- **Transparent encryption**: Encrypt/decrypt data within SQL queries
- **Key management**: Secure master key handling via external scripts
- **Multiple algorithms**: AEAD encryption, hashing, key derivation
- **Performance**: Hardware-accelerated crypto operations
- **Compliance**: Helps meet data protection requirements

## Use Cases in Sinex

### Sensitive Event Payloads
```sql
-- Encrypt sensitive fields in event payloads
UPDATE core.events 
SET payload = jsonb_set(
    payload,
    '{password}',
    to_jsonb(pgsodium.crypto_aead_det_encrypt(
        payload->>'password'::text,
        'event_encryption'::text,
        id::uuid
    ))
)
WHERE event_type = 'auth.login_attempt';
```

### Personal Information
- Email addresses in communication events
- API keys in configuration events
- File paths containing usernames
- Browser history URLs
- Clipboard content

### Knowledge Management
- Private notes or journal entries
- Encrypted document content
- Sensitive entity relationships

## Implementation Architecture

### 1. Master Key Management
```nix
# Via agenix in main NixOS config
age.secrets.pgsodium_master_key = {
  file = ./secrets/pgsodium_master_key.age;
  owner = "postgres";
  mode = "0400";
};

# PostgreSQL configuration
services.postgresql.settings."pgsodium.getkey_script" = 
  pkgs.writeShellScript "pgsodium-getkey" ''
    cat ${config.age.secrets.pgsodium_master_key.path}
  '';
```

### 2. Database Schema
```sql
-- Enable extension
CREATE EXTENSION IF NOT EXISTS pgsodium;

-- Create encryption keys table
CREATE TABLE IF NOT EXISTS pgsodium.key (
    id BIGSERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    key_id UUID DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Example: Encrypted columns
ALTER TABLE core.artifacts 
ADD COLUMN content_encrypted BYTEA;

-- Encrypt on insert
INSERT INTO core.artifacts (content_encrypted)
VALUES (pgsodium.crypto_aead_det_encrypt(
    'sensitive content'::bytea,
    'artifacts'::bytea,
    artifact_id::uuid
));
```

### 3. Application Integration
```rust
// Transparent decryption in queries
let query = QueryBuilder::new()
    .select("pgsodium.crypto_aead_det_decrypt(
        content_encrypted,
        'artifacts'::bytea,
        artifact_id::uuid
    ) as content")
    .from("core.artifacts")
    .where_eq("artifact_id", artifact_id);
```

## Security Benefits

### Defense in Depth
- **Layer 1**: LUKS full-disk encryption
- **Layer 2**: PostgreSQL file permissions
- **Layer 3**: pgsodium field-level encryption
- **Layer 4**: Access control via database roles

### Key Rotation
```sql
-- Rotate encryption for specific data
UPDATE sensitive_table
SET encrypted_field = pgsodium.crypto_aead_det_encrypt(
    pgsodium.crypto_aead_det_decrypt(
        encrypted_field,
        old_key,
        nonce
    ),
    new_key,
    nonce
);
```

## Implementation Roadmap

### Phase 1: Foundation
- [ ] Add pgsodium to PostgreSQL extensions
- [ ] Configure master key via agenix
- [ ] Create key management schema
- [ ] Document encryption policies

### Phase 2: Core Integration
- [ ] Encrypt sensitive event payload fields
- [ ] Add encryption to artifact storage
- [ ] Implement key rotation procedures
- [ ] Create audit trail for encryption operations

### Phase 3: Advanced Features
- [ ] Row-level encryption policies
- [ ] Searchable encryption for certain fields
- [ ] Performance optimization
- [ ] Backup/restore procedures

## Performance Considerations

- **Overhead**: ~10-20% for encrypted operations
- **Indexing**: Cannot index encrypted fields directly
- **Memory**: Additional buffers for crypto operations
- **CPU**: Hardware AES-NI acceleration recommended

## Alternative Approaches

### Application-Level Encryption
- More control but more complexity
- Requires key management in application
- Cannot use database features on encrypted data

### Transparent Data Encryption (TDE)
- Encrypts entire database files
- Less granular than pgsodium
- Not available in standard PostgreSQL

## Current Status

**Not implemented, and not on the active hardening roadmap.** This document is retained as an
exploratory design note only. Sinex currently treats capture-time privacy controls plus host
full-disk encryption as the intended baseline, and does not treat pgsodium rollout as a standing
security deficiency.

## References

- [pgsodium Documentation](https://github.com/michelp/pgsodium)
- TIM-PostgreSQLSecurityEncryption (planned)
- ADR-006: Secrets Management (for master key handling)
