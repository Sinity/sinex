# Encryption at Rest

Status: deployment security doctrine.

This is a deliberately small doctrine. For a single-user self-hosted system,
the realistic threat model (`nixos/modules/security-threat-model.md` T1, T3) collapses
"encryption at rest" to two load-bearing controls plus a handful of operator
checks. Everything else is depth-in-defense that does not change the answer
to "what happens if a laptop is stolen?"

## Doctrine

1. **LUKS is the primary control.** Full-disk encryption on every partition
   that holds Sinex state. Without it, no application-layer mechanism is
   sufficient. With it, the most realistic attacks (physical theft, lost
   device) are already mitigated against a non-state actor.
2. **Application-level encryption is depth, not foundation.**
   `Strategy::Encrypt` in the PrivacyEngine wraps designated payload fields
   in an `⌜enc:v1:…⌝` token at admission time. This defends against T5
   (application-level DB access) and T3 (any future remote sync), not
   against T1. If LUKS is compromised, the agenix-managed key sitting at
   `/run/agenix/sinex-privacy-key` is gone too.
3. **Encrypted backups are mandatory for offsite.** Any backup leaving the
   host must be wrapped with `age` (preferred — already in the Nix
   ecosystem) before upload. Local backups inherit LUKS.
4. **Future remote replication requires a concrete encryption design.**
   The retired git-annex initremote pattern does not apply. Before any
   CAS-remote or replica is added, an issue must specify key management,
   restore testing, and operator audit.

## Storage Inventory and Control Mapping

| Tier | Storage | Primary control | Application layer | Notes |
|---|---|---|---|---|
| PostgreSQL data dir | Local disk | LUKS | `Strategy::Encrypt` tokens for HIGH/CRITICAL fields | data dir 0700 owned by `postgres` (T2 mitigation) |
| Local BLAKE3 CAS | Local disk | LUKS | None by default | CAS root 0700 owned by sinex service user |
| Agenix run dir | `/run/agenix/` (tmpfs) | LUKS + tmpfs lifetime | n/a | 0400 on key files; owned by sinex service user |
| Live backups (local) | Local disk | LUKS | n/a | Stored on LUKS-backed partition |
| Offsite backups | Remote | Mandatory `age` envelope | n/a | Unencrypted upload is a deployment error |
| Future CAS remote | Remote | TBD per future issue | TBD | Open: no design committed |

## Application-Level Field Encryption

`Strategy::Encrypt` is the only application-layer encryption currently in
scope. Its responsibilities:

- Run at admission time inside the PrivacyEngine.
- Match payload fields by rule (financial amount/account, health
  measurement values, designated freeform fields).
- Replace the matched substring with `⌜enc:v1:<base64>⌝`.
- Key consumed from `/run/agenix/sinex-privacy-key` (agenix-managed).
- Decryption happens application-side, never inside Postgres.

The encryption strategy is intentionally narrow. Whole-payload encryption
breaks the storage model (no jsonb query, no FTS, no derived projections).
Field-level tokens preserve queryability for non-sensitive metadata while
removing plaintext for the operator-designated fields.

## Key Management

| Key | Purpose | Source | Path at runtime | Permissions |
|---|---|---|---|---|
| `sinex-privacy-key` | `Strategy::Encrypt` token cipher | agenix | `/run/agenix/sinex-privacy-key` | 0400, sinex user |
| `sinex-local-db` | PostgreSQL scram-sha-256 password | agenix | `/run/agenix/sinex-local-db` | 0400, sinex user |
| `sinex-gateway-admin-token` | API admin RPC bearer | agenix | `/run/agenix/sinex-gateway-admin-token` | 0400, sinex user |

Operator constraints:

- Keys are never stored in the database.
- Keys are never exposed via environment variables visible to `ps` /
  `/proc`.
- Keys are generated once and managed through agenix (`age.secrets.*` in
  the NixOS configuration; see `nixos/modules/deployment-topology.md` secret
  inventory).

### Rotation

`xtask privacy key --generate` can generate a new hex key. Replacing the
agenix secret changes the key used for future encryption, but there is no
implemented database rekey command for existing `⌜enc:v1:...⌝` tokens. Do
not document or rely on routine privacy-key rotation until such a command
exists.

For PostgreSQL credentials and API admin tokens: regenerate the agenix
secret, redeploy, and restart the affected service. Token-suffix RBAC has
no revocation surface today (`nixos/modules/deployment-topology.md`).

## What This Doc Intentionally Does Not Specify

This is a pragmatic doctrine, not a luxurious one. The following are
deliberately out of scope until a concrete need lands:

- **pgsodium configuration**: not part of the active at-rest doctrine.
- **Hardware tokens / TPM-bound keys**: agenix + LUKS is the baseline.
  TPM unsealing is a host-config concern, not a Sinex feature.
- **Per-source key derivation**: a single privacy key suffices for the
  current `Strategy::Encrypt` use. Key separation can come with pgsodium.
- **Remote CAS replication encryption**: requires a concrete future
  issue. Do not pre-design the key model here.
- **Backup PITR (WAL archiving) encryption**: same rule — encrypt at the
  envelope before upload; engine specifics belong with the backup design.

If the answer to "what cryptographic property are we adding?" is "depth
beyond LUKS for the realistic threats", and the answer to "what attack
does it prevent that LUKS does not?" is "none that we can name", the
control is not in scope for this doc.

## Related

- `docs/security/threat_model.md` (T1, T3, T5)
- `nixos/modules/deployment-topology.md` (agenix secret inventory,
  service-user permissions)
- `crate/sinexctl/docs/operator_data_lifecycle.md` (operator data lifecycle)
- Issues: #1042 (admission and field-protection policy), #1447 (per-event
  source-material integrity), #1360 (archive secrecy policy)
