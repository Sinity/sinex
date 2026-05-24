# Threat Model

Status: design authority. Supersedes target-vision/reference/privacy-and-operations.md §1.

This is the load-bearing threat model that other privacy/security and deployment
design docs assume. When a doc says "fail closed", "trust boundary", "operator
attestation", or "encrypted at rest", it refers back to the threats and
assumptions enumerated here.

## Operating Assumptions

1. **Single deployment shape.** Sinex runs as a local-first service on a
   user-owned workstation. There is no network-exposed multi-tenant deployment
   in scope. The user is both data subject and data controller.
2. **Captured data is intimate.** Keystrokes, browsing, clipboard, health,
   financial, voice, AI conversation. A single breach is materially worse than
   a typical SaaS leak.
3. **The operator is the only line of defense.** No compliance team. No
   external SOC. The system must give the operator the controls and the
   evidence to use them.
4. **LUKS is presumed.** Full-disk encryption on the host is the foundation.
   Every storage-tier control assumes the partition is LUKS-backed.
5. **Trust boundaries are real.** See `runtime-boundaries.md` for the process
   topology that these threats apply to.

## Threats

Six threats anchor the rest of the privacy/security design. Each carries a
realistic likelihood, an impact, and the primary mitigation other docs rely on.

### T1. Physical device theft or seizure

- **Likelihood**: Medium. Laptops vanish; legal seizure happens.
- **Impact**: Critical. Without disk encryption, the PostgreSQL data directory
  and BLAKE3 CAS blobstore are readable by anyone with physical access.
- **Primary mitigation**: LUKS FDE on every partition that holds Sinex state
  (PostgreSQL data dir, CAS root, backup staging, agenix run dir). This is a
  deployment requirement, not a Sinex feature.
- **Secondary mitigation**: Screen lock. Hibernate-with-encrypted-swap if the
  host suspends. Application-layer field encryption (`Strategy::Encrypt`) adds
  depth but does not replace LUKS — if the LUKS key is compromised, app-layer
  keys living next to it on `/run/agenix/` are gone too.
- **Status**: LUKS is a host-config concern. `at-rest-encryption.md` codifies
  the assertion/warning surface inside Sinex.

### T2. Unauthorized local access (other accounts, malware, rogue processes)

- **Likelihood**: Low on a single-user workstation, medium on shared/family
  machines, low-medium when malware is the attacker.
- **Impact**: High. A second local identity could read the PostgreSQL data
  directory or CAS blobs if file permissions are wrong, or query the database
  via the Unix socket without credentials.
- **Primary mitigation**: A dedicated `sinex` service user (uid 991 on the
  reference deployment) that owns the database and ingestion paths. Data
  directories at `0700`. Database role isolation: `sinex_ingestd`,
  `sinex_gateway`, `sinex_readonly`, `sinex_admin` (see
  `nixos/modules/database.nix`). No `SUPERUSER` granted to app roles. Gateway
  RPC requires bearer-token auth even over a local socket.
- **Status**: Service-user model implemented. Permission model documented in
  `deployment-topology.md`.

### T3. Cloud sync and offsite-backup leaks

- **Likelihood**: Medium for users running syncthing, rsync-to-VPS, or cloud
  drive backups. Currently low for the reference deployment (local-only).
- **Impact**: High when unencrypted data lands in an attacker-accessible store.
- **Primary mitigation**: Explicit sync exclusion (`.stignore`, rsync excludes)
  for PostgreSQL data dir, CAS root, agenix run dir, and any unencrypted
  export. Mandatory encryption (`age` recommended) for any backup that leaves
  the host. A future remote CAS replication design must specify encryption,
  key management, and restore testing before implementation; the retired
  git-annex initremote pattern does not apply.
- **Status**: Documentation + `sinexctl doctor` audit. No remote replication
  currently active.

### T4. Accidental exposure in logs and output

- **Likelihood**: High. The single most common real-world failure for systems
  of this shape: credentials in commands hit the journal, clipboard contents
  end up in `tracing::error!`, gateway error responses leak payload fragments.
- **Impact**: Medium to high. Usually internal exposure first, but log
  aggregators, pastebins, and screenshots multiply it quickly.
- **Primary mitigation**: `PrivacyEngine` processes shell commands, clipboard
  text, window titles, journal messages, and D-Bus payloads at admission.
  Tracing discipline: payload content only at `debug!`, never at `error!`.
  Gateway logs request metadata, never response payloads. CLI truncates
  payload fields without `--full`. Full-text search indexes the
  post-PrivacyEngine text, never the raw payload.
- **Status**: PrivacyEngine implemented. Discipline in logging code and CLI
  output truncation is ongoing.

### T5. Application-level access to the database

- **Likelihood**: Low-medium. Malware on the same UID, compromised CLI
  consumers, or scripts running with the wrong credentials.
- **Impact**: High. A reader with the right role sees the entire history.
- **Primary mitigation**: Least-privilege roles. `peer` auth for local socket
  connections. Future: row-level security on `core.events`. Optional:
  pgsodium for column-level encryption of CRITICAL-tier fields (financial,
  health). pgsodium is exploratory, not on the active roadmap.
- **Status**: Role separation implemented. RLS and pgsodium tracked as
  depth-in-defense work; not blocking the baseline model.

### T6. Future-self exposure (retention regret)

- **Likelihood**: Certain. Without deliberate retention, the system
  accumulates indefinitely.
- **Impact**: Subjective but real. Raw keystrokes have a half-life. So do
  ephemeral clipboard contents and detailed window titles. Health trends and
  command history compound positive value over time; not all sources are
  equivalent.
- **Primary mitigation**: Source-differentiated retention defaults expressed
  as scheduled archive → tombstone cascades. Private mode for sessions the
  operator does not want captured at all (`runtime-private-mode.md`).
  Operator-facing audit so the user can see what was captured before deciding
  what to keep (`gdrp-rights-surface.md`). Tombstone is the final answer:
  `id + event_type + source + timestamps` survive, content is gone.
- **Status**: Cascade archive/tombstone primitive exists (see #1134). Default
  retention policy and scheduling surface needs design; tracked alongside
  #1072 and the rights interface.

## Summary

| Threat | Likelihood | Impact | Primary control | Authority doc |
|---|---|---|---|---|
| T1 device theft | Medium | Critical | LUKS FDE | `at-rest-encryption.md` |
| T2 local unauthorized | Low–Med | High | Service user + role isolation | `deployment-topology.md` |
| T3 cloud-sync leak | Medium | High | Sync exclusion + encrypted backup | `at-rest-encryption.md` |
| T4 log/output exposure | High | Med–High | PrivacyEngine + logging discipline | `runtime-private-mode.md` (admission boundary) |
| T5 app-level DB access | Low–Med | High | DB role isolation | `deployment-topology.md` |
| T6 future-self regret | Certain | Subjective | Retention + private mode + rights | `gdpr-rights-surface.md` |

## Non-Goals

- This document does not enumerate per-source classification. That belongs
  to a per-source policy table (vision §2) and is consumed by ingestor
  config, not by this threat model.
- This document does not specify CLI surfaces. Operator surfaces for export,
  delete, audit, and redaction live in `gdpr-rights-surface.md`.
- This document does not specify cryptographic primitives. Algorithm and
  key-management choices live in `at-rest-encryption.md`.
- This document does not specify systemd unit shape or preflight phases.
  Those live in `deployment-topology.md`.

## Related

- `docs/architecture/runtime-private-mode.md`
- `docs/architecture/evidence-lanes.md`
- `docs/architecture/at-rest-encryption.md`
- `docs/architecture/gdpr-rights-surface.md`
- `docs/architecture/deployment-topology.md`
- `docs/architecture/runtime-boundaries.md`
- Issues: #1042, #1065, #1071, #1072, #1442
