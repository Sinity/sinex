# Email Source Domain

Status: design contract for email as a Sinex source domain. Backlog tracking
is in [#1070](https://github.com/Sinity/sinex/issues/1070) (staged export
parser backlog) and #466 (email ingestor design). Privacy/admission shape
flows through #1042.

Email is a high-sensitivity communication source with two acquisition modes:
mailbox export (staged source material, parsed offline) and live mailbox
sync (Gmail API or IMAP). This record covers both, with the staged path as
the first slice and the live path explicitly separated per the
`staged-source-parser-substrate.md` runtime-boundary rule.

## What This Owns

- Email as source material: Gmail OAuth tokens, IMAP credentials, exported
  mbox/eml files, and how each is registered in `raw.source_material_registry`.
- Per-message classification (personal, newsletter, marketing) and the
  ingestion paths each class takes.
- Body encryption policy for personal mail.
- Attachment handling and downstream document-pipeline triggering.
- Email-specific event taxonomy (`communication.email/*`).

## What This Does Not Own

- IMAP/SMTP transport for outgoing mail. Sinex is a receiver, not a sender.
- Contact entity resolution. Email addresses are one of the sources that
  feed `docs/architecture/entity-resolution.md`.
- General communication analytics (frequency, response latency, etc.). Those
  are downstream synthesis surfaces.
- The on-disk encryption at rest for all sensitive material. That belongs to
  `docs/architecture/at-rest-encryption.md`. Email body encryption is one
  caller of that primitive, not a separate scheme.

## Source Layering

```
staged mailbox export ──┐
                        ├── source material parser (offline)
                        │     └── communication.email/* (material events)
                        │
live Gmail / IMAP ──────┤
                        └── live mailbox sync (separate runtime process)
                              └── communication.email/* (material events)
```

Per `staged-source-parser-substrate.md`, the staged path is the first
implementation slice — exports can be parsed without OAuth, secrets, or live
network access, which keeps the first ingest deterministic and replayable.
Live sync is a separate runtime process gated on privacy admission (#1042)
and operator workflows (#1065, #1072).

## Event Taxonomy

Source prefix: `communication.email`.

| Event | Anchor | Notes |
| --- | --- | --- |
| `message.received` | RFC 2822 `Message-ID` | Personal/newsletter/marketing differentiated by payload flags |
| `message.sent` | `Message-ID` | "Sent" folder messages |
| `thread.started` | thread id | Thread lifecycle synthesis (may be derived) |

Payload fields stay close to the wire format: `from_address`, `to_addresses`,
`cc_addresses`, `subject`, `received_at`, `size_bytes`, `has_attachments`,
`attachment_count`, `labels`, plus classification flags `is_marketing`,
`is_newsletter`.

The bodies do not live in the event payload. They are stored separately,
encrypted, and indexed selectively — see below.

## Acquisition Modes

### Staged Export

Exported mbox/eml directories are registered as source material via
`sinexctl sources stage`. A directory-shape adapter enumerates messages and
dispatches the email parser. Each parsed message emits a material-provenance
event. The export is the durable ground truth; replay re-derives events.

This path requires no secrets. It is the right first slice for #1070.

### Live Gmail

OAuth2 refresh tokens, never client credentials, live in agenix:

```nix
services.sinex.emailSync.gmail = {
  client_id = "...";
  client_secret = config.age.secrets."gmail-oauth-secret".path;
  refresh_token_path = config.age.secrets."gmail-refresh-token".path;
};
```

The initial OAuth dance is a one-time manual step (`sinexctl email auth
gmail`); the running node only refreshes access tokens. Incremental sync
uses Gmail's `historyId`:

1. Initial sync uses `after:YYYY/MM/DD` to page through history.
2. Checkpoint `{last_history_id, last_sync_ts}` is stored in NATS KV.
3. Subsequent runs call `users.history.list(startHistoryId=...)` for deltas.

`historyId` is monotonic within an account and survives label changes —
significantly cheaper than re-listing.

### Live IMAP Fallback

For Fastmail, Protonmail bridge, self-hosted servers, etc.:

```rust
enum EmailBackend {
    Gmail { client: GmailApiClient },
    Imap {
        host: String, port: u16,
        username: String,
        password_path: PathBuf,   // agenix secret
        use_tls: bool,
    },
}
```

IMAP checkpoint is `(mailbox, uid_validity, last_uid)`. `UIDVALIDITY` must
be part of the checkpoint to detect mailbox resets — IMAP UIDs are only
valid within one validity epoch.

## Classification Pipeline

Before bodies are touched, every message is classified:

| Signal | Classification |
| --- | --- |
| `List-Unsubscribe` header present | `is_newsletter = true` |
| From-address matches configured marketing domains | `is_marketing = true` |
| `X-Mailer` contains `bulk` | `is_marketing = true` |
| From-address in configured newsletter senders | `is_newsletter = true` |
| Otherwise | `is_personal = true` |

Classification consequences:

| Class | Ingestion |
| --- | --- |
| Marketing | Metadata only; body discarded. |
| Newsletter | Body extracted and routed to the document layer for parsing and embedding. |
| Personal | Full metadata event + encrypted body blob. |

Classification flags ride on the event payload so downstream consumers
(search, retention, analytics) can filter without re-classifying.

## Body Encryption

Personal email bodies are encrypted at rest before storage. The encryption
primitive is shared with the rest of the sensitive-content path — see
`docs/architecture/at-rest-encryption.md`.

Storage shape:

| Surface | Stored as |
| --- | --- |
| Body cipher | Encrypted blob, content-addressed |
| FTS index | `subject`, `from_address` only — body never indexed in plaintext |
| Search-time decryption | Requires explicit operator/user step; not implicit |

The key used is the privacy engine's configured key, consistent with shell
token encryption and other sensitive surfaces. Newsletter bodies are not
encrypted — they are routed through the ordinary document layer because
their content is not personal.

Until `docs/architecture/at-rest-encryption.md` lands, this contract
references the privacy engine's `Strategy::Encrypt` (XChaCha20-Poly1305)
directly. The cross-ref keeps stable when the encryption layer is promoted
to its own record.

## Attachments

```
MATCH attachment_mime_type:
  | image/*              → blob storage, emit blob.attached
  | application/pdf      → blob storage + trigger document-ingestor
  | application/msword   → blob storage + trigger document-ingestor
  | text/*               → store inline in annotation (no separate blob)
  | _                    → blob storage, no processing pipeline
```

Inline images (`Content-Disposition: inline`, `Content-ID` set) are stripped
from the body before encryption and stored as referenced blobs.

## Retention

Email retention is an explicit operator decision, not an implicit default.
Bodies are encrypted, blobs are content-addressed, and the existing retention
work (#1065/#1072) covers how to purge bodies while preserving event
metadata. This record commits only that retention runs at the body-blob layer,
not by mutating event payloads in place.

## Privacy

Email is the highest-sensitivity communication source.

| Surface | Policy |
| --- | --- |
| Body (personal) | Encrypted at rest; never indexed plaintext. |
| Body (newsletter) | Document-layer admission; redaction per the engine. |
| Body (marketing) | Discarded. |
| Subject, from-address | Plaintext FTS allowed; still subject to admission. |
| Attachments | Blob storage with privacy class inherited from body class. |
| OAuth/IMAP secrets | agenix only; never in repo, never in event payloads. |

The privacy admission policy in #1042 owns the explicit per-field shape; this
record commits to the encryption-by-default invariant for personal bodies.

## Open Questions

- Whether the staged-export and live-sync paths should share a single parser
  binary with a runtime-mode flag, or live as two source units. Default
  expectation per `staged-source-parser-substrate.md`: same parser, two
  runtime placements.
- How "sent" detection works on IMAP servers with non-standard "Sent"
  folders. Per-account config, not heuristics.
- Whether thread reconstruction is a material-provenance event (parsed from
  References/In-Reply-To headers) or a synthesis derivation. Default: thread
  identity is parsed material; thread analytics are synthesis.

## Boundaries

- Do not store personal-mail bodies in plaintext, ever.
- Do not put OAuth tokens or IMAP passwords into event payloads or logs.
- Do not include the bodies of personal mail in broad-scope context packs
  or search results without explicit decrypt-with-consent.
- Do not bundle staged-export and live-sync into one runtime process. They
  are different trust and capability profiles.

**Related:** `docs/architecture/staged-source-parser-substrate.md`,
`docs/architecture/at-rest-encryption.md`,
`docs/architecture/entity-resolution.md`,
`docs/architecture/document-layer-v1.md`,
issues #1070, #466, #1042, #1065, #1072.
