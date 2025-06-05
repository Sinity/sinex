# TIM-EmailAccessProtocols: Email Access (IMAP / Gmail API)

*   **Relevant ADR:** (N/A directly, core ingestor for email data)
*   **Original UG Context:** Section 14

This TIM details the technical aspects of accessing and ingesting email content and metadata into the Exocortex, focusing on the Gmail API and considerations for IMAP.

## 1. Rationale Summary

Email is a significant source of personal information, communication, and attachments. Ingesting emails allows them to be linked into the Exocortex knowledge graph, made searchable, and their content (text, attachments) processed. The Gmail API is preferred for Gmail accounts due to structured data access, while IMAP offers broader compatibility.

## 2. Gmail API [UG Sec 14.1, 14.2, 14.3, CR2]

### 2.1. Authentication and OAuth2 Scopes

*   **Authentication:** OAuth 2.0. Exocortex Email Ingestor (`agent/email_gmail_ingestor`) needs to:
    1.  Guide user through Google OAuth consent flow.
    2.  Obtain access token and refresh token.
    3.  Store refresh token securely (e.g., encrypted via `agenix`, referenced by agent config). Use access token for API calls, refresh when expired.
*   **Recommended OAuth2 Scopes [CR2]:**
    *   `https://www.googleapis.com/auth/gmail.readonly`: Read all email resources (messages, threads, labels, history) and settings. **Preferred for basic Exocortex ingest.**
    *   `https://www.googleapis.com/auth/gmail.metadata`: Read-only metadata (headers, labels, IDs) but not bodies/attachments. (Too limited for full ingest).
    *   *(Avoid `gmail.modify` or `mail.google.com/` unless Exocortex needs to send mail or manage labels/trash, which is not primary ingest scope).*
*   **API Endpoint Base:** `https://gmail.googleapis.com/gmail/v1/users/{userId}/...` (`{userId}` is usually `"me"`).

### 2.2. Rate Limits and Quota Costs [CR2]

*   **Daily Project Quota:** 1 billion units/day (shared by all users of your GCP project).
*   **Per-User Rate Limit:** ~250 quota units/sec (burst).
*   **Method Quota Costs (Examples):**
    *   `users.messages.list`: 5 units.
    *   `users.messages.get` (format `METADATA`): 5 units.
    *   `users.messages.get` (format `FULL` or `RAW`): Variable, 5 units + per-byte cost.
    *   `users.history.list`: 5 units per page.
*   **Error Handling:** Implement exponential backoff for HTTP 429 (Too Many Requests) or 403 (UserRateLimitExceeded/QuotaExceeded). Respect `Retry-After` header.

### 2.3. Message JSON Structure (Key Fields from `users.messages.get`) [UG Sec 14.2, CR2]

A Gmail message resource includes:
*   `id`: Immutable message ID (string).
*   `threadId`: Thread ID (string).
*   `labelIds`: Array of label ID strings (e.g., "INBOX", "SENT", "STARRED", "UNREAD").
*   `snippet`: Short plain text snippet.
*   `historyId`: ID of last history record modifying this message.
*   `internalDate`: Unix ms timestamp (when Gmail received/created).
*   `payload`: Parsed MIME message structure (root `MessagePart`).
    *   `partId`: String (e.g., "0", "1", "0.1").
    *   `mimeType`: String (e.g., "multipart/alternative", "text/plain", "image/jpeg").
    *   `filename`: For attachment parts.
    *   `headers`: Array of `{name, value}` objects (Subject, From, To, Cc, Date, Message-ID, In-Reply-To, References, etc.).
    *   `body`:
        *   `attachmentId`: String (if content is separate attachment).
        *   `size`: Integer (bytes of body data).
        *   `data`: Base64url-encoded content (for inline parts like text, small images).
    *   `parts`: Array of nested `MessagePart` objects if this part is multipart.
*   `sizeEstimate`: Integer (total message size in bytes).
*   `raw`: Base64url-encoded full RFC 2822 message (if requested with `format="RAW"`).

### 2.4. Ingestion Workflow for Gmail Agent

1.  **Initial Sync (History):**
    *   Use `users.history.list` to get a sequence of changes (new messages, label changes, deletions) since a `startHistoryId` (persisted by agent from last sync).
    *   For each `messageAdded` history record, get the `message.id`.
2.  **Fetch Message Details:**
    *   For each new `message.id` (or messages found via `users.messages.list` with a query like `labelIds:INBOX is:unread` for ongoing sync):
        *   Call `users.messages.get` with `id` and `format="FULL"` (or `format="METADATA"` first, then `FULL` if needed).
3.  **Process Payload (Recursive MIME Traversal):**
    *   Parse the `payload` structure.
    *   Identify the primary textual content (prefer `text/plain`, then `text/html` which needs sanitization/conversion to Markdown).
    *   Identify attachments (see Section 2.5).
4.  **Eventification & Storage:**
    *   For each email message, create a `core_artifacts` entry:
        *   `artifact_type = 'email_message'`
        *   `canonical_identifier = gmail_message_id` (or `Message-ID` header value if globally unique)
        *   `current_title = Subject` header
        *   `properties`: `{ "from": "...", "to": ["..."], "cc": ["..."], "date_sent_iso": "...", "thread_id_gmail": "...", "labels_gmail": ["..."] }`
    *   Store extracted plain text or Markdown version in `core_artifact_contents`, linked to the `artifact_id`. `core_artifacts.current_content_id` points here.
    *   Log `email.message.ingested_gmail` event to `raw.events`. Payload includes `artifact_id`, `content_id`, key headers, and references to any attachment `blob_id`s.
5.  **Attachment Handling (See Section 2.5).**
6.  **Watermarking:** Persist the latest `historyId` processed to resume sync efficiently.

### 2.5. Attachment Handling (Gmail API) [UG Sec 14.3, CR2]

*   **Limits:** Gmail total email size ~25MB. Individual attachment limits also apply.
*   **API Access:**
    *   Small attachments may be inline in `message.payload.parts[...].body.data` (Base64url).
    *   Larger ones have an `attachmentId` in `message.payload.parts[...].body`. Fetch using `users.messages.attachments.get(userId, messageId, attachmentId)`. This returns `MessagePartBody` with `data` (Base64url).
*   **Exocortex Storage:**
    1.  Download attachment data.
    2.  Decode Base64url.
    3.  Compute BLAKE3 hash.
    4.  Store as `core_blobs` (git-annexed).
    5.  The `email.message.ingested_gmail` event payload or `core_artifacts.properties` for the email artifact should list associated attachment `blob_id`s or `annex_key`s with their original filenames and MIME types.

## 3. IMAP (Internet Message Access Protocol)

IMAP provides broader compatibility for non-Gmail accounts.

*   **Libraries:** Python `imaplib` (standard library), Rust `imap` crate.
*   **Connection:** Requires IMAP server hostname, port (993 for IMAPS/SSL, 143 for STARTTLS), username, password (or app-specific password/OAuth2 token if server supports it). Store credentials securely (`agenix`).
*   **Core IMAP Operations for Ingestion:**
    1.  `LOGIN`
    2.  `SELECT "INBOX"` (or other mailboxes like "Sent", "Archive").
    3.  `SEARCH UNSEEN` (to find unread messages) or `SEARCH SINCE <date>` (for incremental sync). Returns sequence numbers or UIDs.
        *   UIDs are persistent per mailbox; sequence numbers can change. Prefer UIDs.
    4.  `FETCH <uid_or_seq_num> (UID RFC822.HEADER BODY.PEEK[TEXT] FLAGS ENVELOPE INTERNALDATE)`:
        *   `UID`: Get message UID.
        *   `RFC822.HEADER` or `ENVELOPE`: Get standard email headers.
        *   `BODY.PEEK[TEXT]` or `BODY.PEEK[1]`: Get plain text part of body without marking as read.
        *   `BODY.PEEK[]` or `RFC822`: Get full raw message content (for parsing MIME structure and attachments).
        *   `FLAGS`: Get flags (`\Seen`, `\Answered`, `\Flagged`, `\Deleted`, `\Draft`).
        *   `INTERNALDATE`: Server internal arrival time.
    5.  Parse fetched data (RFC 822/MIME parsing using libraries like Python `email` package).
    6.  Process similar to Gmail: create `core_artifacts`, `core_artifact_contents`, store attachments as `core_blobs`, log `email.message.ingested_imap` event.
    7.  Watermarking: Store highest `UID` processed for each mailbox (or `INTERNALDATE`) to resume sync.
*   **Challenges with IMAP:**
    *   MIME parsing can be complex.
    *   No standard "thread ID" like Gmail; threading must be inferred from `In-Reply-To` / `References` headers.
    *   Attachment handling involves parsing multipart MIME structures.
    *   Less structured metadata compared to Gmail API (e.g., labels are often mapped to IMAP keywords or folders, requiring interpretation).
    *   Performance can be slower for fetching many messages compared to batch APIs like Gmail's.

