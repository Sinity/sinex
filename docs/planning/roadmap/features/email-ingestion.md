# Email Ingestion (Gmail API / IMAP)

**Status**: Designed, not implemented
**Implementation**: 0% (Design complete, implementation not started)
**Priority**: Medium
**Dependencies**: Gmail API credentials, OAuth2 flow, IMAP libraries, email parsing
**Blocks**: Email content ingestion, communication analysis, attachment processing

## Overview

Email is a significant source of personal information, communication, and attachments. This feature enables ingesting emails into the Sinex knowledge graph, making them searchable and linkable with other data. The Gmail API is preferred for Gmail accounts due to structured data access, while IMAP offers broader compatibility.

## Technical Specification

### Gmail API Integration

**Authentication**:
- OAuth 2.0 flow for user consent
- Secure storage of refresh tokens (via agenix)
- Automatic token refresh on expiration

**Recommended OAuth2 Scopes**:
- `https://www.googleapis.com/auth/gmail.readonly` - Full read access (preferred)
- Avoid modify scopes unless needed for specific features

**Rate Limits**:
- Daily quota: 1 billion units/day (project-wide)
- Per-user: ~250 quota units/sec (burst)
- Method costs:
  - `messages.list`: 5 units
  - `messages.get` (metadata): 5 units
  - `messages.get` (full): 5 units + per-byte cost

### Message Structure

Gmail message key fields:
```json
{
  "id": "message_id",
  "threadId": "thread_id",
  "labelIds": ["INBOX", "UNREAD"],
  "snippet": "Short preview text...",
  "historyId": "12345",
  "internalDate": "1234567890000",
  "payload": {
    "partId": "0",
    "mimeType": "multipart/alternative",
    "headers": [
      {"name": "Subject", "value": "..."},
      {"name": "From", "value": "..."},
      {"name": "To", "value": "..."}
    ],
    "body": {...},
    "parts": [...]
  }
}
```

### Ingestion Workflow

1. **Initial Sync**:
   - Use `history.list` for incremental changes
   - Track `startHistoryId` for resumption

2. **Message Processing**:
   - Fetch message details with appropriate format
   - Parse MIME structure recursively
   - Extract text content (prefer plain text)

3. **Storage**:
   - Create `core.artifacts` entry:
     - `artifact_type`: 'email_message'
     - `canonical_identifier`: gmail_message_id
     - Store metadata in properties
   - Save content in `core.artifact_contents`
   - Log ingestion event

4. **Attachment Handling**:
   - Download via `attachments.get` API
   - Decode Base64url encoding
   - Store as `core.blobs` with git-annex
   - Link to parent email artifact

### IMAP Protocol Support

For non-Gmail accounts:

**Connection**:
- Server hostname, port (993 for IMAPS, 143 for STARTTLS)
- Username/password or app-specific tokens
- Secure credential storage

**Core Operations**:
```
LOGIN
SELECT "INBOX"
SEARCH UNSEEN
FETCH <uid> (UID RFC822.HEADER BODY.PEEK[TEXT] FLAGS)
```

**Challenges**:
- Complex MIME parsing
- No standard thread IDs (infer from headers)
- Less structured metadata
- Performance limitations

## Implementation Architecture

### Email Ingestion Agent

**Responsibilities**:
- OAuth2 flow management
- API quota management
- Incremental sync logic
- Error handling and retry

**Event Schema**:
```json
{
  "source": "agent.email_gmail_ingestor",
  "event_type": "email.message.ingested",
  "payload": {
    "artifact_id": "ULID",
    "content_id": "ULID",
    "message_id": "gmail_id",
    "thread_id": "thread_id",
    "from": "sender@example.com",
    "to": ["recipient@example.com"],
    "subject": "Email subject",
    "date_sent": "2024-01-01T12:00:00Z",
    "labels": ["INBOX", "IMPORTANT"],
    "attachment_blob_ids": ["blob_id1", "blob_id2"]
  }
}
```

### MIME Processing

**Text Extraction**:
1. Traverse multipart structure
2. Prioritize text/plain parts
3. Convert HTML to Markdown if needed
4. Handle encoding issues gracefully

**Attachment Processing**:
1. Identify attachment parts
2. Extract metadata (filename, MIME type)
3. Download and store content
4. Generate preview if applicable

## Implementation Plan

### Phase 1: Gmail API Core
- [ ] OAuth2 authentication flow
- [ ] Basic message fetching
- [ ] Text content extraction
- [ ] Simple storage in artifacts

### Phase 2: Advanced Features
- [ ] Attachment handling
- [ ] Thread reconstruction
- [ ] Label/folder mapping
- [ ] Contact extraction

### Phase 3: IMAP Support
- [ ] IMAP connection management
- [ ] MIME parser implementation
- [ ] Multi-provider support
- [ ] Performance optimization

### Phase 4: Integration
- [ ] Search integration
- [ ] Knowledge graph linking
- [ ] Duplicate detection
- [ ] Privacy controls

## Performance Considerations

### Gmail API
- Batch requests where possible
- Implement exponential backoff
- Cache processed message IDs
- Use partial sync via history API

### IMAP
- Connection pooling
- Parallel message fetching
- Local caching of headers
- Efficient UID tracking

## Privacy and Security

### Data Protection
- Encrypt stored credentials
- Minimal scope requests
- User control over data retention
- Audit logging of access

### Compliance
- Respect user's email retention policies
- Support data export/deletion
- Clear consent for email access
- Transparent data usage

## Future Enhancements

- **Smart Categorization**: ML-based email classification
- **Relationship Mapping**: Build communication graphs
- **Attachment Intelligence**: Content extraction from PDFs, documents
- **Email Analytics**: Communication patterns and insights
- **Multi-Account Management**: Unified inbox across providers
- **Real-time Sync**: Push notifications for new emails