# Privacy, authority, and operations

## Privacy grammar

Privacy is not just a settings page. Every object view must indicate privacy state:

- raw visible
- metadata-only
- redacted
- private-mode suppressed
- permission denied
- policy blocked
- deletion/tombstone pending
- export restricted

The UI should show existence and reason when safe, but never leak suppressed content.

Current private-mode controls are real. Broader privacy audit/export/delete/redact workflows should be target unless backed by current issue implementation.

## Authority grammar

Risky actions use a consistent state machine:

1. Read/inspect
2. Preview/dry-run
3. Confirm authority
4. Execute
5. Monitor
6. Result
7. Audit
8. Recover/rollback where supported

Apply this to:

- replay operations
- DLQ purge/requeue
- lifecycle archive/restore/tombstone
- snapshot/restore drills
- parser promotion
- privacy deletion/redaction/export
- semantic lane promotion/discard
- task mutations

## Operations Room

The Operations Room should show operation runs as objects:

- operation id
- domain/kind
- status
- requester/authority
- dry-run preview
- affected refs
- progress/events
- errors/caveats
- audit refs
- rollback/restore affordances if applicable

Operations should be searchable and traceable, not just command output.
