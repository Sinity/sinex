Gmail configuration

Capture uses the operator's own Gmail account with the full `https://mail.google.com/` scope. Every mailbox label is included, including sent mail, drafts, Spam and Trash. The source performs reads; provider deletion records describe Gmail's state and do not remove stored mail.

Discovery reads the account file declared as `input:gmail`. An explicit `--read gmail=PATH` uses the same account configuration:

```json
{
  "account": "owner@example.test",
  "credentials": {
    "client_id_file": "/run/agenix/gmail-client-id",
    "client_secret_file": "/run/agenix/gmail-client-secret",
    "refresh_token_file": "/run/agenix/gmail-refresh-token"
  }
}
```

Relative credential paths resolve beside the account configuration. File contents are trimmed and must be nonempty. Unknown configuration fields and inline secrets are refused. Credentials are loaded at refresh time and access tokens remain in runtime memory. Grant the daemon read access to the files through the existing Sinnix agenix secret wiring (`sinnix.secrets.paths`); secret contents must never enter Nix store files or binding settings.

`gmail::oauth::authorization_url(client_id, redirect_uri, state)` requests full Gmail access and an offline refresh token. The operator completes Google's consent flow and places the resulting refresh token in the configured secret file. Capture never opens a browser or broadens a previously granted scope automatically. The first API profile must identify the configured account.

A history 404 stores `mail.history_gap`. The next poll obtains and stores a new profile baseline, then automatically lists the complete mailbox and resumes history from that baseline. Restarting at any point continues this sequence from admitted progress. Temporary provider errors, authorization failures and rate limits keep the previous committed progress. Missing credential files are reported as unavailable, awaiting credentials.

Raw RFC message responses include attachment bytes; replay parses those stored bytes without Gmail or credential files. Label definitions are stored alongside provider pages/messages. Provider label IDs remain on `mail.provider_message`; the RFC mail event receives label names and the Gmail thread ID for existing mail fusion.

Reference: Google `users.messages.get` documents raw message retrieval and full Gmail scope: https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.messages/get. Offline OAuth consent and refresh are documented at https://developers.google.com/identity/protocols/oauth2/web-server.
