//! Aggregated test entrypoint for tests/runtime/*.rs (sinex-v7od).
//!
//! Consolidates what used to be N separate `[[test]]` binaries (each
//! relinking the full sinexd lib) into one. cargo-nextest still runs every
//! test function in its own process regardless of binary layout, so this
//! loses no test isolation.

mod runtime {
    mod email_gmail_api_cursor_adapter_test;
    mod email_imap_sync_adapter_test;
    mod email_mbox_file_adapter_test;
}
