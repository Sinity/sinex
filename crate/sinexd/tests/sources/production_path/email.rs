//! Production-path obligation tests for the `email.mailbox` package (#1469).
//!
//! These cases exercise the accepted staged mailbox mode through the shared
//! production-path harness. Unit tests in `source_contracts/email.rs` cover the
//! detailed identity fields; this module proves the registered package mode is
//! no longer blocked from the production-path matrix for RFC822 drops, Maildir
//! entries, and MBOX slices.

#[cfg(test)]
#[path = "email_test.rs"]
mod tests;
