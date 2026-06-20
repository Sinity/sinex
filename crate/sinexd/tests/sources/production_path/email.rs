//! Production-path obligation tests for the `email.mailbox` package (#1469).
//!
//! These cases exercise the accepted staged mailbox mode through the shared
//! production-path harness. Unit tests in `source_contracts/email.rs` cover the
//! detailed identity fields; this module proves the registered package mode is
//! no longer blocked from the production-path matrix for RFC822 drops, Maildir
//! entries, and MBOX slices.

#[cfg(test)]
mod tests {
    use xtask::sandbox::prelude::*;

    const RFC822_FIXTURE: &[u8] = b"Message-ID: <rfc822-pp@example.com>\r\n\
Date: Tue, 14 Jan 2025 12:00:00 +0000\r\n\
From: Alice <alice@example.com>\r\n\
To: Bob <bob@example.com>\r\n\
Subject: Production path RFC822\r\n\
Bcc: Hidden <hidden@example.com>\r\n\
List-Id: team.example.com\r\n\
\r\n\
Hello from a staged RFC822 drop.\r\n";

    const MAILDIR_FIXTURE: &[u8] = b"Message-ID: <maildir-pp@example.com>\n\
Date: Tue, 14 Jan 2025 12:01:00 +0000\n\
From: Alice <alice@example.com>\n\
To: Bob <bob@example.com>\n\
Subject: Production path Maildir\n\
\n\
Hello from Maildir.\n";

    const MBOX_FIXTURE: &[u8] = b"Message-ID: <mbox-pp@example.com>\n\
Date: Tue, 14 Jan 2025 12:02:00 +0000\n\
From: Alice <alice@example.com>\n\
To: Bob <bob@example.com>\n\
Subject: Production path MBOX\n\
\n\
Hello from an MBOX slice.\n";

    #[sinex_test]
    async fn email_rfc822_drop_obligations() -> TestResult<()> {
        let failures = crate::_run_case_with_logical_path(
            "email.mailbox",
            crate::AdapterKind::StaticFile,
            RFC822_FIXTURE,
            "imports/inbox/rfc822-pp.eml",
            &["email.message.received"],
            crate::ALL_OBLIGATIONS,
        )
        .await;

        if failures.is_empty() {
            Ok(())
        } else {
            Err(color_eyre::eyre::eyre!(
                "email RFC822 production-path obligations failed: {failures:#?}"
            ))
        }
    }

    #[sinex_test]
    async fn email_maildir_entry_obligations() -> TestResult<()> {
        let failures = crate::_run_case_with_logical_path(
            "email.mailbox",
            crate::AdapterKind::StaticFile,
            MAILDIR_FIXTURE,
            "Maildir/INBOX/cur/1710000000.M1P1.host:2,RS",
            &["email.message.received"],
            crate::ALL_OBLIGATIONS,
        )
        .await;

        if failures.is_empty() {
            Ok(())
        } else {
            Err(color_eyre::eyre::eyre!(
                "email Maildir production-path obligations failed: {failures:#?}"
            ))
        }
    }

    #[sinex_test]
    async fn email_mbox_slice_obligations() -> TestResult<()> {
        let failures = crate::_run_case_with_logical_path(
            "email.mailbox",
            crate::AdapterKind::StaticFile,
            MBOX_FIXTURE,
            "exports/inbox.mbox",
            &["email.message.received"],
            crate::ALL_OBLIGATIONS,
        )
        .await;

        if failures.is_empty() {
            Ok(())
        } else {
            Err(color_eyre::eyre::eyre!(
                "email MBOX production-path obligations failed: {failures:#?}"
            ))
        }
    }
}
