//! Wave B production-path obligation tests for terminal source units.
//!
//! Source units covered:
//! - `terminal.atuin-history`  (SqliteRowAdapter + AtuinHistoryParser)
//! - `terminal.bash-history`   (AppendOnlyFileAdapter + BashHistoryParser)
//! - `terminal.zsh-history`    (AppendOnlyFileAdapter + ZshHistoryParser)
//! - `terminal.text-history`   (AppendOnlyFileAdapter + TextHistoryParser)
//! - `terminal.fish-history`   (SqliteRowAdapter + FishHistoryParser)
//!
//! SQLite-backed fixtures (atuin, fish) pass pre-serialised JSON rows
//! as fixture bytes, matching what `SqliteRowAdapter` would produce.
//! AppendOnlyFile fixtures (bash, zsh, text) pass plain-text lines.

#[cfg(test)]
mod tests {
    use xtask::sandbox::prelude::*;

    // -------------------------------------------------------------------------
    // Fixtures
    // -------------------------------------------------------------------------

    /// Atuin SQLite row serialised as JSON.
    /// Fields match what `SqliteRowAdapter` produces from the `history` table:
    /// id, command, cwd, session, hostname, timestamp (ns), duration (ns), exit.
    const ATUIN_FIXTURE: &[u8] = br#"{"id":"01HW0000000000000000000001","command":"echo hello","cwd":"/home/sinity","session":"01HW0000000000000000000002","hostname":"sinnix-prime","timestamp":1700000000000000000,"duration":12345678,"exit":0}"#;

    /// Fish SQLite row serialised as JSON.
    /// Fields: command (required), when (optional Unix seconds).
    const FISH_FIXTURE: &[u8] = br#"{"ROWID":1,"command":"git status","when":1700000000}"#;

    /// A plain bash history line (no timestamp prefix).
    const BASH_FIXTURE: &[u8] = b"ls -la /home/sinity\n";

    /// A plain zsh history line (no extended prefix).
    const ZSH_PLAIN_FIXTURE: &[u8] = b"cd /realm/project/sinex\n";

    /// A zsh history line with extended-history prefix `: ts:elapsed;cmd`.
    const ZSH_EXTENDED_FIXTURE: &[u8] = b": 1700000000:0;cargo check\n";

    /// A plain text history line (catch-all parser).
    const TEXT_FIXTURE: &[u8] = b"make build\n";

    // -------------------------------------------------------------------------
    // terminal.atuin-history
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn terminal_atuin_history_obligations(_ctx: TestContext) -> TestResult<()> {
        let failures = crate::_run_case(
            "terminal.atuin-history",
            crate::AdapterKind::SqliteRow,
            ATUIN_FIXTURE,
            &["command.executed"],
            crate::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "terminal.atuin-history obligations failed: {failures:#?}"
        );
        Ok(())
    }

    // -------------------------------------------------------------------------
    // terminal.bash-history
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn terminal_bash_history_obligations(_ctx: TestContext) -> TestResult<()> {
        let failures = crate::_run_case(
            "terminal.bash-history",
            crate::AdapterKind::AppendOnlyFile,
            BASH_FIXTURE,
            &["command.imported"],
            crate::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "terminal.bash-history obligations failed: {failures:#?}"
        );
        Ok(())
    }

    // -------------------------------------------------------------------------
    // terminal.zsh-history (plain)
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn terminal_zsh_history_plain_obligations(_ctx: TestContext) -> TestResult<()> {
        let failures = crate::_run_case(
            "terminal.zsh-history",
            crate::AdapterKind::AppendOnlyFile,
            ZSH_PLAIN_FIXTURE,
            &["command.imported"],
            crate::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "terminal.zsh-history (plain) obligations failed: {failures:#?}"
        );
        Ok(())
    }

    // -------------------------------------------------------------------------
    // terminal.zsh-history (extended prefix)
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn terminal_zsh_history_extended_obligations(_ctx: TestContext) -> TestResult<()> {
        let failures = crate::_run_case(
            "terminal.zsh-history",
            crate::AdapterKind::AppendOnlyFile,
            ZSH_EXTENDED_FIXTURE,
            &["command.imported"],
            crate::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "terminal.zsh-history (extended) obligations failed: {failures:#?}"
        );
        Ok(())
    }

    // -------------------------------------------------------------------------
    // terminal.text-history
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn terminal_text_history_obligations(_ctx: TestContext) -> TestResult<()> {
        let failures = crate::_run_case(
            "terminal.text-history",
            crate::AdapterKind::AppendOnlyFile,
            TEXT_FIXTURE,
            &["command.imported"],
            crate::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "terminal.text-history obligations failed: {failures:#?}"
        );
        Ok(())
    }

    // -------------------------------------------------------------------------
    // terminal.fish-history
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn terminal_fish_history_obligations(_ctx: TestContext) -> TestResult<()> {
        let failures = crate::_run_case(
            "terminal.fish-history",
            crate::AdapterKind::SqliteRow,
            FISH_FIXTURE,
            &["command.imported"],
            crate::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "terminal.fish-history obligations failed: {failures:#?}"
        );
        Ok(())
    }
}
