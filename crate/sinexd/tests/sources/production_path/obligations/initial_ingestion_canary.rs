use xtask::sandbox::prelude::*;

/// `WeeChat` log line that the declarative `WeeChatMessageRecord` parser
/// accepts. Must match the tab-separated format:
/// `YYYY-MM-DD HH:MM:SS\tnick\tmessage`
const WEECHAT_FIXTURE_LINE: &[u8] = b"2024-01-15 14:23:45\tsinity\thello from harness canary";

/// Verify that the `weechat.message` declarative parser is reachable through
/// the production-path harness and produces `irc.message` events.
///
/// This is the Wave A end-to-end integration test. Wave B subagents add
/// analogous tests inside the fenced regions of this file or by calling
/// `run()` directly from their own `#[sinex_test]`.
#[sinex_test]
async fn weechat_message_canary() -> TestResult<()> {
    let result = super::run(
        "weechat.message",
        crate::AdapterKind::AppendOnlyFile,
        WEECHAT_FIXTURE_LINE,
        &["irc.message"],
    )
    .await;

    result.map_err(|e| color_eyre::eyre::eyre!("{e}"))
}
