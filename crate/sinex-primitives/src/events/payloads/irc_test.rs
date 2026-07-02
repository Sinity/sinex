use super::*;
use crate::events::EventPayload as _;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn irc_payloads_declare_weechat_event_pairs() -> TestResult<()> {
    assert_eq!(IrcJoinPayload::SOURCE.as_static_str(), "irc");
    assert_eq!(IrcJoinPayload::EVENT_TYPE.as_static_str(), "irc.join");
    assert_eq!(IrcPartPayload::EVENT_TYPE.as_static_str(), "irc.part");
    assert_eq!(
        IrcServerNoticePayload::EVENT_TYPE.as_static_str(),
        "irc.server_notice"
    );
    assert_eq!(IrcMessagePayload::EVENT_TYPE.as_static_str(), "irc.message");
    Ok(())
}
