use super::{ControlCommandKind, control_command_kind};
use xtask::sandbox::{TestResult, sinex_test};

#[sinex_test]
async fn classifies_known_control_subjects() -> TestResult<()> {
    assert_eq!(
        control_command_kind("sinex.control.sources.weechat.scan"),
        Some(ControlCommandKind::Scan)
    );
    assert_eq!(
        control_command_kind("sinex.control.sources.weechat.drain"),
        Some(ControlCommandKind::Drain)
    );
    assert_eq!(
        control_command_kind("sinex.control.sources.weechat.resume"),
        Some(ControlCommandKind::Resume)
    );
    // `.parse` must classify as Parse so the command listener's wildcard
    // subscription skips it deliberately (the dedicated parse listener
    // responds) instead of treating it as an unsupported subject.
    assert_eq!(
        control_command_kind("sinex.control.sources.weechat.parse"),
        Some(ControlCommandKind::Parse)
    );
    assert_eq!(
        control_command_kind("sinex.control.sources.weechat.unknown"),
        None
    );

    Ok(())
}
