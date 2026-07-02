use super::*;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn parse_replay_state_accepts_known_variants() -> TestResult<()> {
    let states = [
        ("planning", DbReplayState::Planning),
        ("PREVIEWED", DbReplayState::Previewed),
        ("Approved", DbReplayState::Approved),
        ("cancelling", DbReplayState::Cancelling),
    ];
    for (input, expected) in states {
        assert_eq!(parse_replay_state(input).unwrap(), expected);
    }
    assert!(parse_replay_state("unknown").is_err());
    Ok(())
}
