//! Command fusion (architecture §6.4): one fused command from every source that
//! saw it (Atuin, zsh, kitty), by declared rules, with every source record as a
//! parent. The key is the host and the minute the command started, so readings
//! of one command from several sources meet in one key.
use crate::{Env, InputQuery, KeyState, KeyView, Keyed};
use serde_json::{Value, json};
use sinex_model::{Event, Timestamp};

pub struct CommandFusion;

impl Keyed for CommandFusion {
    const NAME: &'static str = "command.fusion";
    const VERSION: u32 = 1;
    const INPUTS: &'static [&'static str] = &["command.executed", "command.imported"];

    fn keys_for(&self, input: &KeyView<'_>) -> Vec<Value> {
        let host = input
            .payload
            .get("hostname")
            .and_then(Value::as_str)
            .map(|h| h.split(':').next().unwrap_or(h).to_owned());
        match (host, input.ts_orig) {
            (Some(host), Some(t)) => {
                vec![json!({"host": host, "minute": t.0.unix_timestamp().div_euclid(60)})]
            }
            // No time or no host: nothing to fuse with; the record stays separate.
            _ => vec![],
        }
    }

    fn inputs(&self, _key: &Value) -> InputQuery {
        todo!("architecture §6.4: the key's window and host as an input query")
    }

    fn compute(&self, _key: &Value, _inputs: &[Event], _env: &Env) -> Vec<Event> {
        todo!(
            "architecture §6.4: declared fusion rules; best time evidence; every source record a parent"
        )
    }

    fn key_state(&self, _key: &Value, _now: Timestamp) -> KeyState {
        todo!("architecture §6.1: a minute key closes once its window has settled")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readings_of_one_command_share_a_key() {
        let t = Timestamp::from_unix_secs(1_700_000_030).unwrap();
        let atuin = json!({"hostname": "sinnix-prime:sinity"});
        let zsh = json!({"hostname": "sinnix-prime"});
        let a = CommandFusion.keys_for(&KeyView {
            event_type: "command.executed",
            ts_orig: Some(t),
            payload: &atuin,
        });
        let z = CommandFusion.keys_for(&KeyView {
            event_type: "command.imported",
            ts_orig: Some(t),
            payload: &zsh,
        });
        assert_eq!(a, z);
        assert!(
            CommandFusion
                .keys_for(&KeyView {
                    event_type: "command.imported",
                    ts_orig: None,
                    payload: &zsh
                })
                .is_empty()
        );
    }
}
