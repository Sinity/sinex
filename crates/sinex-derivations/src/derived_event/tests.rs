use super::*;
use crate::Computed;
use crate::fusion::tests::{at, zsh};
use sinex_model::event_types::shell::ShellSession;

fn session(host: &str) -> ShellSession {
    ShellSession {
        host: host.into(),
        started_at: at(1.0),
        ended_at: at(2.0),
        end_is_lower_bound: false,
        day: "2026-01-01".into(),
        commands: 1,
        failed: 0,
        cwds: vec!["/synthetic/project".into()],
    }
}

#[devkit::test]
fn a_native_derived_nul_is_a_typed_failure_and_discards_the_complete_key() {
    let parent = zsh("synthetic parent", 1.0);
    let first = derived(&session("first"), "synthetic.sessions", [&parent]);
    assert!(first.is_ok());
    let mut invalid = session("second");
    invalid.cwds.push("before\0after".into());
    let second = derived(&invalid, "synthetic.sessions", [&parent]);
    let expected = BuildError::UnstorableCharacter {
        pointer: "/cwds/1".into(),
    };
    assert_eq!(second, Err(expected.clone()));
    let result = Computed::from_outputs([first, second], true);
    assert_eq!(result.error, Some(expected));
    assert!(result.outputs.is_empty());
    assert!(!result.waiting);
}

#[devkit::test]
fn complete_native_outputs_keep_their_order_and_waiting_state() {
    let parent = zsh("synthetic parent", 1.0);
    let result = Computed::from_outputs(
        [
            derived(&session("first"), "synthetic.sessions", [&parent]),
            derived(&session("second"), "synthetic.sessions", [&parent]),
        ],
        true,
    );
    assert!(result.error.is_none());
    assert_eq!(
        result
            .outputs
            .iter()
            .map(|event| event.payload["host"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["first", "second"]
    );
    assert!(result.waiting);
    assert_eq!(
        derived(&session("unparented"), "synthetic.sessions", []),
        Err(BuildError::NoParents)
    );
}

#[devkit::test]
fn declared_json_outputs_use_the_native_unstorable_character_guard() {
    let parent = zsh("synthetic parent", 1.0);
    let mut declaration = sinex_model::KindDeclaration::schemaless(
        sinex_model::KindRef {
            name: "synthetic.derived".into(),
            version: 1,
        },
        "synthetic.producer".into(),
    );
    declaration.key_rule = Some(sinex_model::kinds::KeyRule::Fields {
        fields: vec!["id".into()],
    });
    let valid = serde_json::json!({"id":"stable", "a/b":{"~field":"ordinary text"}});
    assert!(derived_json(&declaration, None, &valid, "synthetic.producer", [&parent]).is_ok());
    let invalid = serde_json::json!({"id":"stable", "a/b":{"~field":"before\0after"}});
    assert_eq!(
        derived_json(
            &declaration,
            None,
            &invalid,
            "synthetic.producer",
            [&parent]
        ),
        Err(BuildError::UnstorableCharacter {
            pointer: "/a~1b/~0field".into()
        })
    );
    let registered = sinex_model::event_types::find("shell.session").unwrap();
    let mut native = serde_json::to_value(session("ordinary")).unwrap();
    native["cwds"] = serde_json::json!(["before\0after"]);
    assert_eq!(
        derived_json(
            &(registered.declaration)(),
            Some(registered.build),
            &native,
            "synthetic.sessions",
            [&parent]
        ),
        Err(BuildError::UnstorableCharacter {
            pointer: "/cwds/0".into()
        })
    );
}
