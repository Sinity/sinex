use super::*;
use sinex_primitives::rpc::{RpcMutability, method_catalog};
use xtask::sandbox::prelude::sinex_test;

#[sinex_test]
async fn all_registry_entries_have_at_least_one_format_or_note()
-> xtask::sandbox::TestResult<()> {
    for (cmd, cap) in build() {
        assert!(
            !cap.supported.is_empty() || cap.note.is_some(),
            "command `{cmd}` has no supported formats and no explanatory note"
        );
    }
    Ok(())
}

#[sinex_test]
async fn validate_format_rejects_unsupported() -> xtask::sandbox::TestResult<()> {
    let result = validate_format("_complete", OutputFormat::Dot);
    assert!(result.is_err(), "_complete should reject dot");
    let msg = result.unwrap_err();
    assert!(msg.contains("_complete"), "error should name the command");
    assert!(
        msg.contains("Dot"),
        "error should name the unsupported format"
    );
    Ok(())
}

#[sinex_test]
async fn validate_format_accepts_supported() -> xtask::sandbox::TestResult<()> {
    assert!(validate_format("events query", OutputFormat::Json).is_ok());
    assert!(validate_format("events query", OutputFormat::Ndjson).is_ok());
    assert!(validate_format("events context", OutputFormat::Json).is_ok());
    assert!(validate_format("events context", OutputFormat::Ndjson).is_err());
    assert!(validate_format("events explain", OutputFormat::Json).is_ok());
    assert!(validate_format("events explain", OutputFormat::Ndjson).is_err());
    assert!(validate_format("events timeline", OutputFormat::Json).is_ok());
    assert!(validate_format("events timeline", OutputFormat::Ndjson).is_err());
    assert!(validate_format("events trace", OutputFormat::Json).is_ok());
    assert!(validate_format("events trace", OutputFormat::Dot).is_ok());
    assert!(validate_format("events trace", OutputFormat::Ndjson).is_err());
    assert!(validate_format("events watch", OutputFormat::Json).is_ok());
    assert!(validate_format("events watch", OutputFormat::Ndjson).is_ok());
    assert!(validate_format("ops replay watch", OutputFormat::Ndjson).is_ok());
    Ok(())
}

#[sinex_test]
async fn command_catalog_covers_registry_entries() -> xtask::sandbox::TestResult<()> {
    let reg = registry();
    let catalog = command_catalog();
    assert_eq!(catalog.len(), reg.len());
    for entry in catalog {
        assert!(
            reg.contains_key(entry.path),
            "catalog entry `{}` must be backed by the format registry",
            entry.path
        );
    }
    Ok(())
}

#[sinex_test]
async fn command_catalog_serializes_machine_readable_matrix() -> xtask::sandbox::TestResult<()>
{
    let value = serde_json::to_value(command_catalog())?;
    let entries = value
        .as_array()
        .ok_or_else(|| color_eyre::eyre::eyre!("command catalog must serialize as an array"))?;

    assert_eq!(entries.len(), registry().len());
    for entry in entries {
        assert!(entry["path"].as_str().is_some());
        assert!(entry["family"].as_str().is_some());
        assert!(entry["effect"].as_str().is_some());
        assert!(entry["backing_rpc_methods"].as_array().is_some());
        assert!(entry["required_rpc_role"].is_string() || entry["required_rpc_role"].is_null());
        assert!(entry["mutation_guards"].as_array().is_some());
        assert!(entry["capability"]["supported"].as_array().is_some());
        assert!(entry["capability"]["streaming"].as_bool().is_some());
    }
    Ok(())
}

#[sinex_test]
async fn command_catalog_classifies_known_effects() -> xtask::sandbox::TestResult<()> {
    let catalog = command_catalog();
    let effect_for = |path: &str| {
        catalog
            .iter()
            .find(|entry| entry.path == path)
            .map(|entry| entry.effect)
    };

    assert_eq!(effect_for("events query"), Some(CommandEffect::ReadOnly));
    assert_eq!(effect_for("query"), Some(CommandEffect::ReadOnly));
    assert_eq!(
        effect_for("events relations within"),
        Some(CommandEffect::ReadOnly)
    );
    assert_eq!(effect_for("events watch"), Some(CommandEffect::Streaming));
    assert_eq!(effect_for("events annotate"), Some(CommandEffect::Mutating));
    assert_eq!(effect_for("_complete"), Some(CommandEffect::ReadOnly));
    assert_eq!(effect_for("ops dlq requeue"), Some(CommandEffect::Mutating));
    assert_eq!(
        effect_for("privacy private-mode enable"),
        Some(CommandEffect::Mutating)
    );
    assert_eq!(effect_for("privacy audit"), Some(CommandEffect::ReadOnly));
    assert_eq!(effect_for("ops audit"), Some(CommandEffect::ReadOnly));
    assert_eq!(
        effect_for("semantic curation finalize"),
        Some(CommandEffect::Mutating)
    );
    assert_eq!(
        effect_for("ops instructions hyprland-workspace"),
        Some(CommandEffect::Mutating)
    );
    assert_eq!(effect_for("ops replay plan"), Some(CommandEffect::Mutating));
    assert_eq!(
        effect_for("ops replay preview"),
        Some(CommandEffect::Mutating)
    );
    assert_eq!(
        effect_for("ops state inspect"),
        Some(CommandEffect::ReadOnly)
    );
    assert_eq!(
        effect_for("ops state restore"),
        Some(CommandEffect::Mutating)
    );
    assert_eq!(
        effect_for("ops state snapshot"),
        Some(CommandEffect::Mutating)
    );
    Ok(())
}

#[sinex_test]
async fn command_catalog_streaming_rows_advertise_ndjson() -> xtask::sandbox::TestResult<()> {
    for entry in command_catalog() {
        if entry.capability.streaming {
            assert_eq!(
                entry.effect,
                CommandEffect::Streaming,
                "streaming command `{}` must be classified as streaming",
                entry.path
            );
            assert!(
                entry.capability.supports(OutputFormat::Ndjson),
                "streaming command `{}` must advertise ndjson row output",
                entry.path
            );
            continue;
        }

        assert_ne!(
            entry.effect,
            CommandEffect::Streaming,
            "non-streaming command `{}` must not be classified as streaming",
            entry.path
        );

        if entry.capability.supports(OutputFormat::Ndjson) {
            assert!(
                entry.capability.note.is_some(),
                "finite ndjson-capable command `{}` must document what one ndjson row means",
                entry.path
            );
        }
    }
    Ok(())
}

#[sinex_test]
async fn command_catalog_dot_output_is_graph_scoped() -> xtask::sandbox::TestResult<()> {
    for entry in command_catalog() {
        if !entry.capability.supports(OutputFormat::Dot) {
            continue;
        }

        assert!(
            matches!(entry.path, "events trace" | "ops replay graph"),
            "command `{}` advertises dot output but is not a graph-shaped surface",
            entry.path
        );
        assert!(
            entry.capability.note.is_some(),
            "dot-capable command `{}` must document the graph it renders",
            entry.path
        );
    }
    Ok(())
}

#[sinex_test]
async fn command_catalog_mutating_entries_declare_guards() -> xtask::sandbox::TestResult<()> {
    for entry in command_catalog() {
        if entry.effect != CommandEffect::Mutating {
            assert!(
                entry.mutation_guards.is_empty(),
                "non-mutating command `{}` must not declare mutation guards",
                entry.path
            );
            continue;
        }

        assert!(
            !entry.mutation_guards.is_empty(),
            "mutating command `{}` must declare at least one mutation guard",
            entry.path
        );
    }
    Ok(())
}

#[sinex_test]
async fn command_catalog_rpc_mutations_declare_rpc_auth() -> xtask::sandbox::TestResult<()> {
    for entry in command_catalog() {
        if entry.effect != CommandEffect::Mutating || entry.backing_rpc_methods.is_empty() {
            continue;
        }

        assert!(
            entry
                .mutation_guards
                .contains(&CommandMutationGuard::RpcAuth),
            "mutating RPC-backed command `{}` must declare rpc_auth",
            entry.path
        );
    }
    Ok(())
}

#[sinex_test]
async fn command_catalog_local_mutations_declare_operator_guard()
-> xtask::sandbox::TestResult<()> {
    for entry in command_catalog() {
        if entry.effect != CommandEffect::Mutating || !entry.backing_rpc_methods.is_empty() {
            continue;
        }

        let has_local_guard = entry.mutation_guards.iter().any(|guard| {
            matches!(
                guard,
                CommandMutationGuard::DryRun
                    | CommandMutationGuard::Confirmation
                    | CommandMutationGuard::LocalMaintenance
            )
        });
        assert!(
            has_local_guard,
            "local mutating command `{}` must declare a dry-run, confirmation, or local-maintenance guard",
            entry.path
        );
    }
    Ok(())
}

#[sinex_test]
async fn command_catalog_backing_rpc_methods_are_known() -> xtask::sandbox::TestResult<()> {
    let rpc_catalog = method_catalog()
        .into_iter()
        .map(|method| (method.name, method))
        .collect::<std::collections::BTreeMap<_, _>>();

    for entry in command_catalog() {
        for method_name in entry.backing_rpc_methods {
            assert!(
                rpc_catalog.contains_key(method_name),
                "command `{}` references unknown RPC method `{method_name}`",
                entry.path
            );
        }
    }
    Ok(())
}

#[sinex_test]
async fn command_catalog_required_role_matches_backing_rpc_methods()
-> xtask::sandbox::TestResult<()> {
    let rpc_catalog = method_catalog()
        .into_iter()
        .map(|method| (method.name, method))
        .collect::<std::collections::BTreeMap<_, _>>();

    for entry in command_catalog() {
        if entry.backing_rpc_methods.is_empty() {
            assert_eq!(
                entry.required_rpc_role, None,
                "local command `{}` must not claim an RPC role",
                entry.path
            );
            continue;
        }

        let expected = entry
            .backing_rpc_methods
            .iter()
            .filter_map(|method_name| rpc_catalog.get(method_name))
            .map(|method| method.role)
            .max_by_key(|role| rpc_role_rank(*role));

        assert_eq!(
            entry.required_rpc_role, expected,
            "command `{}` must expose the maximum required backing RPC role",
            entry.path
        );
    }
    Ok(())
}

#[sinex_test]
async fn command_catalog_effect_matches_backing_rpc_mutability()
-> xtask::sandbox::TestResult<()> {
    let rpc_catalog = method_catalog()
        .into_iter()
        .map(|method| (method.name, method))
        .collect::<std::collections::BTreeMap<_, _>>();

    for entry in command_catalog() {
        if entry.backing_rpc_methods.is_empty() {
            continue;
        }

        let has_mutating_rpc = entry
            .backing_rpc_methods
            .iter()
            .filter_map(|method_name| rpc_catalog.get(method_name))
            .any(|method| method.mutability == RpcMutability::Mutating);

        if has_mutating_rpc {
            assert_eq!(
                entry.effect,
                CommandEffect::Mutating,
                "command `{}` must be mutating because at least one backing RPC mutates",
                entry.path
            );
        } else {
            assert_ne!(
                entry.effect,
                CommandEffect::Mutating,
                "command `{}` is marked mutating but all backing RPC methods are read-only",
                entry.path
            );
        }
    }
    Ok(())
}

#[sinex_test]
async fn command_modules_do_not_use_raw_rpc_escape_hatch() -> xtask::sandbox::TestResult<()> {
    let commands_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("commands");
    for entry in std::fs::read_dir(commands_dir)? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
            continue;
        }
        let body = std::fs::read_to_string(&path)?;
        assert!(
            !body.contains("call_raw_rpc"),
            "command module `{}` must use a typed GatewayClient method",
            path.display()
        );
    }
    Ok(())
}

#[sinex_test]
async fn validate_format_rejects_unknown_command() -> xtask::sandbox::TestResult<()> {
    let result = validate_format("nonexistent command", OutputFormat::Json);
    assert!(result.is_err(), "unknown commands should fail closed");
    assert!(
        result
            .unwrap_err()
            .contains("missing from the output-format registry"),
        "error should explain the missing registry entry"
    );
    Ok(())
}

#[sinex_test]
async fn registry_covers_canonical_key_commands() -> xtask::sandbox::TestResult<()> {
    let reg = build();
    let required = [
        "events query",
        "events explain",
        "events annotate",
        "events relations within",
        "events context",
        "runtime automata",
        "runtime gateway ping",
        "runtime gateway version",
        "runtime health",
        "runtime list",
        "ops replay plan",
        "ops replay watch",
        "ops dlq list",
        "ops verify",
        "ops demo",
    ];
    for cmd in required {
        assert!(reg.contains_key(cmd), "registry is missing `{cmd}`");
    }
    Ok(())
}

#[sinex_test]
async fn streaming_commands_are_marked() -> xtask::sandbox::TestResult<()> {
    let reg = build();
    assert!(
        reg["events watch"].streaming,
        "`events watch` must be marked streaming"
    );
    assert!(
        reg["ops replay watch"].streaming,
        "`ops replay watch` must be marked streaming"
    );
    Ok(())
}
