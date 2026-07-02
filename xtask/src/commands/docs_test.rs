use super::*;
use crate::command_catalog::{ArgInfo, CommandInfo};
use crate::command_docs::{render_command_guide, render_command_reference};
use crate::sandbox::sinex_test;

#[sinex_test]
async fn test_docs_command_metadata() -> ::xtask::sandbox::TestResult<()> {
    let cmd = DocsCommand {
        subcommand: DocsSubcommand::Build {
            package: vec![],
            open: false,
            private: false,
            all_features: false,
        },
    };

    let metadata = cmd.metadata();
    assert!(metadata.timeout.is_some());
    Ok(())
}

#[sinex_test]
async fn test_docs_command_name() -> ::xtask::sandbox::TestResult<()> {
    let cmd = DocsCommand {
        subcommand: DocsSubcommand::Serve {
            port: 8080,
            build: false,
        },
    };

    assert_eq!(cmd.name(), "docs");
    Ok(())
}

#[sinex_test]
async fn test_find_workspace_root_reports_manifest_read_failures()
-> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let manifest = temp.path().join("Cargo.toml");
    std::fs::create_dir(&manifest)?;

    let error = find_workspace_root(temp.path().to_path_buf()).unwrap_err();
    assert!(format!("{error:#}").contains("Failed to read workspace manifest"));
    Ok(())
}

#[sinex_test]
async fn test_find_workspace_root_finds_workspace_manifest() -> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    std::fs::write(
        temp.path().join("Cargo.toml"),
        "[workspace]\nmembers = []\n",
    )?;
    let nested = temp.path().join("nested/child");
    std::fs::create_dir_all(&nested)?;

    let workspace = find_workspace_root(nested)?;
    assert_eq!(workspace, temp.path());
    Ok(())
}

#[sinex_test]
async fn test_render_command_reference_renders_nested_sections() -> ::xtask::sandbox::TestResult<()>
{
    let rendered = render_command_reference(&[CommandInfo {
        name: "check".to_string(),
        about: Some("Compile verification".to_string()),
        args: vec![ArgInfo {
            name: "package".to_string(),
            short: Some('p'),
            long: Some("package".to_string()),
            help: Some("Check specific package(s) only".to_string()),
            required: false,
            global: false,
            possible_values: vec![],
            takes_value: true,
        }],
        subcommands: vec![CommandInfo {
            name: "deep".to_string(),
            about: Some("Nested sample".to_string()),
            args: vec![],
            subcommands: vec![],
        }],
    }]);

    assert!(rendered.contains("# xtask Command Reference"));
    assert!(rendered.contains("## `xtask check`"));
    assert!(rendered.contains("| `-p, --package` | yes | no | Check specific package(s) only |"));
    assert!(rendered.contains("### `xtask check deep`"));
    Ok(())
}

#[sinex_test]
async fn test_render_command_guide_renders_curated_sections() -> ::xtask::sandbox::TestResult<()> {
    let rendered = render_command_guide(&crate::command_catalog::collect_command_catalog());

    assert!(rendered.contains("# xtask Command Guide"));
    assert!(rendered.contains("## Agent Defaults"));
    assert!(rendered.contains("`xtask check`"));
    assert!(rendered.contains("`xtask fix --smart`"));
    assert!(!rendered.contains("xtask fix --check"));
    Ok(())
}

#[sinex_test]
async fn test_schema_bundle_major_version_parses_semver() -> ::xtask::sandbox::TestResult<()> {
    assert_eq!(
        sinex_primitives::events::schema_registry::schema_bundle_major_version("1.0.0").unwrap(),
        1
    );
    assert_eq!(
        sinex_primitives::events::schema_registry::schema_bundle_major_version("1").unwrap(),
        1
    );
    assert!(sinex_primitives::events::schema_registry::schema_bundle_major_version("").is_err());
    assert!(
        sinex_primitives::events::schema_registry::schema_bundle_major_version("x.0.0").is_err()
    );
    Ok(())
}

#[sinex_test]
async fn test_schema_bundle_content_hash_matches_registry_contract()
-> ::xtask::sandbox::TestResult<()> {
    let schema = serde_json::json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "properties": {
            "created_at": {
                "format": "date-time",
                "type": "string"
            },
            "path": {
                "type": "string"
            },
            "permissions": {
                "format": "uint32",
                "minimum": 0.0,
                "type": ["integer", "null"]
            },
            "size": {
                "format": "uint64",
                "minimum": 0.0,
                "type": "integer"
            }
        },
        "required": ["created_at", "path", "size"],
        "title": "FileCreatedPayload",
        "type": "object"
    });
    assert_eq!(
        sinex_primitives::events::schema_registry::calculate_schema_content_hash(
            "fs-watcher",
            "file.created",
            "1.0.0",
            &schema
        )
        .unwrap(),
        "dfed8161f597e83e0efaff7ed7efb56ea960fc51c00bb401bc06c154220dcaed"
    );
    Ok(())
}

#[sinex_test]
async fn test_render_ast_grep_catalog_renders_rule_details() -> ::xtask::sandbox::TestResult<()> {
    let rendered = render_ast_grep_catalog(&[
        AstGrepRuleCatalogEntry {
            id: "cargo-command-outside-process".to_string(),
            message: "Keep cargo spawning centralized".to_string(),
            severity: "error".to_string(),
            language: Some("rust".to_string()),
            note: Some("Use xtask::process helpers.".to_string()),
            ignores: Some(vec!["xtask/src/process.rs".to_string()]),
        },
        AstGrepRuleCatalogEntry {
            id: "string-from-literal".to_string(),
            message: "Prefer .to_string() or .into()".to_string(),
            severity: "hint".to_string(),
            language: Some("rust".to_string()),
            note: None,
            ignores: None,
        },
    ]);

    assert!(rendered.contains("# ast-grep Rule Catalog"));
    assert!(rendered.contains("`cargo-command-outside-process`"));
    assert!(rendered.contains("Within xtask automation, `error` severity is blocking"));
    assert!(rendered.contains("`xtask/src/process.rs`"));
    Ok(())
}
