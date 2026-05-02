use color_eyre::eyre::Result;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use crate::command::{CommandContext, CommandResult};

const SCENARIO_CATEGORIES: &[&str] = &[
    "source_material",
    "replay",
    "runtime",
    "node_adapter",
    "gateway",
    "schema",
    "command_contract",
    "deployment_boundary",
];
const SCENARIO_LANES: &[&str] = &["fast", "heavy", "soak", "vm"];

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub(crate) struct ScenarioCatalogEntry {
    pub(crate) id: String,
    pub(crate) test_name: String,
    pub(crate) package: Option<String>,
    pub(crate) path: String,
    pub(crate) category: String,
    pub(crate) lane: String,
    pub(crate) cost_tier: String,
    pub(crate) tags: Vec<String>,
    pub(crate) fixtures: Vec<String>,
    pub(crate) subject_refs: Vec<String>,
    pub(crate) claim_ids: Vec<String>,
}

#[derive(Debug, Clone, Default, serde::Serialize, PartialEq, Eq)]
pub(super) struct ScenarioSelection {
    pub(super) entries: Vec<ScenarioCatalogEntry>,
    pub(super) filter: Option<String>,
    pub(super) packages: Vec<String>,
}

pub(super) fn validate_scenario_filters(categories: &[String], lanes: &[String]) -> Result<()> {
    validate_scenario_filter_values("category", categories, SCENARIO_CATEGORIES)?;
    validate_scenario_filter_values("lane", lanes, SCENARIO_LANES)
}

fn normalize_filter_values(values: &[String]) -> Vec<String> {
    let mut normalized = values
        .iter()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    normalized.sort();
    normalized.dedup();
    normalized
}

fn validate_scenario_filter_values(name: &str, values: &[String], allowed: &[&str]) -> Result<()> {
    for value in normalize_filter_values(values) {
        if !allowed.iter().any(|candidate| *candidate == value) {
            return Err(color_eyre::eyre::eyre!(
                "invalid scenario {name} `{value}`; expected one of: {}",
                allowed.join(", ")
            ));
        }
    }
    Ok(())
}

pub(super) fn render_scenario_catalog(
    ctx: &CommandContext,
    entries: &[ScenarioCatalogEntry],
) -> Result<CommandResult> {
    if ctx.is_human() {
        for entry in entries {
            let package = entry.package.as_deref().unwrap_or("unknown-package");
            println!(
                "{} [{}:{}:{}] {} ({})",
                entry.test_name, entry.category, entry.lane, entry.cost_tier, entry.id, package
            );
        }
    }
    Ok(CommandResult::success()
        .with_detail(format!("{} scenario(s) discovered", entries.len()))
        .with_data(serde_json::json!({ "scenarios": entries })))
}

pub(super) fn select_scenarios(
    entries: Vec<ScenarioCatalogEntry>,
    tags: &[String],
    categories: &[String],
    lanes: &[String],
) -> Vec<ScenarioCatalogEntry> {
    let tags = normalize_filter_values(tags);
    let categories = normalize_filter_values(categories);
    let lanes = normalize_filter_values(lanes);

    entries
        .into_iter()
        .filter(|entry| {
            tags.iter().all(|tag| {
                entry.tags.iter().any(|candidate| candidate == tag)
                    || entry.id == *tag
                    || entry.claim_ids.iter().any(|claim| claim == tag)
            })
        })
        .filter(|entry| {
            categories.is_empty() || categories.iter().any(|value| value == &entry.category)
        })
        .filter(|entry| lanes.is_empty() || lanes.iter().any(|value| value == &entry.lane))
        .collect()
}

pub(super) fn scenario_packages(entries: &[ScenarioCatalogEntry]) -> Vec<String> {
    let mut packages = entries
        .iter()
        .filter_map(|entry| entry.package.clone())
        .collect::<Vec<_>>();
    packages.sort();
    packages.dedup();
    packages
}

pub(super) fn scenario_nextest_filter(entries: &[ScenarioCatalogEntry]) -> Option<String> {
    if entries.is_empty() {
        return None;
    }
    let names = entries
        .iter()
        .map(|entry| regex_literal(&entry.test_name))
        .collect::<Vec<_>>()
        .join("|");
    Some(format!("test(/({names})/)"))
}

pub(super) fn merge_nextest_filters(
    user_filter: Option<&str>,
    scenario_filter: Option<&str>,
) -> Option<String> {
    match (user_filter, scenario_filter) {
        (Some(user_filter), Some(scenario_filter)) => {
            Some(format!("({user_filter}) & {scenario_filter}"))
        }
        (Some(user_filter), None) => Some(user_filter.to_string()),
        (None, Some(scenario_filter)) => Some(scenario_filter.to_string()),
        (None, None) => None,
    }
}

fn regex_literal(input: &str) -> String {
    let mut escaped = String::with_capacity(input.len());
    for ch in input.chars() {
        if matches!(
            ch,
            '\\' | '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$'
        ) {
            escaped.push('\\');
        }
        escaped.push(ch);
    }
    escaped
}

pub(crate) fn discover_scenario_catalog(
    workspace_root: &Path,
) -> Result<Vec<ScenarioCatalogEntry>> {
    let mut entries = Vec::new();
    for root in ["crate", "tests", "xtask"] {
        let root = workspace_root.join(root);
        if !root.exists() {
            continue;
        }
        for entry in walkdir::WalkDir::new(&root)
            .into_iter()
            .filter_map(std::result::Result::ok)
        {
            if !entry.file_type().is_file()
                || entry.path().extension().and_then(|ext| ext.to_str()) != Some("rs")
            {
                continue;
            }
            let source = fs::read_to_string(entry.path())?;
            entries.extend(extract_scenario_catalog_entries(
                workspace_root,
                entry.path(),
                &source,
            )?);
        }
    }
    entries.sort_by(|left, right| {
        left.package
            .cmp(&right.package)
            .then_with(|| left.test_name.cmp(&right.test_name))
    });
    Ok(entries)
}

fn extract_scenario_catalog_entries(
    workspace_root: &Path,
    path: &Path,
    source: &str,
) -> Result<Vec<ScenarioCatalogEntry>> {
    let mut entries = Vec::new();
    let mut offset = 0usize;
    while let Some(relative_start) = source[offset..].find("#[sinex_test(") {
        let attr_start = offset + relative_start;
        let Some(attr_end) = find_attribute_end(source, attr_start) else {
            break;
        };
        let attr_text = &source[attr_start..=attr_end];
        if let Some(values) = parse_scenario_attr_values(attr_text) {
            let Some(test_name) = find_next_async_test_name(source, attr_end + 1) else {
                offset = attr_end + 1;
                continue;
            };
            let path = path.strip_prefix(workspace_root).unwrap_or(path);
            entries.push(ScenarioCatalogEntry {
                id: values.id,
                test_name,
                package: package_name_for_path(workspace_root, path),
                path: path.display().to_string(),
                category: values.category,
                lane: values.lane,
                cost_tier: values.cost_tier,
                tags: values.tags,
                fixtures: values.fixtures,
                subject_refs: values.subject_refs,
                claim_ids: values.claim_ids,
            });
        }
        offset = attr_end + 1;
    }
    Ok(entries)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedScenarioAttrs {
    id: String,
    category: String,
    lane: String,
    cost_tier: String,
    tags: Vec<String>,
    fixtures: Vec<String>,
    subject_refs: Vec<String>,
    claim_ids: Vec<String>,
}

fn parse_scenario_attr_values(attr_text: &str) -> Option<ParsedScenarioAttrs> {
    let values = parse_string_name_values(attr_text);
    let id = values.get("scenario")?.clone();
    let category = values.get("category")?.trim().to_ascii_lowercase();
    let lane = values.get("lane")?.trim().to_ascii_lowercase();
    let cost_tier = values
        .get("cost_tier")
        .map_or_else(|| lane.clone(), |value| value.trim().to_ascii_lowercase());
    Some(ParsedScenarioAttrs {
        id,
        category,
        lane,
        cost_tier,
        tags: split_csv(values.get("tags").map_or("", String::as_str)),
        fixtures: split_csv(values.get("fixtures").map_or("", String::as_str)),
        subject_refs: split_csv(values.get("subjects").map_or("", String::as_str)),
        claim_ids: split_csv(values.get("claims").map_or("", String::as_str)),
    })
}

fn split_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

fn parse_string_name_values(input: &str) -> BTreeMap<String, String> {
    let mut values = BTreeMap::new();
    let bytes = input.as_bytes();
    let mut idx = 0usize;
    while idx < bytes.len() {
        while idx < bytes.len() && !is_ident_start(bytes[idx]) {
            idx += 1;
        }
        let key_start = idx;
        while idx < bytes.len() && is_ident_continue(bytes[idx]) {
            idx += 1;
        }
        if key_start == idx {
            break;
        }
        let key = &input[key_start..idx];
        while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }
        if idx >= bytes.len() || bytes[idx] != b'=' {
            continue;
        }
        idx += 1;
        while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }
        if idx >= bytes.len() || bytes[idx] != b'"' {
            continue;
        }
        idx += 1;
        let mut value = String::new();
        while idx < bytes.len() {
            match bytes[idx] {
                b'\\' if idx + 1 < bytes.len() => {
                    idx += 1;
                    value.push(bytes[idx] as char);
                    idx += 1;
                }
                b'"' => {
                    idx += 1;
                    break;
                }
                byte => {
                    value.push(byte as char);
                    idx += 1;
                }
            }
        }
        values.insert(key.to_string(), value);
    }
    values
}

fn is_ident_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

fn is_ident_continue(byte: u8) -> bool {
    is_ident_start(byte) || byte.is_ascii_digit()
}

fn find_attribute_end(source: &str, attr_start: usize) -> Option<usize> {
    let bytes = source.as_bytes();
    let mut depth = 0usize;
    let mut idx = attr_start;
    while idx < bytes.len() {
        match bytes[idx] {
            b'[' => depth += 1,
            b']' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(idx);
                }
            }
            _ => {}
        }
        idx += 1;
    }
    None
}

fn find_next_async_test_name(source: &str, start: usize) -> Option<String> {
    let search_window = &source[start..source.len().min(start.saturating_add(2048))];
    let fn_start = search_window.find("async fn ")? + "async fn ".len();
    let name_start = start + fn_start;
    let name_end = source[name_start..]
        .find(|ch: char| !(ch == '_' || ch.is_ascii_alphanumeric()))
        .map_or(source.len(), |end| name_start + end);
    Some(source[name_start..name_end].to_string())
}

fn package_name_for_path(workspace_root: &Path, relative_path: &Path) -> Option<String> {
    let absolute_path = workspace_root.join(relative_path);
    let mut current = absolute_path.parent();
    while let Some(dir) = current {
        let manifest = dir.join("Cargo.toml");
        if manifest.exists()
            && let Ok(source) = fs::read_to_string(&manifest)
            && let Ok(value) = source.parse::<toml::Value>()
            && let Some(name) = value
                .get("package")
                .and_then(|package| package.get("name"))
                .and_then(toml::Value::as_str)
        {
            return Some(name.to_string());
        }
        if dir == workspace_root {
            break;
        }
        current = dir.parent();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;

    #[sinex_test]
    async fn scenario_catalog_extracts_sinex_test_metadata() -> ::xtask::sandbox::TestResult<()> {
        let root = tempfile::tempdir()?;
        let package_dir = root.path().join("crate/lib/example");
        std::fs::create_dir_all(package_dir.join("tests"))?;
        std::fs::write(
            package_dir.join("Cargo.toml"),
            r#"[package]
name = "sinex-example"
version = "0.1.0"
edition = "2024"
"#,
        )?;
        let path = package_dir.join("tests/scenario.rs");
        let source = r#"
#[sinex_test(
    timeout = 90,
    scenario = "runtime.restart-recovery.v1",
    category = "runtime",
    lane = "heavy",
    cost_tier = "integration",
    tags = "runtime,restart,recovery",
    fixtures = "postgres,nats,ingestd",
    subjects = "issue:324,node:runtime",
    claims = "restart-recovers,ledger-complete"
)]
async fn runtime_restart_scenario(ctx: TestContext) -> Result<()> {
    let _ = ctx;
    Ok(())
}
"#;
        std::fs::write(&path, source)?;

        let entries = extract_scenario_catalog_entries(root.path(), &path, source)?;

        assert_eq!(entries.len(), 1);
        let entry = &entries[0];
        assert_eq!(entry.package.as_deref(), Some("sinex-example"));
        assert_eq!(entry.test_name, "runtime_restart_scenario");
        assert_eq!(entry.id, "runtime.restart-recovery.v1");
        assert_eq!(entry.category, "runtime");
        assert_eq!(entry.lane, "heavy");
        assert_eq!(entry.cost_tier, "integration");
        assert_eq!(entry.tags, ["runtime", "restart", "recovery"]);
        assert_eq!(entry.fixtures, ["postgres", "nats", "ingestd"]);
        assert_eq!(entry.subject_refs, ["issue:324", "node:runtime"]);
        assert_eq!(entry.claim_ids, ["restart-recovers", "ledger-complete"]);
        Ok(())
    }

    #[sinex_test]
    async fn scenario_selection_builds_filter_and_package_scope() -> ::xtask::sandbox::TestResult<()>
    {
        let entries = vec![
            ScenarioCatalogEntry {
                id: "source-material.row-stream.v1".to_string(),
                test_name: "source_material_row_stream".to_string(),
                package: Some("sinex-node-sdk".to_string()),
                path: "crate/lib/sinex-node-sdk/tests/material_acquisition.rs".to_string(),
                category: "source_material".to_string(),
                lane: "fast".to_string(),
                cost_tier: "integration".to_string(),
                tags: vec!["source_material".to_string(), "row_stream".to_string()],
                fixtures: vec!["postgres".to_string()],
                subject_refs: vec!["issue:315".to_string()],
                claim_ids: vec!["stable-anchors".to_string()],
            },
            ScenarioCatalogEntry {
                id: "runtime.restart.v1".to_string(),
                test_name: "runtime_restart".to_string(),
                package: Some("sinex-node-sdk".to_string()),
                path: "crate/lib/sinex-node-sdk/tests/material_acquisition.rs".to_string(),
                category: "runtime".to_string(),
                lane: "heavy".to_string(),
                cost_tier: "integration".to_string(),
                tags: vec!["runtime".to_string(), "restart".to_string()],
                fixtures: vec!["nats".to_string()],
                subject_refs: vec!["issue:324".to_string()],
                claim_ids: vec!["restart-recovers".to_string()],
            },
        ];

        let selected = select_scenarios(entries, &["restart".into()], &["runtime".into()], &[]);

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].test_name, "runtime_restart");
        assert_eq!(scenario_packages(&selected), ["sinex-node-sdk"]);
        assert_eq!(
            scenario_nextest_filter(&selected).as_deref(),
            Some("test(/(runtime_restart)/)")
        );
        assert_eq!(
            merge_nextest_filters(
                Some("package(sinex-node-sdk)"),
                scenario_nextest_filter(&selected).as_deref()
            )
            .as_deref(),
            Some("(package(sinex-node-sdk)) & test(/(runtime_restart)/)")
        );
        Ok(())
    }

    #[sinex_test]
    async fn validate_scenario_filters_rejects_unknown_values() -> ::xtask::sandbox::TestResult<()>
    {
        let error = validate_scenario_filters(&["bogus".into()], &[])
            .expect_err("unknown category should be rejected");
        assert!(format!("{error:#}").contains("invalid scenario category"));

        let error = validate_scenario_filters(&[], &["expensive".into()])
            .expect_err("unknown lane should be rejected");
        assert!(format!("{error:#}").contains("invalid scenario lane"));
        Ok(())
    }
}
