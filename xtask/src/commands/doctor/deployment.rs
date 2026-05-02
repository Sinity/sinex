use super::{RECOMMENDED_INOTIFY_MAX_USER_WATCHES, workspace_tls_dir};
use crate::command::CommandContext;
use color_eyre::eyre::{Result, WrapErr, eyre};
use console::style;
use serde::Serialize;
use sinex_node_sdk::preflight::configuration::{
    validate_activitywatch_db, validate_terminal_history_source,
};
use sinex_node_sdk::preflight::services::{SystemdServiceDetails, inspect_systemd_service};
use sinex_primitives::{DeploymentReadinessDescriptor, DeploymentReadinessMode};
use std::path::{Path, PathBuf};
use std::process::Command;

mod database;
mod gateway;
mod nats;

#[cfg(test)]
pub(super) use database::resolve_database_probe_target;
pub(crate) use database::resolve_effective_database_probe_url;
pub(super) use database::{check_schema_apply, redact_database_url_password};
pub(crate) use gateway::check_gateway_ready;
#[cfg(test)]
pub(super) use gateway::{
    build_gateway_probe_client, interpret_gateway_ready_response, normalize_gateway_base_url,
    resolve_gateway_probe_tls_paths,
};
use nats::check_nats_streams;
#[cfg(test)]
pub(super) use nats::{
    apply_descriptor_nats_overrides, required_nats_stream_names, resolve_deployment_nats_config,
};

/// Result of a single deployment readiness check.
#[derive(Debug, Serialize)]
pub struct DeploymentReadinessItem {
    pub name: String,
    /// `"pass"`, `"fail"`, or `"skip"`
    pub status: String,
    pub description: String,
    #[serde(skip_serializing_if = "is_false")]
    pub blocking: bool,
}

#[derive(Debug, Serialize)]
pub struct DeploymentReadinessReport {
    pub items: Vec<DeploymentReadinessItem>,
    pub overall: bool,
}

#[derive(Debug, Clone)]
pub(super) struct TargetIdentity {
    pub(super) user: String,
    pub(super) uid: u32,
    pub(super) home: PathBuf,
}

impl DeploymentReadinessItem {
    pub(super) fn pass(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: "pass".into(),
            description: description.into(),
            blocking: true,
        }
    }

    pub(super) fn fail(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: "fail".into(),
            description: description.into(),
            blocking: true,
        }
    }

    pub(super) fn skip(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: "skip".into(),
            description: description.into(),
            blocking: false,
        }
    }

    pub(super) fn skip_blocking(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: "skip".into(),
            description: description.into(),
            blocking: true,
        }
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

pub(super) fn deployment_readiness_overall(items: &[DeploymentReadinessItem]) -> bool {
    let failed = items.iter().any(|item| item.status == "fail");
    let blocking_skipped = items
        .iter()
        .any(|item| item.status == "skip" && item.blocking);
    !failed && !blocking_skipped
}

fn env_truthy(key: &str) -> bool {
    std::env::var(key)
        .is_ok_and(|value| matches!(value.to_lowercase().as_str(), "1" | "true" | "yes" | "on"))
}

fn path_from_env_or_default(env_key: &str, default_path: PathBuf) -> Option<PathBuf> {
    std::env::var(env_key)
        .ok()
        .map(PathBuf::from)
        .or_else(|| default_path.exists().then_some(default_path))
}

fn descriptor_secret_path(
    descriptor: Option<&DeploymentReadinessDescriptor>,
    selector: impl FnOnce(&DeploymentReadinessDescriptor) -> Option<PathBuf>,
    env_key: &str,
    default_path: PathBuf,
) -> Option<PathBuf> {
    if let Some(descriptor) = descriptor {
        selector(descriptor)
    } else {
        path_from_env_or_default(env_key, default_path)
    }
}

fn load_deployment_descriptor() -> (
    Option<DeploymentReadinessDescriptor>,
    DeploymentReadinessItem,
) {
    let configured_path = DeploymentReadinessDescriptor::configured_path();
    match DeploymentReadinessDescriptor::load() {
        Ok(Some(descriptor)) => {
            let source =
                configured_path.unwrap_or_else(DeploymentReadinessDescriptor::default_path);
            let mode = match descriptor.mode {
                DeploymentReadinessMode::Prepared => "prepared",
                DeploymentReadinessMode::Enabled => "enabled",
                DeploymentReadinessMode::Unknown => "unknown",
            };
            let declared_source = descriptor
                .source
                .clone()
                .unwrap_or_else(|| "deployment descriptor".to_string());
            (
                Some(descriptor),
                DeploymentReadinessItem::pass(
                    "deployment-descriptor",
                    format!(
                        "Loaded {declared_source} ({mode} mode) from {}",
                        source.display()
                    ),
                ),
            )
        }
        Ok(None) => (
            None,
            DeploymentReadinessItem::fail(
                "deployment-descriptor",
                "No deployment readiness descriptor found; deployment readiness requires a config-derived descriptor from /etc/sinex/deployment-readiness.json or SINEX_DEPLOYMENT_READINESS_CONFIG",
            ),
        ),
        Err(error) => (
            None,
            DeploymentReadinessItem::fail("deployment-descriptor", error.to_string()),
        ),
    }
}

fn read_passwd_entry(username: &str) -> Result<Option<(u32, PathBuf)>> {
    let contents = match std::fs::read_to_string("/etc/passwd") {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).wrap_err("failed to read /etc/passwd"),
    };

    for line in contents.lines() {
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() < 7 || fields[0] != username {
            continue;
        }

        let uid = fields[2]
            .parse::<u32>()
            .wrap_err_with(|| format!("failed to parse UID for {username} from /etc/passwd"))?;
        return Ok(Some((uid, PathBuf::from(fields[5]))));
    }

    Ok(None)
}

fn command_output(command: &str, args: &[&str], description: &str) -> Result<String> {
    let output = Command::new(command)
        .args(args)
        .output()
        .wrap_err_with(|| {
            format!(
                "failed to run `{command} {}` for {description}",
                args.join(" ")
            )
        })?;
    if !output.status.success() {
        color_eyre::eyre::bail!(
            "`{command} {}` failed with status {} while resolving {description}",
            args.join(" "),
            output
                .status
                .code()
                .map_or_else(|| "signal".to_string(), |code| code.to_string())
        );
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

pub(super) fn resolve_target_identity(
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> Result<TargetIdentity> {
    let descriptor_target = descriptor.and_then(|value| value.target.as_ref());
    let env_target_user = std::env::var("SINEX_TARGET_USER")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let explicit_target_user = descriptor_target
        .map(|target| target.user.clone())
        .or_else(|| env_target_user.clone());

    if descriptor.is_some() && descriptor_target.is_none() && env_target_user.is_none() {
        color_eyre::eyre::bail!(
            "deployment descriptor is present but does not declare target.user; set SINEX_TARGET_USER or fix the descriptor"
        );
    }
    let Some(user) = explicit_target_user.clone() else {
        color_eyre::eyre::bail!(
            "deployment readiness refuses to guess the target user; set SINEX_TARGET_USER or provide a deployment descriptor with target.user"
        );
    };
    let passwd_entry = read_passwd_entry(&user)?;
    let explicit_uid = if let Some(uid) = descriptor_target.and_then(|target| target.uid) {
        Some(uid)
    } else if let Some(uid) = std::env::var("SINEX_TARGET_UID")
        .ok()
        .filter(|value| !value.trim().is_empty())
    {
        Some(
            uid.parse::<u32>()
                .wrap_err("failed to parse SINEX_TARGET_UID for deployment readiness")?,
        )
    } else {
        None
    };
    let explicit_home = descriptor_target
        .and_then(|target| target.home.clone())
        .or_else(|| {
            std::env::var("SINEX_TARGET_HOME")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .map(PathBuf::from)
        });

    if passwd_entry.is_none() && (explicit_uid.is_none() || explicit_home.is_none()) {
        color_eyre::eyre::bail!(
            "deployment target user '{user}' is missing from /etc/passwd; declare target.uid and target.home (or SINEX_TARGET_UID/SINEX_TARGET_HOME) explicitly"
        );
    }

    let uid = explicit_uid
        .or_else(|| passwd_entry.as_ref().map(|(uid, _)| *uid))
        .ok_or_else(|| {
            eyre!(
                "deployment target user '{user}' has no resolvable UID; declare target.uid explicitly"
            )
        })?;

    let home = explicit_home
        .or_else(|| passwd_entry.as_ref().map(|(_, home)| home.clone()))
        .ok_or_else(|| {
            eyre!(
                "deployment target user '{user}' has no resolvable home; declare target.home explicitly"
            )
        })?;

    Ok(TargetIdentity { user, uid, home })
}

fn terminal_source_candidates(
    target: &TargetIdentity,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> Vec<(String, PathBuf)> {
    if let Some(descriptor) = descriptor {
        return descriptor
            .terminal
            .history_sources
            .iter()
            .map(|source| (source.shell.clone(), source.path.clone()))
            .collect();
    }

    vec![
        ("bash".to_string(), target.home.join(".bash_history")),
        ("zsh".to_string(), target.home.join(".zsh_history")),
        (
            "atuin".to_string(),
            target.home.join(".local/share/atuin/history.db"),
        ),
    ]
}

fn activitywatch_db_for_target(
    target: &TargetIdentity,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> PathBuf {
    descriptor
        .and_then(|value| value.desktop.activitywatch_db_path.clone())
        .unwrap_or_else(|| {
            target
                .home
                .join(".local/share/activitywatch/aw-server-rust/sqlite.db")
        })
}

pub(super) fn runtime_dir_for_target(
    target: &TargetIdentity,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> Result<PathBuf> {
    if let Some(descriptor) = descriptor {
        return Ok(descriptor
            .desktop
            .runtime_dir
            .clone()
            .unwrap_or_else(|| PathBuf::from(format!("/run/user/{}", target.uid))));
    }

    if let Some(runtime_dir) = std::env::var("SINEX_HYPRLAND_RUNTIME_DIR")
        .ok()
        .map(PathBuf::from)
    {
        return Ok(runtime_dir);
    }

    let current_uid = current_process_uid()
        .wrap_err("failed to resolve current principal for Hyprland runtime selection")?;
    if current_uid == target.uid
        && let Some(runtime_dir) = std::env::var("XDG_RUNTIME_DIR").ok().map(PathBuf::from)
    {
        return Ok(runtime_dir);
    }

    Ok(PathBuf::from(format!("/run/user/{}", target.uid)))
}

fn configured_hyprland_sockets(
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> (Option<PathBuf>, Option<PathBuf>) {
    if let Some(descriptor) = descriptor {
        return (
            descriptor.desktop.hyprland_event_socket.clone(),
            descriptor.desktop.hyprland_command_socket.clone(),
        );
    }

    (
        std::env::var("SINEX_HYPRLAND_EVENT_SOCKET")
            .ok()
            .map(PathBuf::from),
        std::env::var("SINEX_HYPRLAND_COMMAND_SOCKET")
            .ok()
            .map(PathBuf::from),
    )
}

fn configured_hyprland_instance_signature(
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> Option<String> {
    if let Some(descriptor) = descriptor {
        return descriptor.desktop.hyprland_instance_signature.clone();
    }

    std::env::var("SINEX_HYPRLAND_INSTANCE_SIGNATURE")
        .ok()
        .or_else(|| std::env::var("HYPRLAND_INSTANCE_SIGNATURE").ok())
}

fn current_process_uid() -> Result<u32> {
    if let Some(uid) = std::env::var("UID")
        .ok()
        .filter(|value| !value.trim().is_empty())
    {
        return uid
            .parse::<u32>()
            .wrap_err("failed to parse UID environment variable for the current principal");
    }

    let uid = command_output("id", &["-u"], "current process UID")
        .wrap_err("failed to determine the current principal via `id -u`")?;
    uid.parse::<u32>()
        .wrap_err_with(|| format!("failed to parse `id -u` output as a UID: {uid}"))
}

pub(super) async fn check_node_entrypoints(
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> DeploymentReadinessItem {
    let Some(descriptor) = descriptor else {
        return DeploymentReadinessItem::fail(
            "node-entrypoints",
            "Deployment readiness requires a descriptor-declared managed unit set",
        );
    };
    let units = &descriptor.managed_units;

    if units.is_empty() {
        return match descriptor.mode {
            DeploymentReadinessMode::Prepared => DeploymentReadinessItem::skip(
                "node-entrypoints",
                "Prepared deployment descriptor does not declare any managed units yet",
            ),
            _ => DeploymentReadinessItem::fail(
                "node-entrypoints",
                "Deployment descriptor does not declare managed units",
            ),
        };
    }

    let mut unavailable = Vec::new();
    let mut notify_contract_violations = Vec::new();

    for unit in units {
        let service_data = match inspect_systemd_service(unit).await {
            Ok(service_data) => service_data,
            Err(error) => {
                return DeploymentReadinessItem::fail(
                    "node-entrypoints",
                    format!("Could not query systemd for {unit}: {error}"),
                );
            }
        };

        if !service_data.is_loaded() {
            unavailable.push(unit.clone());
            continue;
        }

        let contract_violations = service_data.notify_contract_violations();
        if !contract_violations.is_empty() {
            notify_contract_violations.push(format!("{unit} {}", contract_violations.join(", ")));
        }
    }

    if !unavailable.is_empty() {
        return DeploymentReadinessItem::fail(
            "node-entrypoints",
            format!(
                "Managed Sinex units are missing or not loaded: {}",
                unavailable.join(", ")
            ),
        );
    }

    if !notify_contract_violations.is_empty() {
        return DeploymentReadinessItem::fail(
            "node-entrypoints",
            format!(
                "Managed units violate the notify service contract: {}",
                notify_contract_violations.join(", ")
            ),
        );
    }

    DeploymentReadinessItem::pass(
        "node-entrypoints",
        format!(
            "Managed Sinex units are present in systemd with notify/watchdog contract intact: {}",
            units.join(", ")
        ),
    )
}

/// Check 2: /realm is readable by the resolved deployment principal.
pub(super) fn check_realm_accessible(target: &TargetIdentity) -> DeploymentReadinessItem {
    let realm = std::path::Path::new("/realm");
    if !realm.exists() {
        return DeploymentReadinessItem::fail("realm-accessible", "/realm does not exist");
    }

    let current_uid = match current_process_uid() {
        Ok(uid) => uid,
        Err(error) => {
            return DeploymentReadinessItem::skip_blocking(
                "realm-accessible",
                format!(
                    "Could not determine the current principal: {error:#}; rerun as {} or root to validate /realm access honestly",
                    target.user
                ),
            );
        }
    };

    if current_uid != target.uid && current_uid != 0 {
        return DeploymentReadinessItem::skip_blocking(
            "realm-accessible",
            format!(
                "Current principal uid {} differs from target uid {}; rerun as {} or root to validate /realm access",
                current_uid, target.uid, target.user
            ),
        );
    }

    match std::fs::read_dir(realm) {
        Ok(_) => DeploymentReadinessItem::pass(
            "realm-accessible",
            format!("/realm is readable for deployment target {}", target.user),
        ),
        Err(error) => DeploymentReadinessItem::fail(
            "realm-accessible",
            format!(
                "/realm exists but is not readable for deployment target {}: {error}",
                target.user
            ),
        ),
    }
}

/// Check 3: terminal history sources currently consumed by the node are readable.
pub(super) fn check_terminal_sources(
    target: &TargetIdentity,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> DeploymentReadinessItem {
    let terminal_enabled = descriptor.is_none_or(|value| value.terminal.surface.enabled);
    if !terminal_enabled {
        return DeploymentReadinessItem::skip(
            "terminal-sources",
            "Terminal ingestion is disabled in the deployment descriptor",
        );
    }

    let candidates = terminal_source_candidates(target, descriptor);
    if descriptor.is_some() && candidates.is_empty() {
        return DeploymentReadinessItem::fail(
            "terminal-sources",
            "Terminal ingestion is enabled in the deployment descriptor but terminal.history_sources is empty",
        );
    }

    let mut readable = Vec::new();
    let mut unreadable = Vec::new();

    for (label, path) in candidates {
        if !path.exists() {
            continue;
        }

        let check = validate_terminal_history_source(&label, &path);

        match check {
            Ok(()) => readable.push(format!("{label}:{}", path.display())),
            Err(error) => unreadable.push(format!("{label}:{} ({error})", path.display())),
        }
    }

    if !unreadable.is_empty() {
        DeploymentReadinessItem::fail(
            "terminal-sources",
            format!(
                "Unreadable target-user history sources for {}: {}",
                target.user,
                unreadable.join(", ")
            ),
        )
    } else if !readable.is_empty() {
        DeploymentReadinessItem::pass(
            "terminal-sources",
            format!(
                "Readable target-user history sources for {}: {}",
                target.user,
                readable.join(", ")
            ),
        )
    } else {
        DeploymentReadinessItem::fail(
            "terminal-sources",
            format!(
                "No readable terminal history sources found under {} for target user {}",
                target.home.display(),
                target.user
            ),
        )
    }
}

/// Check 4: Hyprland sockets exist under the resolved runtime directory.
pub(super) fn check_hyprland_socket(
    target: &TargetIdentity,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> DeploymentReadinessItem {
    let desktop_enabled = descriptor.is_none_or(|value| value.desktop.surface.enabled);
    if !desktop_enabled {
        return DeploymentReadinessItem::skip(
            "hyprland-socket",
            "Desktop ingestion is disabled in the deployment descriptor",
        );
    }

    let (configured_event_socket, configured_command_socket) =
        configured_hyprland_sockets(descriptor);
    if let Some(event_socket) = configured_event_socket {
        let command_socket = configured_command_socket;
        if event_socket.exists() {
            return DeploymentReadinessItem::pass(
                "hyprland-socket",
                format!(
                    "Configured Hyprland event socket {} is present (command socket present: {})",
                    event_socket.display(),
                    command_socket.as_ref().is_some_and(|path| path.exists())
                ),
            );
        }

        return DeploymentReadinessItem::fail(
            "hyprland-socket",
            format!(
                "Configured Hyprland event socket {} is missing",
                event_socket.display()
            ),
        );
    }

    let hypr_dir = match runtime_dir_for_target(target, descriptor) {
        Ok(runtime_dir) => runtime_dir.join("hypr"),
        Err(error) => {
            return DeploymentReadinessItem::fail("hyprland-socket", format!("{error:#}"));
        }
    };
    if !hypr_dir.exists() {
        return DeploymentReadinessItem::fail(
            "hyprland-socket",
            format!(
                "{} does not exist for target user {} (Hyprland runtime is unavailable)",
                hypr_dir.display(),
                target.user
            ),
        );
    }

    if let Some(signature) = configured_hyprland_instance_signature(descriptor) {
        let base = hypr_dir.join(&signature);
        let event_socket = base.join(".socket2.sock");
        let command_socket = base.join(".socket.sock");
        if event_socket.exists() {
            return DeploymentReadinessItem::pass(
                "hyprland-socket",
                format!(
                    "Resolved Hyprland sockets under {} (command socket present: {})",
                    base.display(),
                    command_socket.exists()
                ),
            );
        }

        return DeploymentReadinessItem::fail(
            "hyprland-socket",
            format!(
                "Configured Hyprland instance {} under {} is missing .socket2.sock",
                signature,
                hypr_dir.display()
            ),
        );
    }

    match std::fs::read_dir(&hypr_dir) {
        Ok(entries) => match collect_hyprland_socket_candidates(
            entries.map(|entry| entry.map(|value| value.path())),
        ) {
            Ok(candidates) => match candidates.as_slice() {
                [candidate] => DeploymentReadinessItem::pass(
                    "hyprland-socket",
                    format!("Found Hyprland event socket under {}", candidate.display()),
                ),
                [] => DeploymentReadinessItem::fail(
                    "hyprland-socket",
                    format!(
                        "{} exists but contains no Hyprland event sockets",
                        hypr_dir.display()
                    ),
                ),
                _ => DeploymentReadinessItem::fail(
                    "hyprland-socket",
                    format!(
                        "Multiple Hyprland instances found under {}; set SINEX_HYPRLAND_INSTANCE_SIGNATURE or SINEX_HYPRLAND_EVENT_SOCKET",
                        hypr_dir.display()
                    ),
                ),
            },
            Err(error) => DeploymentReadinessItem::fail(
                "hyprland-socket",
                format!("Could not inspect {}: {error}", hypr_dir.display()),
            ),
        },
        Err(e) => DeploymentReadinessItem::fail(
            "hyprland-socket",
            format!("Could not read {}: {e}", hypr_dir.display()),
        ),
    }
}

pub(super) fn collect_hyprland_socket_candidates<I>(entries: I) -> std::io::Result<Vec<PathBuf>>
where
    I: IntoIterator<Item = std::io::Result<PathBuf>>,
{
    let mut candidates = Vec::new();
    for entry in entries {
        let path = entry?;
        if path.join(".socket2.sock").exists() {
            candidates.push(path);
        }
    }
    Ok(candidates)
}

pub(super) fn check_activitywatch_db(
    target: &TargetIdentity,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> DeploymentReadinessItem {
    let desktop_enabled = descriptor.is_none_or(|value| value.desktop.surface.enabled);
    if !desktop_enabled {
        return DeploymentReadinessItem::skip(
            "activitywatch-db",
            "Desktop ingestion is disabled in the deployment descriptor",
        );
    }

    if descriptor.is_some()
        && descriptor
            .and_then(|value| value.desktop.activitywatch_db_path.as_ref())
            .is_none()
    {
        return DeploymentReadinessItem::fail(
            "activitywatch-db",
            "Desktop ingestion is enabled in the deployment descriptor but desktop.activitywatch_db_path is unset",
        );
    }

    let path = activitywatch_db_for_target(target, descriptor);
    if !path.exists() {
        return DeploymentReadinessItem::fail(
            "activitywatch-db",
            format!(
                "No ActivityWatch SQLite database found at {} for target user {}",
                path.display(),
                target.user
            ),
        );
    }

    match validate_activitywatch_db(&path) {
        Ok(()) => DeploymentReadinessItem::pass(
            "activitywatch-db",
            format!(
                "ActivityWatch SQLite history is readable at {} for target user {}",
                path.display(),
                target.user
            ),
        ),
        Err(error) => DeploymentReadinessItem::fail(
            "activitywatch-db",
            format!(
                "Unreadable ActivityWatch history for {} at {} ({error})",
                target.user,
                path.display()
            ),
        ),
    }
}

/// Check 5: git-annex is on PATH.
fn check_git_annex() -> DeploymentReadinessItem {
    match which::which("git-annex") {
        Ok(path) => DeploymentReadinessItem::pass(
            "git-annex",
            format!("git-annex found at {}", path.display()),
        ),
        Err(_) => DeploymentReadinessItem::fail("git-annex", "git-annex not found on PATH"),
    }
}

/// Check 6: inotify watch limit is high enough for real filesystem deployment.
fn check_inotify_limit(
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> DeploymentReadinessItem {
    if descriptor.is_some_and(|value| !value.filesystem.enabled) {
        return DeploymentReadinessItem::skip(
            "inotify-max-user-watches",
            "Filesystem ingestion is disabled in the deployment descriptor",
        );
    }

    let path = "/proc/sys/fs/inotify/max_user_watches";
    let contents = match std::fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) => {
            return DeploymentReadinessItem::fail(
                "inotify-max-user-watches",
                format!("Could not read {path}: {error}"),
            );
        }
    };

    let Ok(value) = contents.trim().parse::<u64>() else {
        return DeploymentReadinessItem::fail(
            "inotify-max-user-watches",
            format!("Could not parse {} as an integer", contents.trim()),
        );
    };

    if value >= RECOMMENDED_INOTIFY_MAX_USER_WATCHES {
        DeploymentReadinessItem::pass("inotify-max-user-watches", format!("Configured to {value}"))
    } else {
        DeploymentReadinessItem::fail(
            "inotify-max-user-watches",
            format!(
                "Configured to {value}; expected at least {RECOMMENDED_INOTIFY_MAX_USER_WATCHES}"
            ),
        )
    }
}

fn validate_document_root_path(path: &Path) -> Result<()> {
    let metadata = std::fs::metadata(path)
        .wrap_err_with(|| format!("failed to inspect document root {}", path.display()))?;

    if metadata.is_dir() {
        std::fs::read_dir(path)
            .map(|_| ())
            .wrap_err_with(|| format!("failed to enumerate document root {}", path.display()))
    } else if metadata.is_file() {
        std::fs::File::open(path)
            .map(|_| ())
            .wrap_err_with(|| format!("failed to read document root {}", path.display()))
    } else {
        Err(eyre!(
            "document root {} is neither a file nor a directory",
            path.display()
        ))
    }
}

pub(super) fn check_document_roots(
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> DeploymentReadinessItem {
    let Some(descriptor) = descriptor else {
        return DeploymentReadinessItem::skip(
            "document-roots",
            "No deployment descriptor available for document root validation",
        );
    };

    if !descriptor.document.surface.enabled {
        return DeploymentReadinessItem::skip(
            "document-roots",
            "Document ingestion is disabled in the deployment descriptor",
        );
    }

    if descriptor.document.allowed_roots.is_empty() {
        return DeploymentReadinessItem::fail(
            "document-roots",
            "Document ingestion is enabled but no allowed roots are declared",
        );
    }

    let mut readable = Vec::new();
    let mut unreadable = Vec::new();
    for path in &descriptor.document.allowed_roots {
        match validate_document_root_path(path) {
            Ok(()) => readable.push(path.display().to_string()),
            Err(error) => unreadable.push(format!("{} ({error:#})", path.display())),
        }
    }

    if unreadable.is_empty() {
        DeploymentReadinessItem::pass(
            "document-roots",
            format!("Readable document roots: {}", readable.join(", ")),
        )
    } else {
        DeploymentReadinessItem::fail(
            "document-roots",
            format!("Unreadable document roots: {}", unreadable.join(", ")),
        )
    }
}

async fn check_document_scan_units(
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> DeploymentReadinessItem {
    let scan_details = if let Some(unit) =
        descriptor.and_then(|deployment| deployment.document.scan_service_unit.as_deref())
    {
        Some(
            inspect_systemd_service(unit)
                .await
                .map_err(|error| error.to_string()),
        )
    } else {
        None
    };
    let timer_details = if let Some(unit) =
        descriptor.and_then(|deployment| deployment.document.timer_unit.as_deref())
    {
        Some(
            inspect_systemd_service(unit)
                .await
                .map_err(|error| error.to_string()),
        )
    } else {
        None
    };

    evaluate_document_scan_units(descriptor, scan_details, timer_details)
}

pub(super) fn evaluate_document_scan_units(
    descriptor: Option<&DeploymentReadinessDescriptor>,
    scan_details: Option<std::result::Result<SystemdServiceDetails, String>>,
    timer_details: Option<std::result::Result<SystemdServiceDetails, String>>,
) -> DeploymentReadinessItem {
    let Some(descriptor) = descriptor else {
        return DeploymentReadinessItem::skip(
            "document-scan-units",
            "No deployment descriptor available for document scan unit validation",
        );
    };

    if !descriptor.document.surface.enabled {
        return DeploymentReadinessItem::skip(
            "document-scan-units",
            "Document ingestion is disabled in the deployment descriptor",
        );
    }

    let Some(scan_service_unit) = descriptor.document.scan_service_unit.as_deref() else {
        return DeploymentReadinessItem::fail(
            "document-scan-units",
            "Document ingestion is enabled but no scan service unit is declared",
        );
    };

    let Some(scan_details) = scan_details else {
        return DeploymentReadinessItem::fail(
            "document-scan-units",
            format!(
                "Could not query systemd for {scan_service_unit}: no service details collected"
            ),
        );
    };

    let scan_details = match scan_details {
        Ok(details) => details,
        Err(error) => {
            return DeploymentReadinessItem::fail(
                "document-scan-units",
                format!("Could not query systemd for {scan_service_unit}: {error}"),
            );
        }
    };

    if !scan_details.is_loaded() {
        return DeploymentReadinessItem::fail(
            "document-scan-units",
            format!("Document scan service {scan_service_unit} is not loaded"),
        );
    }

    if scan_details.active_state == "failed" {
        return DeploymentReadinessItem::fail(
            "document-scan-units",
            format!(
                "Document scan service {scan_service_unit} is failed ({}/{})",
                scan_details.active_state, scan_details.sub_state
            ),
        );
    }

    let timer_declared = descriptor.document.timer_unit.as_deref();
    let timer_expected = descriptor.document.schedule.is_some();
    if timer_expected && timer_declared.is_none() {
        return DeploymentReadinessItem::fail(
            "document-scan-units",
            "Document ingestion declares a recurring schedule but no timer unit",
        );
    }
    if !timer_expected && timer_declared.is_some() {
        return DeploymentReadinessItem::fail(
            "document-scan-units",
            "Document ingestion declares a timer unit without a recurring schedule",
        );
    }

    let mut summary = vec![format!(
        "{scan_service_unit} loaded ({}/{})",
        scan_details.active_state, scan_details.sub_state
    )];

    if let Some(timer_unit) = timer_declared {
        let Some(timer_details) = timer_details else {
            return DeploymentReadinessItem::fail(
                "document-scan-units",
                format!("Could not query systemd for {timer_unit}: no timer details collected"),
            );
        };

        let timer_details = match timer_details {
            Ok(details) => details,
            Err(error) => {
                return DeploymentReadinessItem::fail(
                    "document-scan-units",
                    format!("Could not query systemd for {timer_unit}: {error}"),
                );
            }
        };

        if !timer_details.is_loaded() {
            return DeploymentReadinessItem::fail(
                "document-scan-units",
                format!("Document scan timer {timer_unit} is not loaded"),
            );
        }

        if !timer_details.is_active() {
            return DeploymentReadinessItem::fail(
                "document-scan-units",
                format!(
                    "Document scan timer {timer_unit} is not active ({}/{})",
                    timer_details.active_state, timer_details.sub_state
                ),
            );
        }

        summary.push(format!(
            "{timer_unit} active ({}/{})",
            timer_details.active_state, timer_details.sub_state
        ));
    } else {
        summary.push("no recurring timer configured".to_string());
    }

    DeploymentReadinessItem::pass("document-scan-units", summary.join("; "))
}

pub(super) fn check_singleton_workstation_topology(
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> DeploymentReadinessItem {
    let Some(descriptor) = descriptor else {
        return DeploymentReadinessItem::skip(
            "singleton-workstation-topology",
            "No deployment descriptor available for planned instance validation",
        );
    };

    if descriptor.mode == DeploymentReadinessMode::Prepared && descriptor.target.is_none() {
        return DeploymentReadinessItem::skip(
            "singleton-workstation-topology",
            "Prepared descriptor does not declare a workstation target yet; singleton defaults are not expected until target wiring exists",
        );
    }

    let surfaces = [
        ("filesystem", &descriptor.filesystem),
        ("terminal", &descriptor.terminal.surface),
        ("desktop", &descriptor.desktop.surface),
        ("system", &descriptor.system),
    ];
    let mut offenders = Vec::new();

    for (name, surface) in surfaces {
        let instances = surface.instances.unwrap_or(1);
        if surface.enabled && instances != 1 {
            offenders.push(format!("{name}={instances}"));
        }
    }

    if offenders.is_empty() {
        DeploymentReadinessItem::pass(
            "singleton-workstation-topology",
            "Workstation capture nodes are pinned to single-instance startup",
        )
    } else {
        DeploymentReadinessItem::fail(
            "singleton-workstation-topology",
            format!(
                "Workstation capture nodes must stay singleton for first enable: {}",
                offenders.join(", ")
            ),
        )
    }
}

/// Check 7: schema-apply readiness — connect to DB and run a simple query.

fn record_secret_file(
    label: &str,
    path: &Path,
    present: &mut Vec<String>,
    missing: &mut Vec<String>,
) {
    if path.is_file() {
        present.push(format!("{label}={}", path.display()));
    } else {
        missing.push(format!("{label} unreadable: {}", path.display()));
    }
}

fn record_secret_pair(
    label: &str,
    first: Option<&PathBuf>,
    first_label: &str,
    second: Option<&PathBuf>,
    second_label: &str,
    present: &mut Vec<String>,
    missing: &mut Vec<String>,
) {
    match (first, second) {
        (Some(first), Some(second)) if first.is_file() && second.is_file() => {
            present.push(format!("{label}={}/{}", first.display(), second.display()));
        }
        (Some(first), Some(second)) => missing.push(format!(
            "{label} unreadable: {first_label}={} {second_label}={}",
            first.display(),
            second.display()
        )),
        (Some(first), None) => {
            missing.push(format!(
                "{label} missing {second_label} for {first_label} {}",
                first.display()
            ));
        }
        (None, Some(second)) => {
            missing.push(format!(
                "{label} missing {first_label} for {second_label} {}",
                second.display()
            ));
        }
        (None, None) => {}
    }
}

pub(super) fn check_secret_materials(
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> DeploymentReadinessItem {
    let default_tls_dir = workspace_tls_dir();
    let descriptor_present = descriptor.is_some();
    let admin_token = descriptor_secret_path(
        descriptor,
        |value| value.secrets.gateway_admin_token_file.clone(),
        "SINEX_GATEWAY_ADMIN_TOKEN_FILE",
        PathBuf::from("/run/agenix/sinex-gateway-admin-token"),
    );
    let db_password = descriptor_secret_path(
        descriptor,
        |value| value.secrets.database_password_file.clone(),
        "SINEX_DATABASE_PASSWORD_FILE",
        PathBuf::from("/run/agenix/sinex-local-db"),
    );
    let gateway_cert = descriptor_secret_path(
        descriptor,
        |value| value.secrets.gateway_tls_cert_file.clone(),
        "SINEX_GATEWAY_TLS_CERT",
        default_tls_dir.join("server.pem"),
    );
    let gateway_key = descriptor_secret_path(
        descriptor,
        |value| value.secrets.gateway_tls_key_file.clone(),
        "SINEX_GATEWAY_TLS_KEY",
        default_tls_dir.join("server-key.pem"),
    );
    let gateway_trust_anchor = descriptor_secret_path(
        descriptor,
        |value| value.secrets.gateway_tls_trust_anchor_file.clone(),
        "SINEX_RPC_CA_CERT",
        default_tls_dir.join("ca.pem"),
    );
    let gateway_client_ca = descriptor_secret_path(
        descriptor,
        |value| value.secrets.gateway_tls_client_ca_file.clone(),
        "SINEX_GATEWAY_TLS_CLIENT_CA",
        default_tls_dir.join("ca.pem"),
    );
    let nats_ca = descriptor_secret_path(
        descriptor,
        |value| value.secrets.nats_ca_cert_file.clone(),
        "SINEX_NATS_CA_CERT",
        PathBuf::from("/run/agenix/sinex-nats-ca"),
    );
    let nats_client_cert = descriptor_secret_path(
        descriptor,
        |value| value.secrets.nats_client_cert_file.clone(),
        "SINEX_NATS_CLIENT_CERT",
        PathBuf::from("/run/agenix/sinex-nats-client-cert"),
    );
    let nats_client_key = descriptor_secret_path(
        descriptor,
        |value| value.secrets.nats_client_key_file.clone(),
        "SINEX_NATS_CLIENT_KEY",
        PathBuf::from("/run/agenix/sinex-nats-client-key"),
    );
    let nats_token = descriptor_secret_path(
        descriptor,
        |value| value.secrets.nats_token_file.clone(),
        "SINEX_NATS_TOKEN_FILE",
        PathBuf::from("/run/agenix/sinex-nats-token"),
    );
    let nats_creds = descriptor_secret_path(
        descriptor,
        |value| value.secrets.nats_creds_file.clone(),
        "SINEX_NATS_CREDS_FILE",
        PathBuf::from("/run/agenix/sinex-nats-client-creds"),
    );
    let nats_nkey = descriptor_secret_path(
        descriptor,
        |value| value.secrets.nats_nkey_seed_file.clone(),
        "SINEX_NATS_NKEY_SEED_FILE",
        PathBuf::from("/run/agenix/sinex-nats-client-nkey"),
    );

    let mtls_expected = descriptor.map_or_else(
        || {
            env_truthy("SINEX_GATEWAY_REQUIRE_CLIENT_TLS")
                || std::env::var("SINEX_GATEWAY_TLS_CLIENT_CA").is_ok()
        },
        |value| {
            value.gateway.require_client_tls || value.secrets.gateway_tls_client_ca_file.is_some()
        },
    );
    let database_password_expected = descriptor.map_or(!descriptor_present, |value| {
        value.database.password_required
    });

    let mut missing = Vec::new();
    let mut present = Vec::new();

    if let Some(path) = admin_token {
        record_secret_file("gateway-admin-token", &path, &mut present, &mut missing);
    } else if !descriptor_present {
        missing.push(
            "gateway-admin-token missing (set SINEX_GATEWAY_ADMIN_TOKEN_FILE or provide /run/agenix/sinex-gateway-admin-token)"
                .to_string(),
        );
    }

    if let Some(path) = db_password {
        record_secret_file("database-password", &path, &mut present, &mut missing);
    } else if database_password_expected {
        missing.push(
            "database-password missing (set SINEX_DATABASE_PASSWORD_FILE or provide /run/agenix/sinex-local-db)"
                .to_string(),
        );
    }

    record_secret_pair(
        "gateway-tls",
        gateway_cert.as_ref(),
        "cert",
        gateway_key.as_ref(),
        "key",
        &mut present,
        &mut missing,
    );
    if gateway_cert.is_none() && gateway_key.is_none() && !descriptor_present {
        missing.push(
            "gateway-tls missing (set SINEX_GATEWAY_TLS_CERT/SINEX_GATEWAY_TLS_KEY or provide .sinex/tls/server.pem + server-key.pem)"
                .to_string(),
        );
    }

    if mtls_expected {
        match gateway_client_ca {
            Some(path) => record_secret_file("gateway-client-ca", &path, &mut present, &mut missing),
            None => missing.push(
                "gateway-client-ca missing (set SINEX_GATEWAY_TLS_CLIENT_CA or provide .sinex/tls/ca.pem)"
                    .to_string(),
            ),
        }
    }

    if let Some(path) = gateway_trust_anchor
        && gateway_cert.as_ref() != Some(&path)
    {
        record_secret_file("gateway-trust-anchor", &path, &mut present, &mut missing);
    }

    if let Some(path) = nats_ca {
        record_secret_file("nats-ca", &path, &mut present, &mut missing);
    }

    record_secret_pair(
        "nats-client-mtls",
        nats_client_cert.as_ref(),
        "cert",
        nats_client_key.as_ref(),
        "key",
        &mut present,
        &mut missing,
    );

    let nats_auth_candidates = [nats_token, nats_creds, nats_nkey];
    let declared_nats_auth = nats_auth_candidates
        .iter()
        .filter(|path| path.is_some())
        .count();
    if declared_nats_auth > 1 {
        missing
            .push("NATS auth is ambiguous; declare only one of token, creds, or nkey".to_string());
    } else {
        for (label, path) in [
            ("nats-token", nats_auth_candidates[0].as_ref()),
            ("nats-creds", nats_auth_candidates[1].as_ref()),
            ("nats-nkey", nats_auth_candidates[2].as_ref()),
        ] {
            if let Some(path) = path {
                record_secret_file(label, path, &mut present, &mut missing);
            }
        }
    }

    if missing.is_empty() && present.is_empty() {
        DeploymentReadinessItem::skip(
            "secret-materials",
            "No deployment secret materials were declared for readiness validation",
        )
    } else if missing.is_empty() {
        DeploymentReadinessItem::pass(
            "secret-materials",
            format!("Deployment secret files available: {}", present.join(", ")),
        )
    } else {
        let description = if present.is_empty() {
            missing.join("; ")
        } else {
            format!("{}; present: {}", missing.join("; "), present.join(", "))
        };
        DeploymentReadinessItem::fail("secret-materials", description)
    }
}

pub(super) async fn execute_deployment_readiness(
    ctx: &CommandContext,
) -> Result<DeploymentReadinessReport> {
    let cfg = crate::config::config();
    let (descriptor, descriptor_item) = load_deployment_descriptor();

    let mut items = vec![
        descriptor_item,
        check_node_entrypoints(descriptor.as_ref()).await,
    ];

    match resolve_target_identity(descriptor.as_ref()) {
        Ok(target) => {
            let descriptor_suffix = descriptor
                .as_ref()
                .and_then(|value| value.source.as_deref())
                .map(|source| format!(" via {source}"))
                .unwrap_or_default();
            items.push(DeploymentReadinessItem::pass(
                "target-identity",
                format!(
                    "Using target user {} (uid {}, home {}) for terminal/desktop checks{}",
                    target.user,
                    target.uid,
                    target.home.display(),
                    descriptor_suffix
                ),
            ));
            items.push(check_realm_accessible(&target));
            items.push(check_terminal_sources(&target, descriptor.as_ref()));
            items.push(check_hyprland_socket(&target, descriptor.as_ref()));
            items.push(check_activitywatch_db(&target, descriptor.as_ref()));
        }
        Err(error) => {
            items.push(DeploymentReadinessItem::fail(
                "target-identity",
                format!("Could not resolve deployment target identity: {error}"),
            ));
            items.push(DeploymentReadinessItem::skip(
                "realm-accessible",
                "Skipped because target identity resolution failed",
            ));
            items.push(DeploymentReadinessItem::skip(
                "terminal-sources",
                "Skipped because target identity resolution failed",
            ));
            items.push(DeploymentReadinessItem::skip(
                "hyprland-socket",
                "Skipped because target identity resolution failed",
            ));
            items.push(DeploymentReadinessItem::skip(
                "activitywatch-db",
                "Skipped because target identity resolution failed",
            ));
        }
    }

    items.push(check_git_annex());
    items.push(check_singleton_workstation_topology(descriptor.as_ref()));
    items.push(check_inotify_limit(descriptor.as_ref()));
    items.push(check_document_roots(descriptor.as_ref()));
    items.push(check_document_scan_units(descriptor.as_ref()).await);
    items.push(check_secret_materials(descriptor.as_ref()));
    items.push(check_schema_apply(cfg.database_url.as_deref(), descriptor.as_ref()).await);
    items.push(check_nats_streams(cfg.nats_url.as_deref(), descriptor.as_ref()).await);
    items.push(check_gateway_ready(cfg.gateway_url.as_deref(), descriptor.as_ref()).await);

    let failed = items.iter().any(|item| item.status == "fail");
    let blocking_skipped = items
        .iter()
        .any(|item| item.status == "skip" && item.blocking);
    let overall_pass = deployment_readiness_overall(&items);

    if ctx.is_human() {
        println!("\n{}", style("Deployment Readiness:").bold());
        for item in &items {
            let (icon, styled_status) = match item.status.as_str() {
                "pass" => (style("✓").green(), style("PASS").green()),
                "fail" => (style("✗").red(), style("FAIL").red()),
                "skip" if item.blocking => (style("!").yellow(), style("SKIP*").yellow()),
                _ => (style("–").dim(), style("SKIP").dim()),
            };
            println!(
                "  {} [{styled_status}] {:<25} {}",
                icon,
                item.name,
                style(&item.description).dim()
            );
        }
        println!();
        if overall_pass {
            println!(
                "{}",
                style("✓ Deployment readiness: all blocking checks passed").green()
            );
        } else if blocking_skipped && !failed {
            println!(
                "{}",
                style("! Deployment readiness: required checks were skipped").yellow()
            );
        } else {
            println!(
                "{}",
                style("✗ Deployment readiness: some checks failed").red()
            );
        }
    }

    Ok(DeploymentReadinessReport {
        items,
        overall: overall_pass,
    })
}
