//! Read-only discovery from application locations and the host's declared inputs.
use crate::sources;
use serde_json::json;
pub use sinex_source_core::discovery::*;
use sinex_source_core::{Binding, InputFormat, Settings};
use std::path::{Path, PathBuf};

#[cfg(not(feature = "full"))]
fn catalog_sources() -> &'static Vec<serde_json::Value> {
    static CATALOG: std::sync::OnceLock<Vec<serde_json::Value>> = std::sync::OnceLock::new();
    CATALOG.get_or_init(|| {
        let catalog: serde_json::Value =
            serde_json::from_str(include_str!("../../../nix/catalog.json"))
                .expect("generated catalog");
        catalog["sources"]
            .as_array()
            .expect("catalog sources")
            .clone()
    })
}

fn known_source(name: &str) -> bool {
    if sources::spec(name).is_some() {
        return true;
    }
    #[cfg(not(feature = "full"))]
    return catalog_sources()
        .iter()
        .any(|source| source["name"] == name);
    #[cfg(feature = "full")]
    false
}
fn known_format(name: &str, source: Option<&str>) -> bool {
    if sources::format(name).is_some_and(|format| source.is_none_or(|source| format.reads(source)))
    {
        return true;
    }
    #[cfg(not(feature = "full"))]
    return catalog_sources().iter().any(|entry| {
        source.is_none_or(|source| entry["name"] == source)
            && entry["formats"]
                .as_array()
                .is_some_and(|formats| formats.iter().any(|format| format == name))
    });
    #[cfg(feature = "full")]
    false
}

pub fn validate(context: &DiscoveryContext) -> Result<(), String> {
    for (source, settings) in &context.sources {
        if !known_source(source) {
            return Err(format!("{source}: no such source"));
        }
        if let Some(name) = &settings.format
            && !known_format(name, Some(source))
        {
            return Err(format!("{source}: format {name} does not read it"));
        }
    }
    for (name, exclusion) in &context.exclude {
        if !known_source(name) {
            return Err(format!("exclude {name}: no such source"));
        }
        if exclusion
            .as_ref()
            .is_some_and(|e| e.reason.trim().is_empty())
        {
            return Err(format!(
                "exclude {name}: reason must contain non-whitespace text"
            ));
        }
    }
    for name in context.inputs.keys() {
        if !known_format(name, None)
            && !sources::formats().iter().any(|format| {
                format
                    .discovery
                    .locations
                    .iter()
                    .any(|location| location.root == "capture" && location.path == name)
            })
        {
            return Err(format!("input format {name}: no such format"));
        }
    }
    Ok(())
}

pub fn discover(context: &DiscoveryContext) -> Result<Vec<DiscoveredInput>, String> {
    validate(context)?;
    Ok(sources::formats()
        .iter()
        .flat_map(|format| (format.discover)(format, context))
        .collect())
}

/// Available files are opened for reading; no input is created or changed.
fn availability(path: &Path) -> (Availability, Option<String>) {
    let result = std::fs::metadata(path).and_then(|metadata| {
        if metadata.is_dir() {
            std::fs::read_dir(path).map(|_| ())
        } else {
            std::fs::File::open(path).map(|_| ())
        }
    });
    match result {
        Ok(()) => (Availability::Reading, None),
        Err(error) => (
            if error.kind() == std::io::ErrorKind::NotFound {
                Availability::WaitingForInput
            } else {
                Availability::Unavailable
            },
            Some(format!("{}: {error}", path.display())),
        ),
    }
}

fn paths(location: &Location, context: &DiscoveryContext) -> Result<Vec<PathBuf>, String> {
    let joined =
        |root: &Option<PathBuf>| root.iter().map(|root| root.join(location.path)).collect();
    Ok(match location.root {
        "home" => joined(&context.home),
        "data" => joined(&context.data_home),
        "config" => joined(&context.config_home),
        "state" => joined(&context.state_home),
        "runtime" => joined(&context.runtime_dir),
        "environment" => context
            .environment
            .get(location.path)
            .map(PathBuf::from)
            .into_iter()
            .collect(),
        "input" => context
            .inputs
            .get(location.path)
            .cloned()
            .unwrap_or_default(),
        "subject" => context
            .subject_roots
            .get(location.path)
            .cloned()
            .unwrap_or_default(),
        "spool" => vec![context.spool.join(location.path)],
        "capture" => {
            let mut out = context
                .inputs
                .get(location.path)
                .cloned()
                .unwrap_or_default();
            if let Some(layout) = &context.layout_file {
                let bytes = std::fs::read(layout)
                    .map_err(|error| format!("{}: {error}", layout.display()))?;
                let layout: serde_json::Value = serde_json::from_slice(&bytes)
                    .map_err(|error| format!("{}: {error}", layout.display()))?;
                if let Some(lane) = layout
                    .get("activity_lanes")
                    .and_then(|lanes| lanes.get(location.path))
                    .and_then(serde_json::Value::as_str)
                {
                    out.extend(
                        context
                            .subject_roots
                            .get("realm")
                            .into_iter()
                            .flatten()
                            .map(|root| root.join("activity").join(lane)),
                    );
                }
            }
            out
        }
        "projects" => {
            let mut out = Vec::new();
            for root in context.subject_roots.get("projects").into_iter().flatten() {
                match std::fs::read_dir(root) {
                    Ok(entries) => {
                        for entry in entries {
                            let path = entry
                                .map_err(|error| error.to_string())?
                                .path()
                                .join("repo");
                            if path.join(".git").exists() {
                                out.push(path);
                            }
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(format!("{}: {error}", root.display())),
                }
            }
            out
        }
        _ => Vec::new(),
    })
}

/// A format owns its locations and planner. Shared source names retain all formats.
pub fn discover_format(format: &InputFormat, context: &DiscoveryContext) -> Vec<DiscoveredInput> {
    let mut out = Vec::new();
    for spec in (format.specs)() {
        let configured = context.sources.get(spec.name).cloned().unwrap_or_default();
        let settings = Settings {
            host: context.host.clone(),
            class: configured.class,
            zone: configured.zone.clone(),
            options: configured.options.clone(),
        };
        let make = |input, availability, reason, bindings| DiscoveredInput {
            format: format.name.into(),
            source: spec.name.into(),
            input,
            settings: settings.clone(),
            availability,
            reason,
            bindings,
        };
        if let Some(Some(exclusion)) = context.exclude.get(spec.name) {
            out.push(make(
                None,
                Availability::Excluded,
                Some(exclusion.reason.clone()),
                vec![],
            ));
            continue;
        }
        if format.discovery.request_only || spec.name.ends_with(".request") {
            out.push(make(None, Availability::RequestOnly, None, vec![]));
            continue;
        }
        let explicit = configured
            .format
            .as_deref()
            .is_none_or(|name| name == format.name);
        let mut inputs: Vec<PathBuf> = if explicit {
            configured.paths.iter().map(PathBuf::from).collect()
        } else {
            vec![]
        };
        inputs.extend(
            context
                .inputs
                .get(format.name)
                .into_iter()
                .flatten()
                .cloned(),
        );
        for location in format.discovery.locations {
            match paths(location, context) {
                Ok(paths) => inputs.extend(paths),
                Err(error) => out.push(make(None, Availability::Unavailable, Some(error), vec![])),
            }
        }
        let mut seen = std::collections::BTreeSet::new();
        inputs.retain(|input| seen.insert(input.clone()));
        for input in inputs {
            let (mut state, mut reason) = availability(&input);
            let mut bindings = Vec::new();
            if state == Availability::Reading {
                match format.plan.map(|plan| plan(&input, &settings)) {
                    Some(Ok(planned)) => {
                        bindings = planned
                            .into_iter()
                            .filter(|b| b.source == spec.name)
                            .collect()
                    }
                    Some(Err(error)) => {
                        state = Availability::Unavailable;
                        reason = Some(error);
                    }
                    None => {
                        state = Availability::RequestOnly;
                    }
                }
            }
            out.push(make(Some(input), state, reason, bindings));
        }
        if format.discovery.owner == Owner::Session {
            let captures = captures(context, Owner::Session).unwrap_or_default();
            for (capture_format, binding) in
                captures.into_iter().filter(|(capture_format, binding)| {
                    capture_format.name == format.name && binding.source == spec.name
                })
            {
                out.push(make(
                    Some(binding.input.clone()),
                    Availability::Capturing,
                    None,
                    vec![binding],
                ));
                let _ = capture_format;
            }
        }
        if out.iter().all(|input| input.source != spec.name) {
            out.push(make(
                None,
                Availability::WaitingForInput,
                Some("no declared input is present".into()),
                vec![],
            ));
        }
    }
    out
}

fn executable(context: &DiscoveryContext, name: &str) -> bool {
    use std::os::unix::fs::PermissionsExt;
    context.environment.get("PATH").is_some_and(|path| {
        std::env::split_paths(path).any(|root| {
            std::fs::metadata(root.join(name))
                .is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
        })
    })
}

fn capability(context: &DiscoveryContext, name: &str) -> bool {
    let variable = |name: &str| {
        context
            .environment
            .get(name)
            .filter(|value| !value.is_empty())
    };
    match name {
        "runtime" => context
            .runtime_dir
            .as_ref()
            .is_some_and(|path| path.is_dir()),
        "wayland" => variable("WAYLAND_DISPLAY").is_some_and(|display| {
            let path = Path::new(display);
            if path.is_absolute() {
                path.exists()
            } else {
                context
                    .runtime_dir
                    .as_ref()
                    .is_some_and(|root| root.join(path).exists())
            }
        }),
        "session_bus" => variable("DBUS_SESSION_BUS_ADDRESS").is_some_and(|address| {
            address.split(';').any(|address| {
                address
                    .strip_prefix("unix:path=")
                    .is_some_and(|path| Path::new(path.split(',').next().unwrap_or(path)).exists())
            })
        }),
        "hyprland" => variable("HYPRLAND_INSTANCE_SIGNATURE").is_some_and(|instance| {
            context.runtime_dir.as_ref().is_some_and(|root| {
                root.join("hypr")
                    .join(instance)
                    .join(".socket2.sock")
                    .exists()
            })
        }),
        "pipewire" => context
            .runtime_dir
            .as_ref()
            .is_some_and(|root| root.join("pipewire-0").exists()),
        "watch" => context.watch.iter().any(|root| root.is_dir()),
        command => executable(context, command),
    }
}

/// The same spool names as explicit session selections, so positions survive discovery.
pub fn captures(
    context: &DiscoveryContext,
    owner: Owner,
) -> Result<Vec<(&'static InputFormat, Binding)>, String> {
    validate(context)?;
    let mut out = Vec::new();
    for format in sources::formats()
        .iter()
        .copied()
        .filter(|format| format.capture.is_some() && format.discovery.owner == owner)
    {
        let spec = (format.specs)()[0];
        if context.exclude.get(spec.name).is_some_and(Option::is_some) {
            continue;
        }
        if !format
            .discovery
            .capabilities
            .iter()
            .all(|name| capability(context, name))
        {
            continue;
        }
        for stream in format.discovery.streams {
            if !stream
                .capabilities
                .iter()
                .all(|name| capability(context, name))
            {
                continue;
            }
            let mut options: serde_json::Value =
                serde_json::from_str(stream.options).expect("declared capture options");
            if stream.capabilities.contains(&"watch") {
                options["roots"] = json!(context.watch);
            }
            if let Some(configured) = context.sources.get(spec.name) {
                if let Some(configured) = configured.options.as_object() {
                    if !options.is_object() {
                        options = json!({});
                    }
                    options.as_object_mut().unwrap().extend(configured.clone());
                }
            }
            let (format, mut binding) = crate::live::binding(
                format.name,
                &context.spool.join(stream.spool),
                &context.host,
                options,
            )?;
            binding.settings.class = context
                .sources
                .get(spec.name)
                .and_then(|source| source.class);
            out.push((format, binding));
        }
    }
    Ok(out)
}

fn suffix(binding: &Binding) -> Option<&str> {
    binding
        .name
        .strip_prefix(&format!("{}:{}", binding.source, binding.input.display()))
}
fn same_identity(left: &Binding, right: &Binding) -> bool {
    left.source == right.source
        && left.parser == right.parser
        && left.settings.host == right.settings.host
        && suffix(left) == suffix(right)
}
fn same_input(left: &Path, right: &Path) -> bool {
    left == right
        || std::fs::canonicalize(left)
            .ok()
            .zip(std::fs::canonicalize(right).ok())
            .is_some_and(|(left, right)| left == right)
}

/// Reuse persisted spellings and names. Ambiguous matches leave every cursor untouched.
pub fn reconcile(
    proposed: Vec<(&'static InputFormat, Binding)>,
    saved: &[Binding],
) -> Result<Vec<(&'static InputFormat, Binding)>, String> {
    let mut out: Vec<(&'static InputFormat, Binding)> = Vec::new();
    for (format, mut binding) in proposed {
        let candidates: Vec<_> = saved
            .iter()
            .filter(|old| same_identity(old, &binding) && same_input(&old.input, &binding.input))
            .collect();
        if candidates.len() > 1 {
            return Err(format!(
                "discovery conflict for {}: several saved bindings match",
                binding.name
            ));
        }
        if let Some(old) = candidates.first() {
            binding.name.clone_from(&old.name);
            binding.input.clone_from(&old.input);
        }
        if let Some((_, previous)) = out.iter().find(|(_, previous)| {
            same_identity(previous, &binding) && same_input(&previous.input, &binding.input)
        }) {
            if previous.settings != binding.settings {
                return Err(format!(
                    "discovery conflict for {}: conflicting settings",
                    binding.name
                ));
            }
            continue;
        }
        out.push((format, binding));
    }
    Ok(out)
}

#[cfg(test)]
mod tests;
