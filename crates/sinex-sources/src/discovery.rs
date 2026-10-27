//! Read-only discovery from application locations and the host's declared inputs.
use crate::sources;
use serde_json::json;
pub use sinex_source_core::discovery::*;
use sinex_source_core::{Binding, InputFormat, Settings};
use std::collections::BTreeSet;
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
        .map(|mut input| {
            if let Some(Some(exclusion)) = context.exclude.get(&input.source) {
                input.availability = Availability::Excluded;
                input.reason = Some(exclusion.reason.clone());
                input.bindings.clear();
            }
            input
        })
        .collect())
}

/// Session capabilities are known by the producer, not the daemon's account.
pub fn status(context: &DiscoveryContext) -> Result<Vec<DiscoveredInput>, String> {
    let mut inputs = discover(context)?;
    for input in &mut inputs {
        if input.capture
            && sources::format(&input.format)
                .is_some_and(|format| format.discovery.owner == Owner::Session)
            && input.availability != Availability::Excluded
        {
            input.availability = Availability::ProducerReported;
            input.reason = Some(
                "session capture status is reported by the producer; no status frame received"
                    .into(),
            );
            input.bindings.clear();
        }
    }
    Ok(inputs)
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
        "declared" => context
            .input_roots
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
                            .input_roots
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
            for root in context.input_roots.get("projects").into_iter().flatten() {
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
            out.sort();
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
            capture: false,
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
        if format.discovery.request_only || format.discovery.requests.contains(&spec.name) {
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
        if format.capture.is_some() {
            out.extend(
                capture_inputs(context, format.discovery.owner, true)
                    .into_iter()
                    .filter(|input| input.format == format.name && input.source == spec.name),
            );
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

/// Unavailable acquisition inputs leave their existing spool readers selected.
#[derive(Debug, Default)]
pub struct CapturePlan {
    pub bindings: Vec<(&'static InputFormat, Binding)>,
    pub exclude: BTreeSet<String>,
}

/// The same spool names as explicit session selections, so positions survive discovery.
pub fn captures(context: &DiscoveryContext, owner: Owner) -> Result<CapturePlan, String> {
    validate(context)?;
    Ok(CapturePlan {
        bindings: select_captures(context, owner, sources::formats()),
        exclude: context
            .exclude
            .iter()
            .filter(|(_, exclusion)| exclusion.is_some())
            .map(|(source, _)| source.clone())
            .collect(),
    })
}

/// Existing spools can drain after their session capabilities have disappeared.
pub fn spools(
    context: &DiscoveryContext,
    owner: Owner,
) -> Result<Vec<(&'static InputFormat, Binding)>, String> {
    validate(context)?;
    let inputs = capture_inputs(context, owner, false);
    let mut bindings = Vec::new();
    for input in inputs {
        if input.availability == Availability::Unavailable {
            return Err(input.reason.unwrap_or_else(|| "spool unavailable".into()));
        }
        let format = sources::format(&input.format).expect("capture format");
        bindings.extend(
            input
                .bindings
                .into_iter()
                .filter(|binding| binding.input.is_dir())
                .map(|binding| (format, binding)),
        );
    }
    Ok(bindings)
}

fn select_captures(
    context: &DiscoveryContext,
    owner: Owner,
    formats: &[&'static InputFormat],
) -> Vec<(&'static InputFormat, Binding)> {
    formats
        .iter()
        .copied()
        .filter(|format| format.capture.is_some() && format.discovery.owner == owner)
        .flat_map(|format| {
            (format.discover)(format, context)
                .into_iter()
                .filter(|input| input.capture && input.availability == Availability::Capturing)
                .flat_map(move |input| {
                    input
                        .bindings
                        .into_iter()
                        .filter(move |binding| {
                            !context
                                .exclude
                                .get(&binding.source)
                                .is_some_and(Option::is_some)
                        })
                        .map(move |binding| (format, binding))
                })
        })
        .collect()
}

fn capture_inputs(
    context: &DiscoveryContext,
    owner: Owner,
    check_capabilities: bool,
) -> Vec<DiscoveredInput> {
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
        let configured = context.sources.get(spec.name).cloned().unwrap_or_default();
        for stream in format.discovery.streams {
            let mut options: serde_json::Value =
                serde_json::from_str(stream.options).expect("declared capture options");
            if stream.capabilities.contains(&"watch") {
                options["roots"] = json!(context.watch);
            }
            if let Some(configured) = configured.options.as_object() {
                if !options.is_object() {
                    options = json!({});
                }
                options.as_object_mut().unwrap().extend(configured.clone());
            }
            let input = context.spool.join(stream.spool);
            let settings = Settings {
                host: context.host.clone(),
                class: configured.class,
                zone: configured.zone.clone(),
                options: options.clone(),
            };
            let missing: Vec<_> = format
                .discovery
                .capabilities
                .iter()
                .chain(stream.capabilities)
                .filter(|name| check_capabilities && !capability(context, name))
                .copied()
                .collect();
            let mut found = DiscoveredInput {
                format: format.name.into(),
                source: spec.name.into(),
                input: Some(input.clone()),
                settings,
                availability: Availability::WaitingForInput,
                capture: true,
                reason: None,
                bindings: vec![],
            };
            if !missing.is_empty() {
                found.reason = Some(format!("capture requires {}", missing.join(", ")));
            } else {
                match crate::live::binding(format.name, &input, &context.host, options) {
                    Ok((_, mut binding)) => {
                        binding.settings.class = configured.class;
                        binding.settings.zone = configured.zone.clone();
                        found.settings = binding.settings.clone();
                        found.bindings.push(binding);
                        found.availability = Availability::Capturing;
                    }
                    Err(error) => {
                        found.availability = Availability::Unavailable;
                        found.reason = Some(error);
                    }
                }
            }
            out.push(found);
        }
    }
    out
}

fn suffix(binding: &Binding) -> Option<&str> {
    binding
        .name
        .strip_prefix(&format!("{}:{}", binding.source, binding.input.display()))
}
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct BindingIdentity {
    source: String,
    parser: String,
    host: String,
    suffix: Option<String>,
    input: PathBuf,
}

impl BindingIdentity {
    fn of(binding: &Binding) -> Self {
        Self {
            source: binding.source.clone(),
            parser: binding.parser.clone(),
            host: binding.settings.host.clone(),
            suffix: suffix(binding).map(str::to_owned),
            input: std::fs::canonicalize(&binding.input).unwrap_or_else(|_| binding.input.clone()),
        }
    }
}

/// Reconciled bindings and inputs that cannot be read without choosing a cursor.
#[derive(Default)]
pub struct BindingPlan {
    pub bindings: Vec<(&'static InputFormat, Binding)>,
    pub unavailable: Vec<(Binding, String)>,
}

/// Reuse persisted spellings and names. A conflict leaves that input's cursors untouched.
pub fn reconcile(
    mut proposed: Vec<(&'static InputFormat, Binding)>,
    saved: &[Binding],
) -> BindingPlan {
    let mut out = BindingPlan::default();
    let mut saved_by_input = std::collections::BTreeMap::<_, Vec<_>>::new();
    for binding in saved {
        saved_by_input
            .entry(BindingIdentity::of(binding))
            .or_default()
            .push(binding);
    }
    let mut proposed_by_input = std::collections::BTreeMap::<_, Vec<_>>::new();
    for (format, binding) in proposed.drain(..) {
        proposed_by_input
            .entry(BindingIdentity::of(&binding))
            .or_default()
            .push((format, binding));
    }
    for (identity, mut group) in proposed_by_input {
        let (format, mut binding) = group.remove(0);
        let aliases: Vec<_> = group.into_iter().map(|(_, binding)| binding).collect();
        let candidates = saved_by_input.remove(&identity).unwrap_or_default();
        let reason = if candidates.len() > 1 {
            Some("several saved bindings match")
        } else if aliases
            .iter()
            .any(|alias| alias.settings != binding.settings)
        {
            Some("conflicting settings")
        } else {
            None
        };
        if let Some(reason) = reason {
            let reason = format!("discovery conflict for {}: {reason}", binding.name);
            for alias in aliases
                .into_iter()
                .chain(candidates.into_iter().cloned())
                .chain(std::iter::once(binding))
            {
                out.unavailable.push((alias, reason.clone()));
            }
            continue;
        }
        if let Some(old) = candidates.first() {
            binding.name.clone_from(&old.name);
            binding.input.clone_from(&old.input);
        }
        out.bindings.push((format, binding));
    }
    out
}

#[cfg(test)]
mod tests;
