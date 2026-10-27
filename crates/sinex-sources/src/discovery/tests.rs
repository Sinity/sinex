use super::*;
use sinex_model::Class;
use std::os::unix::fs::{PermissionsExt, symlink};

fn context(root: &Path) -> DiscoveryContext {
    DiscoveryContext {
        host: "fixture".into(),
        home: Some(root.into()),
        data_home: Some(root.join("data")),
        config_home: Some(root.join("config")),
        spool: root.join("spool"),
        ..Default::default()
    }
}
fn binding(context: &DiscoveryContext, source: &str) -> Vec<Binding> {
    discover(context)
        .unwrap()
        .into_iter()
        .filter(|input| input.source == source)
        .flat_map(|input| input.bindings)
        .collect()
}

#[devkit::test]
fn every_format_declares_discovery_or_a_request_only_role() {
    for format in sources::formats() {
        assert!(
            format.discovery.request_only
                || !format.discovery.locations.is_empty()
                || !format.discovery.streams.is_empty(),
            "{}",
            format.name
        );
        if format.capture.is_some() {
            assert!(!format.discovery.streams.is_empty(), "{}", format.name);
        }
    }
}

#[devkit::test]
fn discovers_late_inputs_and_distinguishes_empty_missing_and_unreadable() {
    let root = tempfile::tempdir().unwrap();
    let context = context(root.path());
    let states = || {
        discover(&context)
            .unwrap()
            .into_iter()
            .find(|input| input.format == "atuin")
            .unwrap()
    };
    assert_eq!(states().availability, Availability::WaitingForInput);
    let path = context.data_home.as_ref().unwrap().join("atuin/history.db");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, b"").unwrap();
    assert_eq!(states().availability, Availability::Reading);
    assert_eq!(states().bindings.len(), 1);
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o0)).unwrap();
    assert_eq!(states().availability, Availability::Unavailable);
    std::fs::remove_file(&path).unwrap();
    assert_eq!(states().availability, Availability::WaitingForInput);
}

#[devkit::test]
fn every_browser_profile_and_shared_source_format_is_discovered() {
    let root = tempfile::tempdir().unwrap();
    let mut context = context(root.path());
    for browser in ["chromium", "google-chrome"] {
        for profile in ["Default", "Profile 1"] {
            let dir = context
                .config_home
                .as_ref()
                .unwrap()
                .join(browser)
                .join(profile);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("History"), b"").unwrap();
        }
    }
    assert_eq!(binding(&context, "browser.chromium-visits").len(), 4);
    assert_eq!(binding(&context, "browser.chromium-urls").len(), 4);
    context.sources.insert(
        "desktop.clipboard".into(),
        SourceConfig {
            format: Some("sinnix-lake".into()),
            ..Default::default()
        },
    );
    let inputs = discover(&context).unwrap();
    assert!(
        inputs
            .iter()
            .any(|input| input.source == "desktop.clipboard" && input.format == "clipboard")
    );
    assert!(
        inputs
            .iter()
            .any(|input| input.source == "desktop.clipboard" && input.format == "sinnix-lake")
    );
}

#[devkit::test]
fn exclusion_requires_a_known_source_and_non_whitespace_reason() {
    let mut context = DiscoveryContext::default();
    context.exclude.insert(
        "desktop.clipboard".into(),
        Some(Exclusion {
            reason: " \n\t".into(),
        }),
    );
    assert!(discover(&context).unwrap_err().contains("non-whitespace"));
    context.exclude.insert(
        "desktop.clipboard".into(),
        Some(Exclusion {
            reason: "fixture choice".into(),
        }),
    );
    assert!(
        discover(&context)
            .unwrap()
            .iter()
            .filter(|input| input.source == "desktop.clipboard")
            .all(|input| input.availability == Availability::Excluded
                && input.reason.as_deref() == Some("fixture choice"))
    );
    context.exclude.insert("missing.source".into(), None);
    assert!(discover(&context).unwrap_err().contains("missing.source"));
}

#[devkit::test]
fn keeps_saved_spelling_suffix_and_name_when_settings_change() {
    let root = tempfile::tempdir().unwrap();
    let input = root.path().join("real");
    let alias = root.path().join("alias");
    std::fs::write(&input, b"").unwrap();
    symlink(&input, &alias).unwrap();
    let format = sources::format("atuin").unwrap();
    let saved = (format.plan.unwrap())(&alias, &Settings::new("fixture"))
        .unwrap()
        .remove(0)
        .named("table");
    let mut proposed = (format.plan.unwrap())(&input, &Settings::new("fixture"))
        .unwrap()
        .remove(0)
        .named("table");
    proposed.settings.class = Some(Class::Guarded);
    let planned = reconcile(
        vec![(format, proposed.clone())],
        std::slice::from_ref(&saved),
    )
    .bindings;
    assert_eq!(planned[0].1.name, saved.name);
    assert_eq!(planned[0].1.input, alias);
    assert_eq!(planned[0].1.settings.class, Some(Class::Guarded));
    assert_eq!(
        reconcile(planned.clone(), std::slice::from_ref(&saved)).bindings[0]
            .1
            .name,
        saved.name
    );
    let mut other = proposed.clone();
    other.name = other.name.replace("#table", "#other");
    assert_eq!(
        reconcile(vec![(format, other.clone())], std::slice::from_ref(&saved)).bindings[0]
            .1
            .name,
        other.name
    );
    assert!(
        reconcile(
            vec![(format, proposed.clone())],
            &[saved.clone(), proposed.clone()]
        )
        .unavailable
        .iter()
        .any(|(_, reason)| reason.contains("several saved"))
    );
    std::fs::remove_file(alias).unwrap();
    assert_eq!(
        reconcile(vec![(format, proposed.clone())], &[saved]).bindings[0]
            .1
            .name,
        proposed.name
    );
}

#[devkit::test]
fn new_aliases_are_deduplicated_only_within_the_same_parser() {
    let root = tempfile::tempdir().unwrap();
    let input = root.path().join("real");
    let alias = root.path().join("alias");
    std::fs::write(&input, b"").unwrap();
    symlink(&input, &alias).unwrap();
    let format = sources::format("atuin").unwrap();
    let first = (format.plan.unwrap())(&alias, &Settings::new("fixture"))
        .unwrap()
        .remove(0);
    let second = (format.plan.unwrap())(&input, &Settings::new("fixture"))
        .unwrap()
        .remove(0);
    assert_eq!(
        reconcile(vec![(format, first.clone()), (format, second.clone())], &[])
            .bindings
            .len(),
        1
    );
    let mut other = second.clone();
    other.parser = "different-parser".into();
    assert_eq!(
        reconcile(vec![(format, first.clone()), (format, other)], &[])
            .bindings
            .len(),
        2
    );
    let mut changed = second;
    changed.settings.options = json!({"changed":true});
    assert!(
        reconcile(vec![(format, first), (format, changed)], &[])
            .unavailable
            .iter()
            .any(|(_, reason)| reason.contains("conflicting settings"))
    );
}

#[devkit::test]
fn session_captures_available_streams_without_selection_flags() {
    let root = tempfile::tempdir().unwrap();
    let mut context = context(root.path());
    let runtime = root.path().join("run");
    std::fs::create_dir(&runtime).unwrap();
    for name in ["wayland-1", "bus", "pipewire-0"] {
        std::fs::write(runtime.join(name), b"").unwrap();
    }
    context.runtime_dir = Some(runtime.clone());
    context
        .environment
        .insert("WAYLAND_DISPLAY".into(), "wayland-1".into());
    context.environment.insert(
        "DBUS_SESSION_BUS_ADDRESS".into(),
        format!("unix:path={}", runtime.join("bus").display()),
    );
    let bin = root.path().join("bin");
    std::fs::create_dir(&bin).unwrap();
    for name in ["wl-paste", "busctl", "grim", "pw-record"] {
        let path = bin.join(name);
        std::fs::write(&path, b"#!/bin/sh\n").unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    context
        .environment
        .insert("PATH".into(), bin.display().to_string());
    let selected = captures(&context, Owner::Session).unwrap().bindings;
    let names: Vec<_> = selected
        .iter()
        .map(|(_, binding)| binding.name.clone())
        .collect();
    for suffix in [
        "clipboard#clipboard",
        "primary#primary",
        "notifications",
        "players",
        "integrations",
        "screen#screen",
        "audio#audio",
    ] {
        assert!(
            names
                .iter()
                .any(|name| name.ends_with(&format!("/{suffix}@fixture"))),
            "{suffix}: {names:?}"
        );
    }
    context.exclude.insert(
        "desktop.clipboard".into(),
        Some(Exclusion {
            reason: "fixture".into(),
        }),
    );
    assert!(
        captures(&context, Owner::Session)
            .unwrap()
            .bindings
            .iter()
            .all(|(_, binding)| binding.source != "desktop.clipboard")
    );
    assert!(
        captures(&DiscoveryContext::default(), Owner::Session)
            .unwrap()
            .bindings
            .is_empty()
    );
}

#[devkit::test]
fn named_capture_paths_come_from_the_host_layout() {
    let root = tempfile::tempdir().unwrap();
    let mut context = context(root.path());
    let dir = root.path().join("activity/terminal/scrollback");
    std::fs::create_dir_all(&dir).unwrap();
    let layout = root.path().join("layout.json");
    std::fs::write(
        &layout,
        br#"{"activity_lanes":{"kitty-scrollback":"terminal/scrollback"}}"#,
    )
    .unwrap();
    context.layout_file = Some(layout);
    context
        .input_roots
        .insert("realm".into(), vec![root.path().into()]);
    let inputs = discover(&context).unwrap();
    assert!(inputs.iter().any(|input| input.format == "kitty-scrollback"
        && input.input.as_ref() == Some(&dir)
        && input.availability == Availability::Reading));
}

#[devkit::test]
fn missing_imap_credentials_are_unavailable_without_disabling_other_inputs() {
    let root = tempfile::tempdir().unwrap();
    let account = root.path().join("account.json");
    std::fs::write(&account,serde_json::to_vec(&json!({"host":"mail.example.test","username":"fixture","password_file":root.path().join("absent-secret")})).unwrap()).unwrap();
    let mut context = context(root.path());
    context.inputs.insert("imap".into(), vec![account]);
    let discovered = discover(&context).unwrap();
    assert!(discovered.iter().any(|input| {
        input.format == "imap"
            && input.availability == Availability::Unavailable
            && input
                .reason
                .as_ref()
                .is_some_and(|reason| reason.contains("absent-secret"))
    }));
    assert!(
        discovered
            .iter()
            .any(|input| input.format == "forecast"
                && input.availability == Availability::RequestOnly)
    );
}

#[devkit::test]
fn invalid_capture_settings_do_not_disable_other_streams() {
    let root = tempfile::tempdir().unwrap();
    let mut context = context(root.path());
    let runtime = root.path().join("run");
    std::fs::create_dir(&runtime).unwrap();
    std::fs::write(runtime.join("wayland-test"), b"").unwrap();
    context.runtime_dir = Some(runtime);
    context
        .environment
        .insert("WAYLAND_DISPLAY".into(), "wayland-test".into());
    let bin = root.path().join("bin");
    std::fs::create_dir(&bin).unwrap();
    let grim = bin.join("grim");
    std::fs::write(&grim, b"#!/bin/sh\n").unwrap();
    std::fs::set_permissions(grim, std::fs::Permissions::from_mode(0o700)).unwrap();
    context
        .environment
        .insert("PATH".into(), bin.display().to_string());
    context.sources.insert(
        "session.media".into(),
        SourceConfig {
            options: json!({"kind":"invalid"}),
            ..Default::default()
        },
    );
    let inputs = discover(&context).unwrap();
    assert!(
        inputs.iter().any(|input| input.source == "session.media"
            && input.availability == Availability::Unavailable)
    );
    assert!(
        captures(&context, Owner::Session)
            .unwrap()
            .bindings
            .iter()
            .any(|(_, binding)| binding.source == "session.integrations")
    );
    assert!(inputs.iter().any(|input| input.source == "library.request"
        && input.availability == Availability::RequestOnly));
}

#[devkit::test]
fn capture_selection_uses_the_formats_discovery_callback() {
    const FORMAT: InputFormat = InputFormat {
        discover: |format, context| {
            let binding = (format.plan.unwrap())(
                &context.spool.join("dynamic-device"),
                &Settings::new(&context.host),
            )
            .unwrap()
            .remove(0);
            vec![DiscoveredInput {
                format: format.name.into(),
                source: binding.source.clone(),
                input: Some(binding.input.clone()),
                settings: binding.settings.clone(),
                availability: Availability::Capturing,
                capture: true,
                reason: None,
                bindings: vec![binding],
            }]
        },
        ..crate::sources::file_watch::FORMAT
    };
    let mut context = DiscoveryContext {
        host: "fixture".into(),
        spool: PathBuf::from("/synthetic/spool"),
        ..Default::default()
    };
    let captures = select_captures(&context, Owner::Session, &[&FORMAT]);
    assert_eq!(captures.len(), 1);
    assert_eq!(
        captures[0].1.input,
        Path::new("/synthetic/spool/dynamic-device")
    );
    context.exclude.insert(
        (FORMAT.specs)()[0].name.into(),
        Some(Exclusion {
            reason: "fixture exclusion".into(),
        }),
    );
    assert!(select_captures(&context, Owner::Session, &[&FORMAT]).is_empty());
}

#[devkit::test]
fn takeout_keeps_calendar_bindings_and_applies_calendar_settings_and_exclusions() {
    let root = tempfile::tempdir().unwrap();
    let archive = root.path().join("takeout.zip");
    std::fs::write(&archive, b"synthetic archive").unwrap();
    let mut context = context(root.path());
    context
        .inputs
        .insert("takeout".into(), vec![archive.clone()]);
    context.sources.insert(
        "calendar.export".into(),
        SourceConfig {
            zone: Some("Europe/Warsaw".into()),
            class: Some(Class::Guarded),
            ..Default::default()
        },
    );
    let calendars = binding(&context, "calendar.export");
    assert_eq!(calendars.len(), 1);
    assert_eq!(calendars[0].input, archive);
    assert_eq!(calendars[0].settings.zone.as_deref(), Some("Europe/Warsaw"));
    assert_eq!(calendars[0].settings.class, Some(Class::Guarded));
    context.exclude.insert(
        "calendar.export".into(),
        Some(Exclusion {
            reason: "fixture".into(),
        }),
    );
    assert!(binding(&context, "calendar.export").is_empty());
    assert!(!binding(&context, "google.fit").is_empty());
}

#[devkit::test]
fn daemon_status_reports_session_ownership_without_session_capabilities() {
    let root = tempfile::tempdir().unwrap();
    let context = context(root.path());
    let inputs = status(&context).unwrap();
    let captures: Vec<_> = inputs.iter().filter(|input| input.capture).collect();
    assert!(!captures.is_empty());
    for input in captures {
        if sources::format(&input.format).unwrap().discovery.owner == Owner::Session {
            assert_eq!(input.availability, Availability::ProducerReported);
            assert!(input.input.is_none());
            assert!(input.bindings.is_empty());
            assert!(
                input
                    .reason
                    .as_deref()
                    .unwrap()
                    .contains("no status frame received")
            );
        }
    }
}
