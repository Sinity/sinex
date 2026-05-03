use color_eyre::eyre::{Context, Result, bail, ensure};
use serde::Serialize;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use walkdir::WalkDir;

const EXCLUDED_COMPONENT_NAMES: &[&str] = &[".git", ".sinex", ".direnv", ".devenv", "target"];

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct FlakeStageReport {
    pub source_root: String,
    pub staged_root: String,
    pub flake_uri: String,
    pub copied_dirs: usize,
    pub copied_files: usize,
    pub copied_symlinks: usize,
    pub excluded_paths: Vec<String>,
    pub unsupported_paths: Vec<String>,
}

pub fn stage_checkout_for_flake(
    source_root: &Path,
    output_dir: Option<&Path>,
    force: bool,
) -> Result<FlakeStageReport> {
    let source_root = source_root
        .canonicalize()
        .with_context(|| format!("failed to resolve source root {}", source_root.display()))?;
    ensure!(
        source_root.is_dir(),
        "{} is not a directory",
        source_root.display()
    );

    let staged_root = prepare_output_dir(&source_root, output_dir, force)?;
    fs::create_dir_all(&staged_root)
        .with_context(|| format!("failed to create {}", staged_root.display()))?;

    let mut report = FlakeStageReport {
        source_root: source_root.display().to_string(),
        staged_root: staged_root.display().to_string(),
        flake_uri: format!("path:{}", staged_root.display()),
        copied_dirs: 0,
        copied_files: 0,
        copied_symlinks: 0,
        excluded_paths: Vec::new(),
        unsupported_paths: Vec::new(),
    };

    let mut iter = WalkDir::new(&source_root).follow_links(false).into_iter();
    while let Some(entry) = iter.next() {
        let entry = entry.with_context(|| {
            format!(
                "failed to walk source tree while staging {}",
                source_root.display()
            )
        })?;
        let path = entry.path();

        if path == source_root {
            continue;
        }

        let relative = path
            .strip_prefix(&source_root)
            .expect("walked path must stay under source root");

        if should_exclude(relative) {
            report.excluded_paths.push(relative.display().to_string());
            if entry.file_type().is_dir() {
                iter.skip_current_dir();
            }
            continue;
        }

        let destination = staged_root.join(relative);
        if entry.file_type().is_dir() {
            fs::create_dir_all(&destination)
                .with_context(|| format!("failed to create {}", destination.display()))?;
            report.copied_dirs += 1;
            continue;
        }

        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        if entry.file_type().is_file() {
            fs::copy(path, &destination).with_context(|| {
                format!(
                    "failed to copy {} -> {}",
                    path.display(),
                    destination.display()
                )
            })?;
            report.copied_files += 1;
            continue;
        }

        if entry.file_type().is_symlink() {
            let target = fs::read_link(path)
                .with_context(|| format!("failed to read symlink {}", path.display()))?;
            recreate_symlink(&target, &destination)?;
            report.copied_symlinks += 1;
            continue;
        }

        report
            .unsupported_paths
            .push(relative.display().to_string());
        if entry.file_type().is_dir() {
            iter.skip_current_dir();
        }
    }

    report.excluded_paths.sort();
    report.excluded_paths.dedup();
    report.unsupported_paths.sort();
    report.unsupported_paths.dedup();

    Ok(report)
}

fn prepare_output_dir(
    source_root: &Path,
    output_dir: Option<&Path>,
    force: bool,
) -> Result<PathBuf> {
    let output_dir = match output_dir {
        Some(path) => {
            let path = if path.is_absolute() {
                path.to_path_buf()
            } else {
                std::env::current_dir()
                    .context("failed to resolve current directory for output path")?
                    .join(path)
            };

            ensure!(
                !path.starts_with(source_root),
                "refusing to stage flake input inside the source tree ({})",
                path.display()
            );

            if path.exists() {
                if !force {
                    bail!(
                        "{} already exists; rerun with --force or choose a different output directory",
                        path.display()
                    );
                }

                if path.is_dir() {
                    fs::remove_dir_all(&path)
                        .with_context(|| format!("failed to remove {}", path.display()))?;
                } else {
                    fs::remove_file(&path)
                        .with_context(|| format!("failed to remove {}", path.display()))?;
                }
            }

            path
        }
        None => unique_temp_stage_dir()?,
    };

    Ok(output_dir)
}

fn unique_temp_stage_dir() -> Result<PathBuf> {
    let base = std::env::temp_dir();
    let pid = std::process::id();
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock moved backwards while preparing flake stage path")?
        .as_nanos();

    for attempt in 0..100u32 {
        let candidate = base.join(format!("sinex-flake-stage-{pid}-{stamp}-{attempt}"));
        if !candidate.exists() {
            return Ok(candidate);
        }
    }

    bail!(
        "failed to allocate a unique flake staging directory under {}",
        base.display()
    );
}

fn should_exclude(relative: &Path) -> bool {
    let mut components = relative.components();

    let Some(first_component) = components.next() else {
        return false;
    };

    let Component::Normal(name) = first_component else {
        return false;
    };
    let Some(name) = name.to_str() else {
        return false;
    };

    if name == "result" || name.starts_with("result-") {
        return true;
    }

    if EXCLUDED_COMPONENT_NAMES.contains(&name) {
        return true;
    }

    components.any(|component| match component {
        Component::Normal(name) => name
            .to_str()
            .is_some_and(|name| EXCLUDED_COMPONENT_NAMES.contains(&name)),
        _ => false,
    })
}

#[cfg(unix)]
fn recreate_symlink(target: &Path, destination: &Path) -> Result<()> {
    std::os::unix::fs::symlink(target, destination).with_context(|| {
        format!(
            "failed to recreate symlink {} -> {}",
            destination.display(),
            target.display()
        )
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;
    use std::os::unix::net::UnixListener;

    #[test]
    fn should_exclude_root_local_artifacts() {
        for relative in [
            Path::new(".git/config"),
            Path::new(".sinex/run/.s.PGSQL.5432"),
            Path::new(
                "crate/lib/sinex-macros/.sinex/trybuild-target/tests/trybuild/foo/Cargo.toml",
            ),
            Path::new("crate/lib/sinex-primitives/target/debug/deps/libfoo.rmeta"),
            Path::new(".direnv/flake-input"),
            Path::new(".devenv/state"),
            Path::new("target/debug/xtask"),
            Path::new("result"),
            Path::new("result-bin"),
        ] {
            assert!(
                should_exclude(relative),
                "expected {} to be excluded",
                relative.display()
            );
        }

        for relative in [
            Path::new("Cargo.toml"),
            Path::new("crate/nodes/sinex-process/Cargo.toml"),
            Path::new("docs/README.md"),
        ] {
            assert!(
                !should_exclude(relative),
                "expected {} to stay included",
                relative.display()
            );
        }
    }

    #[test]
    fn stage_checkout_for_flake_filters_runtime_state_and_keeps_new_sources() -> Result<()> {
        let source = tempfile::tempdir()?;
        let output = tempfile::tempdir()?;

        fs::write(source.path().join("flake.nix"), "{ }\n")?;
        fs::create_dir_all(source.path().join("crate/nodes/new-node"))?;
        fs::write(
            source.path().join("crate/nodes/new-node/Cargo.toml"),
            "[package]\nname = \"new-node\"\nversion = \"0.1.0\"\n",
        )?;

        fs::create_dir_all(source.path().join(".sinex/run"))?;
        let socket_path = source.path().join(".sinex/run/.s.PGSQL.5432");
        let _listener = UnixListener::bind(&socket_path)?;

        let nested_trybuild_manifest = source.path().join(
            "crate/lib/sinex-macros/.sinex/trybuild-target/tests/trybuild/sinex-macros/Cargo.toml",
        );
        fs::create_dir_all(
            nested_trybuild_manifest
                .parent()
                .expect("nested trybuild manifest parent should exist"),
        )?;
        fs::write(
            &nested_trybuild_manifest,
            "[package]\nname = \"trybuild-generated\"\nversion = \"0.1.0\"\n",
        )?;

        fs::create_dir_all(source.path().join("target/debug"))?;
        fs::write(source.path().join("target/debug/xtask"), "binary")?;
        symlink("flake.nix", source.path().join("result"))?;

        let report = stage_checkout_for_flake(source.path(), Some(output.path()), true)?;

        assert!(
            output.path().join("flake.nix").is_file(),
            "top-level source file must be copied"
        );
        assert!(
            output
                .path()
                .join("crate/nodes/new-node/Cargo.toml")
                .is_file(),
            "new source files must survive staging"
        );
        assert!(
            !output.path().join(".sinex").exists(),
            "runtime state directory must be excluded"
        );
        assert!(
            !output.path().join("crate/lib/sinex-macros/.sinex").exists(),
            "nested crate-local runtime state must be excluded"
        );
        assert!(
            !output.path().join("target").exists(),
            "build output must be excluded"
        );
        assert!(
            !output.path().join("result").exists(),
            "checkout-local result symlink must be excluded"
        );
        assert!(
            report
                .excluded_paths
                .iter()
                .any(|path| path == ".sinex" || path.starts_with(".sinex/")),
            "excluded paths should mention runtime state"
        );
        assert!(
            report.excluded_paths.iter().any(|path| {
                path == "crate/lib/sinex-macros/.sinex"
                    || path.starts_with("crate/lib/sinex-macros/.sinex/")
            }),
            "excluded paths should mention nested crate-local runtime state"
        );
        assert!(
            report.flake_uri.starts_with("path:"),
            "staged report should include a flake path URI"
        );

        Ok(())
    }

    #[test]
    fn stage_checkout_for_flake_preserves_regular_symlinks() -> Result<()> {
        let source = tempfile::tempdir()?;
        let output = tempfile::tempdir()?;

        fs::create_dir_all(source.path().join("docs"))?;
        fs::write(source.path().join("docs/README.md"), "hello")?;
        symlink("docs/README.md", source.path().join("docs-link"))?;

        let report = stage_checkout_for_flake(source.path(), Some(output.path()), true)?;

        let staged_link = output.path().join("docs-link");
        assert!(fs::symlink_metadata(&staged_link)?.file_type().is_symlink());
        assert_eq!(
            fs::read_link(&staged_link)?,
            PathBuf::from("docs/README.md")
        );
        assert_eq!(report.copied_symlinks, 1);

        Ok(())
    }
}
