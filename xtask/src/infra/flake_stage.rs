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
#[path = "flake_stage_test.rs"]
mod tests;
