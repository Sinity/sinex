//! Static compile-surface attribution for workspace packages.

use color_eyre::eyre::{Context, Result, bail};
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize)]
pub struct CompileSurfaceReport {
    pub package: String,
    pub manifest_path: String,
    pub source_root: String,
    pub total_rust_files: usize,
    pub total_rust_bytes: u64,
    pub largest_files: Vec<SourceFileSurface>,
    pub largest_module_buckets: Vec<ModuleBucketSurface>,
    pub dependency_sections: Vec<DependencySectionSurface>,
    pub direct_dependency_count: usize,
    pub optional_dependency_count: usize,
    pub path_dependency_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceFileSurface {
    pub path: String,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ModuleBucketSurface {
    pub bucket: String,
    pub files: usize,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DependencySectionSurface {
    pub section: String,
    pub dependencies: Vec<DependencySurface>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DependencySurface {
    pub name: String,
    pub path: Option<String>,
    pub workspace: bool,
    pub optional: bool,
}

pub fn analyze(package: &str, top: usize) -> Result<CompileSurfaceReport> {
    let manifest_path = manifest_path_for_package(package)?;
    let manifest_dir = manifest_path
        .parent()
        .ok_or_else(|| color_eyre::eyre::eyre!("manifest path has no parent"))?;
    let source_root = manifest_dir.join("src");
    let manifest_text = std::fs::read_to_string(&manifest_path)
        .with_context(|| format!("failed to read {}", manifest_path.display()))?;
    let manifest =
        toml::from_str::<toml::Value>(&manifest_text).context("failed to parse Cargo manifest")?;

    let source_files = collect_rust_sources(&source_root)?;
    let total_rust_files = source_files.len();
    let total_rust_bytes = source_files.iter().map(|file| file.bytes).sum();
    let mut largest_files = source_files.clone();
    largest_files.sort_by(|left, right| {
        right
            .bytes
            .cmp(&left.bytes)
            .then(left.path.cmp(&right.path))
    });
    largest_files.truncate(top);

    let mut largest_module_buckets = module_buckets(&source_root, &source_files);
    largest_module_buckets.sort_by(|left, right| {
        right
            .bytes
            .cmp(&left.bytes)
            .then(left.bucket.cmp(&right.bucket))
    });
    largest_module_buckets.truncate(top);

    let dependency_sections = dependency_sections(&manifest);
    let direct_dependency_count = dependency_sections
        .iter()
        .map(|section| section.dependencies.len())
        .sum();
    let optional_dependency_count = dependency_sections
        .iter()
        .flat_map(|section| &section.dependencies)
        .filter(|dependency| dependency.optional)
        .count();
    let path_dependency_count = dependency_sections
        .iter()
        .flat_map(|section| &section.dependencies)
        .filter(|dependency| dependency.path.is_some())
        .count();

    Ok(CompileSurfaceReport {
        package: package.to_string(),
        manifest_path: display_path(&manifest_path),
        source_root: display_path(&source_root),
        total_rust_files,
        total_rust_bytes,
        largest_files,
        largest_module_buckets,
        dependency_sections,
        direct_dependency_count,
        optional_dependency_count,
        path_dependency_count,
    })
}

pub fn render_human(report: &CompileSurfaceReport) -> String {
    let mut out = String::new();
    out.push_str("Compile Surface Analysis\n");
    out.push_str(&format!("Package: {}\n", report.package));
    out.push_str(&format!("Manifest: {}\n", report.manifest_path));
    out.push_str(&format!("Source root: {}\n", report.source_root));
    out.push_str(&format!(
        "Rust source: {} files, {:.1} KiB\n",
        report.total_rust_files,
        report.total_rust_bytes as f64 / 1024.0
    ));
    out.push_str(&format!(
        "Direct manifest deps: {} total, {} path/workspace-local, {} optional\n\n",
        report.direct_dependency_count,
        report.path_dependency_count,
        report.optional_dependency_count
    ));

    out.push_str("Largest source files:\n");
    for (index, file) in report.largest_files.iter().enumerate() {
        out.push_str(&format!(
            "  {}. {} - {:.1} KiB\n",
            index + 1,
            file.path,
            file.bytes as f64 / 1024.0
        ));
    }

    out.push_str("\nLargest module buckets:\n");
    for (index, bucket) in report.largest_module_buckets.iter().enumerate() {
        out.push_str(&format!(
            "  {}. {} - {:.1} KiB ({} files)\n",
            index + 1,
            bucket.bucket,
            bucket.bytes as f64 / 1024.0,
            bucket.files
        ));
    }

    out.push_str("\nDependency sections:\n");
    for section in &report.dependency_sections {
        out.push_str(&format!(
            "  {}: {} deps\n",
            section.section,
            section.dependencies.len()
        ));
        for dependency in &section.dependencies {
            let mut flags = Vec::new();
            if dependency.workspace {
                flags.push("workspace");
            }
            if dependency.path.is_some() {
                flags.push("path");
            }
            if dependency.optional {
                flags.push("optional");
            }
            let flags = if flags.is_empty() {
                String::new()
            } else {
                format!(" ({})", flags.join(", "))
            };
            out.push_str(&format!("    - {}{}\n", dependency.name, flags));
        }
    }

    out
}

fn manifest_path_for_package(package: &str) -> Result<PathBuf> {
    let root = crate::config::workspace_root();
    let candidates = [
        root.join(package).join("Cargo.toml"),
        root.join("crate").join(package).join("Cargo.toml"),
    ];
    for candidate in candidates {
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    bail!("could not find workspace manifest for package '{package}'")
}

fn collect_rust_sources(source_root: &Path) -> Result<Vec<SourceFileSurface>> {
    if !source_root.is_dir() {
        bail!("source root does not exist: {}", source_root.display());
    }

    let mut files = Vec::new();
    for entry in walkdir::WalkDir::new(source_root) {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue;
        }
        if entry.path().extension().and_then(|ext| ext.to_str()) != Some("rs") {
            continue;
        }
        let metadata = entry.metadata()?;
        files.push(SourceFileSurface {
            path: display_path(entry.path()),
            bytes: metadata.len(),
        });
    }
    Ok(files)
}

fn module_buckets(source_root: &Path, files: &[SourceFileSurface]) -> Vec<ModuleBucketSurface> {
    let mut buckets = BTreeMap::<String, (usize, u64)>::new();
    let source_root_prefix = format!("{}/", display_path(source_root));
    for file in files {
        let rel_path = file
            .path
            .strip_prefix(&source_root_prefix)
            .unwrap_or(file.path.as_str());
        let bucket = Path::new(rel_path)
            .components()
            .next()
            .and_then(|component| component.as_os_str().to_str())
            .unwrap_or("<root>")
            .to_string();
        let entry = buckets.entry(bucket).or_default();
        entry.0 += 1;
        entry.1 += file.bytes;
    }

    buckets
        .into_iter()
        .map(|(bucket, (files, bytes))| ModuleBucketSurface {
            bucket,
            files,
            bytes,
        })
        .collect()
}

fn dependency_sections(manifest: &toml::Value) -> Vec<DependencySectionSurface> {
    ["dependencies", "build-dependencies", "dev-dependencies"]
        .into_iter()
        .filter_map(|section| dependency_section(manifest, section))
        .collect()
}

fn dependency_section(manifest: &toml::Value, section: &str) -> Option<DependencySectionSurface> {
    let table = manifest.get(section)?.as_table()?;
    let mut dependencies = table
        .iter()
        .map(|(name, value)| dependency_surface(name, value))
        .collect::<Vec<_>>();
    dependencies.sort_by(|left, right| left.name.cmp(&right.name));
    Some(DependencySectionSurface {
        section: section.to_string(),
        dependencies,
    })
}

fn dependency_surface(name: &str, value: &toml::Value) -> DependencySurface {
    let path = value
        .as_table()
        .and_then(|table| table.get("path"))
        .and_then(toml::Value::as_str)
        .map(ToString::to_string);
    let workspace = value
        .as_table()
        .and_then(|table| table.get("workspace"))
        .and_then(toml::Value::as_bool)
        .unwrap_or(false);
    let optional = value
        .as_table()
        .and_then(|table| table.get("optional"))
        .and_then(toml::Value::as_bool)
        .unwrap_or(false);

    DependencySurface {
        name: name.to_string(),
        path,
        workspace,
        optional,
    }
}

fn display_path(path: &Path) -> String {
    path.strip_prefix(crate::config::workspace_root())
        .unwrap_or(path)
        .display()
        .to_string()
}

#[cfg(test)]
#[path = "compile_surface_test.rs"]
mod tests;
