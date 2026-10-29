//! Converted folders live in a private managed directory owned by each reader.
use super::{PST, pst_driver};
use serde_json::Value;
use sinex_model::{Class, ObservationFacts};
use sinex_source_core::DriverError;
use sinex_source_core::binding::{Binding, Reading, Settings};
use sinex_source_core::shared_pass::{Acquisition, Route, SharedPass, SharedReadStart};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::process::Command;

fn convert(pst: &Path, out: &Path, converter: &Path) -> Result<Vec<PathBuf>, String> {
    // The caller owns this new directory; no existing conversion is removed.
    let status = Command::new(converter)
        .args(["-r", "-8", "-q", "-b", "-o"])
        .arg(out)
        .arg(pst)
        .status()
        .map_err(|error| {
            format!(
                "readpst (libpst) is needed to read {}: {error}",
                pst.display()
            )
        })?;
    if !status.success() {
        return Err(format!("readpst failed on {}: {status}", pst.display()));
    }
    let mut folders = Vec::new();
    collect(out, out, &mut folders)?;
    folders.sort();
    Ok(folders)
}

fn collect(root: &Path, directory: &Path, folders: &mut Vec<PathBuf>) -> Result<(), String> {
    for entry in
        std::fs::read_dir(directory).map_err(|error| format!("{}: {error}", directory.display()))?
    {
        let path = entry.map_err(|error| error.to_string())?.path();
        if path.is_dir() {
            collect(root, &path, folders)?;
        } else if path.file_name().is_some_and(|name| name == "mbox") {
            let folder = path.parent().expect("a converted file has a parent");
            let named = if folder == root {
                root.join("root.mbox")
            } else {
                folder.with_extension("mbox")
            };
            if named.exists() {
                return Err(format!(
                    "converted folder paths collide at {}",
                    named.display()
                ));
            }
            std::fs::rename(&path, &named)
                .map_err(|error| format!("{}: {error}", named.display()))?;
            folders.push(named);
        }
    }
    Ok(())
}

fn scratch() -> Result<tempfile::TempDir, String> {
    tempfile::Builder::new()
        .prefix("sinex-outlook-")
        .tempdir()
        .map_err(|error| format!("Outlook staging: {error}"))
}

fn encode(folder: &Path) -> String {
    folder
        .as_os_str()
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn folder(binding: &Binding) -> Result<PathBuf, String> {
    let prefix = format!("{}:{}#", PST.name, binding.input.display());
    let suffix = format!("@{}", binding.settings.host);
    let encoded = binding
        .name
        .strip_prefix(&prefix)
        .and_then(|name| name.strip_suffix(&suffix))
        .ok_or_else(|| format!("{}: missing converted folder", binding.name))?;
    let (input_hash, encoded) = encoded
        .split_once(':')
        .ok_or("missing native input identity")?;
    if input_hash
        != blake3::hash(binding.input.as_os_str().as_bytes())
            .to_hex()
            .as_str()
    {
        return Err("converted folder belongs to another native input".into());
    }
    if encoded.len() % 2 != 0 || !encoded.is_ascii() {
        return Err("invalid converted folder identity".into());
    }
    let bytes = (0..encoded.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&encoded[index..index + 2], 16))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| "invalid converted folder identity")?;
    let path = PathBuf::from(std::ffi::OsString::from_vec(bytes));
    if path
        .components()
        .any(|part| !matches!(part, std::path::Component::Normal(_)))
    {
        return Err("invalid converted folder path".into());
    }
    Ok(path)
}

pub(super) fn plan(input: &Path, settings: &Settings) -> Result<Vec<Binding>, String> {
    plan_with(input, settings, Path::new("readpst"))
}

fn plan_with(input: &Path, settings: &Settings, converter: &Path) -> Result<Vec<Binding>, String> {
    let mut bindings = Vec::new();
    for pst in sinex_source_core::binding::files(input, true, |path| {
        path.extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("pst"))
    }) {
        let stage = scratch()?;
        for mbox in convert(&pst, stage.path(), converter)? {
            let relative = mbox
                .strip_prefix(stage.path())
                .map_err(|error| error.to_string())?;
            let identity = format!(
                "{}:{}",
                blake3::hash(pst.as_os_str().as_bytes()).to_hex(),
                encode(relative)
            );
            bindings.push(Binding::of(&PST, &pst, settings.clone()).named(&identity));
        }
    }
    Ok(bindings)
}

pub(super) fn reader(binding: &Binding) -> Result<Reading, String> {
    reader_with(binding, Path::new("readpst"))
}

fn reader_with(binding: &Binding, converter: &Path) -> Result<Reading, String> {
    Ok(Reading::Shared(Box::new(build_folder(binding, converter)?)))
}

fn build_folder(binding: &Binding, converter: &Path) -> Result<Folder, String> {
    let folder = folder(binding)?;
    let facts = sinex_source_core::drivers::file_facts(&binding.input)
        .map_err(|error| error.to_string())?;
    let version = Command::new(converter)
        .arg("-V")
        .output()
        .map_err(|error| format!("readpst version: {error}"))?;
    if !version.status.success() {
        return Err(format!("readpst version failed: {}", version.status));
    }
    let mut version_bytes = version.stdout;
    version_bytes.extend(version.stderr);
    let version =
        String::from_utf8(version_bytes).map_err(|error| format!("readpst version: {error}"))?;
    let version = version.trim();
    if version.is_empty() {
        return Err("readpst returned an empty version".into());
    }
    let stage = scratch()?;
    let converted = convert(&binding.input, stage.path(), converter)?;
    let after = sinex_source_core::drivers::file_facts(&binding.input)
        .map_err(|error| error.to_string())?;
    if !same_revision(&facts, &after) {
        return Err("PST changed during conversion; retry an unchanged revision".into());
    }
    let mbox = stage.path().join(&folder);
    if !converted.contains(&mbox) {
        return Err(format!("{}: converted folder disappeared", binding.name));
    }
    let acquisition = Acquisition {
        path: binding.input.clone(),
        facts,
        manifest: sinex_model::Manifest::from_json(serde_json::json!({
            "role": "original-archive", "path": binding.input.display().to_string(), "path_bytes": binding.input.as_os_str().as_bytes(), "media_type": "application/vnd.ms-outlook",
            "acquisition": {"tool": converter.display().to_string(), "version": version, "arguments": ["-r", "-8", "-q", "-b"], "folder": folder.display().to_string(), "folder_bytes": folder.as_os_str().as_bytes()}
        })),
    };
    Ok(Folder {
        driver: pst_driver(mbox, binding.settings.host.clone()),
        acquisition,
        binding: binding.clone(),
        converter: converter.to_owned(),
        _stage: stage,
    })
}

fn same_revision(a: &ObservationFacts, b: &ObservationFacts) -> bool {
    a.size == b.size
        && a.mtime_ns == b.mtime_ns
        && a.ctime_ns == b.ctime_ns
        && a.inode == b.inode
        && a.device == b.device
}

struct Folder {
    binding: Binding,
    converter: PathBuf,
    acquisition: Acquisition,
    driver: sinex_source_core::drivers::export_archive::ExportArchive,
    _stage: tempfile::TempDir,
}
impl SharedPass for Folder {
    fn refresh(&mut self) -> Result<(), DriverError> {
        let now = sinex_source_core::drivers::file_facts(&self.binding.input)
            .map_err(|error| DriverError::Io(error.to_string()))?;
        if !same_revision(&now, &self.acquisition.facts) {
            *self = build_folder(&self.binding, &self.converter).map_err(DriverError::Io)?;
        }
        Ok(())
    }
    fn acquisition(&self) -> Option<&Acquisition> {
        Some(&self.acquisition)
    }
    fn path(&self) -> &Path {
        self.driver.path()
    }
    fn resume(
        &self,
        cursor: &Value,
        facts: &ObservationFacts,
        class: Class,
    ) -> Result<SharedReadStart, DriverError> {
        self.driver.resume(cursor, facts, class)
    }
    fn route(&self, cursor: Value, class: Class) -> Route {
        self.driver.route(cursor, class)
    }
}

#[cfg(test)]
mod tests;
