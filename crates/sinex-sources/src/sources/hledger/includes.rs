//! Include planning reads local files only. Every file uses the shared export
//! reader, so edits to a child are acquired independently of its parent's bytes.
use super::SPEC;
use serde_json::json;
use sinex_model::event_types::finance::{IncludeTarget, IncludedFrom};
use sinex_source_core::binding::{Binding, Settings};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

pub(super) fn plan(input: &Path, settings: &Settings) -> Result<Vec<Binding>, String> {
    if settings
        .option("journal_format")
        .is_some_and(|format| !matches!(format, "journal" | "timeclock" | "timedot"))
    {
        return Err("unknown native journal format".into());
    }
    let mut out = Vec::new();
    let mut active = BTreeSet::new();
    let roots = if input.is_dir() {
        journals(input)?
    } else {
        vec![input.to_path_buf()]
    };
    for root in roots {
        visit(&root, &root, settings, None, None, &mut active, &mut out)?;
    }
    Ok(out)
}

fn journals(root: &Path) -> Result<Vec<PathBuf>, String> {
    let mut found = Vec::new();
    let mut dirs = vec![root.to_path_buf()];
    let mut seen = BTreeSet::new();
    while let Some(dir) = dirs.pop() {
        if !seen.insert(std::fs::canonicalize(&dir).map_err(|e| e.to_string())?) {
            continue;
        }
        for entry in std::fs::read_dir(&dir).map_err(|e| e.to_string())? {
            let path = entry.map_err(|e| e.to_string())?.path();
            if path.is_dir() {
                dirs.push(path);
            } else if matches!(
                path.extension().and_then(|s| s.to_str()),
                Some("journal" | "hledger")
            ) || path.file_name().is_some_and(|s| s == "journal_clean")
            {
                found.push(path);
            }
        }
    }
    found.sort();
    Ok(found)
}

fn visit(
    path: &Path,
    journal: &Path,
    settings: &Settings,
    initial_year: Option<i16>,
    from: Option<IncludedFrom>,
    active: &mut BTreeSet<PathBuf>,
    out: &mut Vec<Binding>,
) -> Result<(), String> {
    let identity = std::fs::canonicalize(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut file = std::fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut prefix = Vec::new();
    file.by_ref()
        .take(512)
        .read_to_end(&mut prefix)
        .map_err(|e| e.to_string())?;
    let archive = prefix.starts_with(&[0x1f, 0x8b])
        || prefix.starts_with(b"PK\x03\x04")
        || prefix.starts_with(b"PK\x05\x06")
        || prefix.get(257..262) == Some(b"ustar");
    active.insert(identity.clone());
    let result = (|| -> Result<(), String> {
        let mut includes = BTreeMap::<String, Vec<IncludeTarget>>::new();
        let mut year = initial_year;
        let mut counts = BTreeMap::<String, u64>::new();
        // Archives are read by the shared archive reader. Native journal include
        // paths refer to the local filesystem, never to an extracted temporary tree.
        if !archive && settings.option("journal_format").unwrap_or("journal") == "journal" {
            file.rewind().map_err(|e| e.to_string())?;
            let ranges = SPEC.framer.ranges(&mut file).map_err(|e| e.to_string())?;
            for (start, end) in ranges {
                file.seek(SeekFrom::Start(start as u64))
                    .map_err(|e| e.to_string())?;
                let mut header = String::new();
                let read =
                    BufReader::new(file.by_ref().take((end - start) as u64)).read_line(&mut header);
                if let Err(error) = read {
                    if error.kind() == std::io::ErrorKind::InvalidData {
                        continue;
                    }
                    return Err(error.to_string());
                }
                let header = header.trim();
                if let Some(value) = header
                    .strip_prefix('Y')
                    .or_else(|| header.strip_prefix("year "))
                    .or_else(|| header.strip_prefix("apply year "))
                    && let Ok(value) = super::split_comment(value).0.trim().parse::<i16>()
                {
                    year = Some(value);
                }
                let Some(target) = header
                    .strip_prefix("include ")
                    .or_else(|| header.strip_prefix("include\t"))
                else {
                    continue;
                };
                file.seek(SeekFrom::Start(start as u64))
                    .map_err(|e| e.to_string())?;
                let mut hash = blake3::Hasher::new();
                let mut block = file.by_ref().take((end - start) as u64);
                let mut buffer = [0; 8192];
                loop {
                    let count = block.read(&mut buffer).map_err(|e| e.to_string())?;
                    if count == 0 {
                        break;
                    }
                    hash.update(&buffer[..count]);
                }
                let hash = hash.finalize().to_hex().to_string();
                let duplicate = counts.entry(hash.clone()).or_default();
                let key = format!("{hash}:{}", *duplicate);
                let from = IncludedFrom {
                    path: path.display().to_string(),
                    block_hash: hash.clone(),
                    duplicate: *duplicate,
                    parent: from.clone().map(Box::new),
                };
                *duplicate += 1;
                let targets = match resolve(path, target.trim(), settings) {
                    Ok(targets) => targets,
                    Err(reason) => {
                        includes.entry(key).or_default().push(IncludeTarget {
                            path: target.trim().into(),
                            reason: Some(reason),
                        });
                        continue;
                    }
                };
                for target in targets {
                    let reason = match std::fs::canonicalize(&target) {
                        Err(e) => Some(format!("{}: {e}", target.display())),
                        Ok(id) if active.contains(&id) => Some("include cycle".into()),
                        Ok(_) => {
                            let format = target_format(header, &target);
                            let mut child_settings = settings.clone();
                            let mut options = child_settings
                                .options
                                .as_object()
                                .cloned()
                                .unwrap_or_default();
                            options.insert("journal_format".into(), json!(format));
                            child_settings.options = options.into();
                            visit(
                                &target,
                                journal,
                                &child_settings,
                                year,
                                Some(from.clone()),
                                active,
                                out,
                            )
                            .err()
                        }
                    };
                    includes
                        .entry(key.clone())
                        .or_default()
                        .push(IncludeTarget {
                            path: target.display().to_string(),
                            reason,
                        });
                }
            }
        }
        let mut options = settings.options.as_object().cloned().unwrap_or_default();
        options.insert("journal".into(), json!(journal.display().to_string()));
        options.insert("initial_year".into(), json!(initial_year));
        options.insert("included_from".into(), json!(from));
        options.insert("includes".into(), json!(includes));
        let mut binding = Binding::of(
            &SPEC,
            path,
            Settings {
                options: options.into(),
                ..settings.clone()
            },
        );
        if let Some(from) = from {
            let parent = blake3::hash(
                serde_json::to_string(&(from, initial_year))
                    .unwrap()
                    .as_bytes(),
            )
            .to_hex();
            binding = binding.named(&format!("include:{parent}"));
        }
        out.push(binding);
        Ok(())
    })();
    active.remove(&identity);
    result
}

fn target_format(header: &str, path: &Path) -> &'static str {
    let target = header
        .strip_prefix("include")
        .unwrap_or(header)
        .trim_start();
    for format in ["journal", "timeclock", "timedot"] {
        if target
            .strip_prefix(format)
            .is_some_and(|rest| rest.starts_with(':'))
        {
            return format;
        }
    }
    match path.extension().and_then(|s| s.to_str()) {
        Some("timeclock") => "timeclock",
        Some("timedot") => "timedot",
        _ => "journal",
    }
}

fn resolve(parent: &Path, target: &str, settings: &Settings) -> Result<Vec<PathBuf>, String> {
    if target.starts_with(['|', '!', '<']) || target.contains("://") {
        return Err(
            "include requires a local file; commands and remote imports are not executed".into(),
        );
    }
    let target = if let Some(target) = target
        .strip_prefix("journal:")
        .or_else(|| target.strip_prefix("timeclock:"))
        .or_else(|| target.strip_prefix("timedot:"))
    {
        target
    } else {
        if let Some((format, _)) = target.split_once(':')
            && matches!(format, "csv" | "ssv" | "tsv" | "rules")
        {
            return Err(format!("{format} includes are not journal records"));
        }
        target
    };
    let parent = if parent.is_absolute() {
        parent.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|e| e.to_string())?
            .join(parent)
    };
    let path = if let Some(rest) = target.strip_prefix("~/") {
        let home = match settings.options.get("include_home") {
            Some(home) => home.as_str().map(PathBuf::from),
            None => std::env::var_os("HOME").map(PathBuf::from),
        };
        home.ok_or("include home directory is unavailable")?
            .join(rest)
    } else {
        parent.parent().unwrap_or(Path::new(".")).join(target)
    };
    if !target.contains(['*', '?', '[']) {
        return Ok(vec![path]);
    }
    let pattern = path.to_str().ok_or("include glob is not UTF-8")?;
    let first = pattern.find(['*', '?', '[']).unwrap();
    let slash = pattern[..first].rfind('/').unwrap_or(0);
    let root = Path::new(&pattern[..slash.max(1)]);
    let expression = glob_expression(pattern)?;
    let matcher = regex::Regex::new(&expression).map_err(|e| e.to_string())?;
    let hidden_names = pattern
        .match_indices("/.")
        .map(|(at, _)| {
            let end = pattern[at + 1..]
                .find('/')
                .map_or(pattern.len(), |end| at + 1 + end);
            regex::Regex::new(&glob_expression(&pattern[..end])?).map_err(|e| e.to_string())
        })
        .collect::<Result<Vec<_>, String>>()?;
    let current = std::fs::canonicalize(&parent).map_err(|e| e.to_string())?;
    let mut paths = Vec::new();
    let mut dirs = vec![root.to_path_buf()];
    let mut seen = BTreeSet::new();
    while let Some(dir) = dirs.pop() {
        let id = std::fs::canonicalize(&dir).map_err(|e| e.to_string())?;
        if !seen.insert(id) {
            continue;
        }
        for entry in std::fs::read_dir(&dir).map_err(|e| e.to_string())? {
            let path = entry.map_err(|e| e.to_string())?.path();
            if path
                .file_name()
                .is_some_and(|s| s.as_encoded_bytes().starts_with(b"."))
            {
                let relative = path.strip_prefix(root).map_err(|e| e.to_string())?;
                let literal_hidden = path
                    .to_str()
                    .is_some_and(|path| hidden_names.iter().any(|name| name.is_match(path)));
                let nested_directory =
                    pattern.contains("**") && path.is_dir() && relative.components().count() > 1;
                if !literal_hidden && !nested_directory {
                    continue;
                }
            }
            if path.is_dir() {
                if pattern.contains("**")
                    || dir.components().count() < Path::new(pattern).components().count() - 1
                {
                    dirs.push(path);
                }
            } else if path.to_str().is_some_and(|s| matcher.is_match(s))
                && std::fs::canonicalize(&path).map_err(|e| e.to_string())? != current
            {
                paths.push(path);
            }
        }
    }
    paths.sort();
    if paths.is_empty() {
        return Err("include glob matches no other files".into());
    }
    Ok(paths)
}

fn glob_expression(pattern: &str) -> Result<String, String> {
    let mut out = String::from("^");
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '*' if chars.peek() == Some(&'*') => {
                chars.next();
                if chars.peek() == Some(&'/') {
                    chars.next();
                    out.push_str("(?:.*/)?");
                } else {
                    out.push_str(".*");
                }
            }
            '*' => out.push_str("[^/]*"),
            '?' => out.push_str("[^/]"),
            '[' => {
                out.push('[');
                if chars.peek() == Some(&'!') {
                    chars.next();
                    out.push('^');
                }
                let mut closed = false;
                for c in chars.by_ref() {
                    out.push(c);
                    if c == ']' {
                        closed = true;
                        break;
                    }
                }
                if !closed {
                    return Err("unfinished include glob character class".into());
                }
            }
            _ => out.push_str(&regex::escape(&c.to_string())),
        }
    }
    out.push('$');
    Ok(out)
}

#[cfg(test)]
mod tests;
