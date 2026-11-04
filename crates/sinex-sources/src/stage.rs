//! Planning a stage: which parser reads each staged file, and the inventory
//! of everything each file holds, known before any record is parsed.
//!
//! Selection is strict ([`sinex_source_core::stage`]). With `--parser NAME`
//! the named format plans the path, as at a canonical location. Without it,
//! every file is tested against the structures formats declare: exactly one
//! match assigns the parser; none, or several, store the file as received
//! with no parser and the matching formats listed as candidates. An archive is
//! decided member by member: each member is read by the one source whose
//! binding selects it, and a member two sources would both read is read by
//! neither, so a source contesting any member reads nothing from that
//! archive. Nothing is guessed from a file name. Candidates are always format
//! names, the names `--parser` takes.
use serde::{Deserialize, Serialize};
use sinex_source_core::binding::{Binding, InputFormat, Reading, Settings};
use sinex_source_core::drivers::export_archive::Container;
use sinex_source_core::shared_pass::{ListedMember, members};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// One path given to `stage`, with the parser named for it, if any.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Item {
    pub path: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parser: Option<String>,
}

/// Every staged file, in the order given (a directory's files in name order).
#[derive(Debug, Default, Serialize)]
pub struct StagePlan {
    pub files: Vec<StagedFile>,
}

/// One staged file (or a directory a named format reads as one input): how
/// it is read and what it holds.
#[derive(Debug, Serialize)]
pub struct StagedFile {
    pub path: PathBuf,
    /// The archive container, for a file that is one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub container: Option<Container>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    pub reading: FileReading,
    /// Each member exactly once, in the archive's own order; a file that is
    /// not an archive is its own one member.
    pub members: Vec<InventoryMember>,
    /// The bindings that read the file, by format.
    #[serde(skip)]
    pub bindings: Vec<(&'static InputFormat, Binding)>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "as", rename_all = "snake_case")]
pub enum FileReading {
    /// Read by these formats' bindings.
    Parsed { formats: Vec<String> },
    /// Stored as received with no parser. `candidates` names the formats that
    /// recognised it, or would each read a member of it, when more than one
    /// did.
    Unparsed { candidates: Vec<String> },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct InventoryMember {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(flatten)]
    pub disposition: Disposition,
}

/// What became of one member. Every member is in exactly one of these.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "disposition", rename_all = "snake_case")]
pub enum Disposition {
    /// Read by these sources' bindings.
    Read { sources: Vec<String> },
    /// The binding meant to read it failed; its bytes stay stored.
    Failed {
        sources: Vec<String>,
        reason: String,
    },
    /// Kept in the stored original and read by nothing. `candidates` names
    /// the formats whose sources would each read it, when several would.
    StoredNotInterpreted { candidates: Vec<String> },
}

impl StagePlan {
    /// Every binding of every file, by format.
    pub fn bindings(&self) -> impl Iterator<Item = &(&'static InputFormat, Binding)> {
        self.files.iter().flat_map(|file| file.bindings.iter())
    }
}

impl StagedFile {
    /// The binding of `source` failed with `reason`: its members are failed.
    pub fn mark_failed(&mut self, source: &str, reason: &str) {
        for member in &mut self.members {
            if let Disposition::Read { sources } = &member.disposition
                && sources.iter().any(|s| s == source)
            {
                member.disposition = Disposition::Failed {
                    sources: sources.clone(),
                    reason: reason.to_owned(),
                };
            }
        }
    }
}

/// Plan `items` against `formats` (the registry's in production). `settings`
/// are recorded with every planned binding.
pub fn plan(
    formats: &[&'static InputFormat],
    items: &[Item],
    settings: &Settings,
) -> Result<StagePlan, String> {
    let mut files = Vec::new();
    for item in items {
        if std::fs::metadata(&item.path).is_err() {
            return Err(format!(
                "{}: no such file or directory",
                item.path.display()
            ));
        }
        match &item.parser {
            Some(name) => files.extend(named(formats, name, &item.path, settings)?),
            None => {
                for path in sinex_source_core::binding::files(
                    &item.path,
                    true,
                    sinex_source_core::binding::any,
                ) {
                    files.extend(detect(formats, &path, settings)?);
                }
            }
        }
    }
    Ok(StagePlan { files })
}

/// The formats that plan inputs from a path.
pub fn planned_format_names(formats: &[&'static InputFormat]) -> Vec<&'static str> {
    formats
        .iter()
        .filter(|f| f.plan.is_some())
        .map(|f| f.name)
        .collect()
}

/// `path` planned by the format named `name`, one staged file per input its
/// bindings read.
fn named(
    formats: &[&'static InputFormat],
    name: &str,
    path: &Path,
    settings: &Settings,
) -> Result<Vec<StagedFile>, String> {
    let format = formats
        .iter()
        .copied()
        .find(|f| f.name == name)
        .ok_or_else(|| {
            format!(
                "no format {name}; one of {}",
                planned_format_names(formats).join(", ")
            )
        })?;
    let plan = format
        .plan
        .ok_or_else(|| format!("{name} is not read from a path"))?;
    let bindings = plan(path, settings)?;
    if bindings.is_empty() {
        return Err(format!(
            "{name} finds nothing to read in {}",
            path.display()
        ));
    }
    let mut inputs: Vec<PathBuf> = Vec::new();
    for binding in &bindings {
        if !inputs.contains(&binding.input) {
            inputs.push(binding.input.clone());
        }
    }
    let mut out = Vec::new();
    for input in inputs {
        let own: Vec<(&'static InputFormat, Binding)> = bindings
            .iter()
            .filter(|b| b.input == input)
            .map(|b| (format, b.clone()))
            .collect();
        let (container, listed) = listing(&input)?;
        // A driver reads a compressed or plain file whole, whatever its
        // selection says; only an archive's members are selected.
        let archive = is_archive(container);
        let selections = own
            .iter()
            .map(|(format, binding)| {
                let selection = if archive {
                    selection(format, binding)?
                } else {
                    None
                };
                Ok((binding.source.clone(), selection))
            })
            .collect::<Result<Vec<_>, String>>()?;
        let members = listed
            .into_iter()
            .map(|member| {
                let sources = selecting(&selections, &member.name);
                InventoryMember {
                    name: member.name,
                    size: member.size,
                    disposition: if sources.is_empty() {
                        Disposition::StoredNotInterpreted { candidates: vec![] }
                    } else {
                        Disposition::Read { sources }
                    },
                }
            })
            .collect();
        out.push(StagedFile {
            size: size(&input),
            path: input,
            container,
            reading: FileReading::Parsed {
                formats: vec![name.to_owned()],
            },
            members,
            bindings: own,
        });
    }
    Ok(out)
}

/// One file tested against every declared structure. A format that
/// recognises the file plans it, and its plan may add inputs the file names
/// (a ledger's includes).
fn detect(
    formats: &[&'static InputFormat],
    path: &Path,
    settings: &Settings,
) -> Result<Vec<StagedFile>, String> {
    let (container, listed) = listing(path)?;
    let archive = is_archive(container);
    let mut whole_file: Vec<&'static InputFormat> = Vec::new();
    let mut archive_formats: Vec<&'static InputFormat> = Vec::new();
    for format in formats.iter().copied().filter(|f| f.plan.is_some()) {
        for structure in format.accepts {
            let matched = if structure.is_archive_layout() {
                archive && listed.iter().any(|m| structure.matches_member(&m.name))
            } else {
                structure.matches_file(path)?
            };
            if matched {
                let list = if structure.is_archive_layout() {
                    &mut archive_formats
                } else {
                    &mut whole_file
                };
                if !list.iter().any(|known| known.name == format.name) {
                    list.push(format);
                }
            }
        }
    }
    let names = |formats: &[&'static InputFormat]| -> Vec<String> {
        formats.iter().map(|f| f.name.to_owned()).collect()
    };
    let unparsed = |candidates: Vec<String>, listed: Vec<ListedMember>| StagedFile {
        path: path.to_path_buf(),
        container,
        size: size(path),
        reading: FileReading::Unparsed {
            candidates: candidates.clone(),
        },
        members: listed
            .into_iter()
            .map(|m| InventoryMember {
                name: m.name,
                size: m.size,
                disposition: Disposition::StoredNotInterpreted {
                    candidates: if archive { vec![] } else { candidates.clone() },
                },
            })
            .collect(),
        bindings: vec![],
    };
    match (whole_file.as_slice(), archive_formats.is_empty()) {
        ([format], true) => named(formats, format.name, path, settings),
        ([], false) => Ok(vec![by_member(
            &archive_formats,
            path,
            container,
            listed,
            settings,
        )?]),
        _ => {
            let mut candidates: Vec<String> = names(&whole_file);
            candidates.extend(names(&archive_formats));
            candidates.sort();
            candidates.dedup();
            Ok(vec![unparsed(candidates, listed)])
        }
    }
}

/// An archive some formats' layouts recognised, decided member by member.
fn by_member(
    archive_formats: &[&'static InputFormat],
    path: &Path,
    container: Option<Container>,
    listed: Vec<ListedMember>,
    settings: &Settings,
) -> Result<StagedFile, String> {
    let mut bindings = Vec::new();
    for format in archive_formats.iter().copied() {
        let plan = format.plan.expect("only planned formats are detected");
        for binding in plan(path, settings)? {
            let selection = selection(format, &binding)?;
            bindings.push((format, binding, selection));
        }
    }
    let selections: Vec<(String, Option<fn(&str) -> bool>)> = bindings
        .iter()
        .map(|(_, binding, selection)| (binding.source.clone(), *selection))
        .collect();
    // The format each source is planned by, for candidates.
    let format_of = |source: &String| -> String {
        bindings
            .iter()
            .find(|(_, binding, _)| &binding.source == source)
            .map(|(format, _, _)| format.name.to_owned())
            .expect("every selecting source has a binding")
    };
    let formats_of = |sources: &[String]| -> Vec<String> {
        let formats: BTreeSet<String> = sources.iter().map(format_of).collect();
        formats.into_iter().collect()
    };
    let per_member: Vec<Vec<String>> = listed
        .iter()
        .map(|member| selecting(&selections, &member.name))
        .collect();
    let contested: BTreeSet<&String> = per_member
        .iter()
        .filter(|sources| sources.len() > 1)
        .flatten()
        .collect();
    let readers: BTreeSet<&String> = per_member
        .iter()
        .filter(|sources| sources.len() == 1)
        .flatten()
        .filter(|source| !contested.contains(source))
        .collect();
    let members: Vec<InventoryMember> = listed
        .iter()
        .zip(&per_member)
        .map(|(member, sources)| InventoryMember {
            name: member.name.clone(),
            size: member.size,
            disposition: match sources.as_slice() {
                [source] if readers.contains(source) => Disposition::Read {
                    sources: sources.clone(),
                },
                _ => Disposition::StoredNotInterpreted {
                    candidates: if sources.len() > 1 {
                        formats_of(sources)
                    } else {
                        vec![]
                    },
                },
            },
        })
        .collect();
    let contested: Vec<String> = contested.into_iter().cloned().collect();
    let contested_formats = formats_of(&contested);
    let kept: Vec<(&'static InputFormat, Binding)> = bindings
        .into_iter()
        .filter(|(_, binding, _)| readers.contains(&binding.source))
        .map(|(format, binding, _)| (format, binding))
        .collect();
    let reading = if kept.is_empty() {
        FileReading::Unparsed {
            candidates: contested_formats,
        }
    } else {
        let mut formats: Vec<String> = Vec::new();
        for (format, _) in &kept {
            if !formats.iter().any(|f| f == format.name) {
                formats.push(format.name.to_owned());
            }
        }
        FileReading::Parsed { formats }
    };
    Ok(StagedFile {
        path: path.to_path_buf(),
        container,
        size: size(path),
        reading,
        members,
        bindings: kept,
    })
}

/// The members a binding reads: `Some` for an archive binding's selection,
/// `None` when it reads its whole input.
fn selection(format: &InputFormat, binding: &Binding) -> Result<Option<fn(&str) -> bool>, String> {
    let reader = format
        .reader
        .ok_or_else(|| format!("{} is not read by a driver", format.name))?;
    Ok(match reader(binding)? {
        Reading::Shared(pass) => pass.member_selection(),
        Reading::Polled(_) => None,
    })
}

/// The sources whose selection reads `member`, each once.
fn selecting(selections: &[(String, Option<fn(&str) -> bool>)], member: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for (source, selection) in selections {
        if selection.is_none_or(|select| select(member)) && !out.contains(source) {
            out.push(source.clone());
        }
    }
    out
}

/// The archive container and members of `path`; a directory, a file that is
/// not an archive, or an input a plan names that does not exist yet is its
/// own one member.
fn listing(path: &Path) -> Result<(Option<Container>, Vec<ListedMember>), String> {
    let whole = || ListedMember {
        name: path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| path.display().to_string()),
        size: size(path),
    };
    if !path.is_file() {
        return Ok((None, vec![whole()]));
    }
    let (container, listed) = members(path).map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(match container {
        Container::Zip | Container::Tar | Container::TarGzip => (Some(container), listed),
        Container::Gzip => (Some(container), vec![whole()]),
        Container::Stateless => (None, vec![whole()]),
    })
}

fn is_archive(container: Option<Container>) -> bool {
    matches!(
        container,
        Some(Container::Zip | Container::Tar | Container::TarGzip)
    )
}

fn size(path: &Path) -> Option<u64> {
    std::fs::metadata(path)
        .ok()
        .filter(|m| m.is_file())
        .map(|m| m.len())
}

#[cfg(test)]
mod tests;
