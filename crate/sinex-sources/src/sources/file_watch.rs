//! Changes to files under watched directories, as the kernel reports them
//! (inotify through `notify`), on `LiveStream`. The kernel keeps no history of
//! these, so capture spools each change as it arrives.
//!
//! Capture watches each directory under the roots, except directories with a
//! declared name (`.git`, `target`, `node_modules`: churn nobody reads), and
//! watches directories created later as they appear. Symlinks are not
//! followed. A directory the kernel refuses to watch, because the host's
//! inotify watch limit is reached, is recorded as `unwatched` with the limit
//! named, so the gap shows instead of looking like silence.
//!
//! This records that a file changed, not what it contains; reading contents
//! is the directory-snapshot driver's job (it must also check that a symlink
//! inside a root does not lead outside it, `kv6l`).
use crate::drivers::live_stream::{self, LiveStream, Rotation, SpoolWriter};
use crate::registry::{Declared, Driver, SourceSpec};
use crate::{Parser, RecordError};
use notify::event::{
    AccessKind, AccessMode, CreateKind, EventKind, ModifyKind, RemoveKind, RenameMode,
};
use notify::{RecursiveMode, Watcher};
use serde_json::{Map, Value, json};
use sinex_model::kinds::machine::FileChanged;
use sinex_model::{Class, InputRef, Timestamp};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::{Duration, Instant};

pub const SPEC: SourceSpec = SourceSpec {
    name: "machine.file-watch",
    driver: Driver::LiveStream,
    consumer: "work episodes, project attribution and recall",
    // Paths name projects, people and documents.
    class: Class::Guarded,
    parser: "file-watch",
    parser_version: 1,
    event_types: &["file.changed"],
    framer: crate::drivers::append_file::lines,
};

/// Directory names skipped by default.
pub const SKIP: &[&str] = &[".git", "target", "node_modules", ".direnv", "__pycache__"];

pub struct Roots {
    pub roots: Vec<PathBuf>,
    /// Directory names not watched (at any depth).
    pub skip: Vec<String>,
}

fn path_json(path: &Path) -> Value {
    match std::str::from_utf8(path.as_os_str().as_bytes()) {
        Ok(text) => json!(text),
        Err(_) => {
            json!({"hex": path.as_os_str().as_bytes().iter().map(|b| format!("{b:02x}")).collect::<String>()})
        }
    }
}

/// The directories to watch under `dir`, skipping declared names and not
/// following symlinks. Directories that cannot be read are returned
/// separately, so capture can record them.
pub fn directories(dir: &Path, skip: &[String]) -> (Vec<PathBuf>, Vec<(PathBuf, String)>) {
    let mut out = Vec::new();
    let mut unreadable = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let entries = match std::fs::read_dir(&d) {
            Ok(e) => e,
            Err(e) => {
                unreadable.push((d, e.to_string()));
                continue;
            }
        };
        out.push(d);
        for entry in entries.flatten() {
            let Ok(kind) = entry.file_type() else {
                continue;
            };
            if kind.is_dir() && !kind.is_symlink() {
                let name = entry.file_name();
                if !skip.iter().any(|s| s.as_bytes() == name.as_bytes()) {
                    stack.push(entry.path());
                }
            }
        }
    }
    (out, unreadable)
}

/// The change, in this source's own stable words.
fn change_of(kind: &EventKind) -> Option<(&'static str, Option<bool>)> {
    Some(match kind {
        EventKind::Create(CreateKind::Folder) => ("created", Some(true)),
        EventKind::Create(CreateKind::File) => ("created", Some(false)),
        EventKind::Create(_) => ("created", None),
        EventKind::Remove(RemoveKind::Folder) => ("removed", Some(true)),
        EventKind::Remove(RemoveKind::File) => ("removed", Some(false)),
        EventKind::Remove(_) => ("removed", None),
        EventKind::Modify(ModifyKind::Name(RenameMode::Both)) => ("renamed", None),
        EventKind::Modify(ModifyKind::Name(RenameMode::From)) => ("moved_from", None),
        EventKind::Modify(ModifyKind::Name(RenameMode::To)) => ("moved_to", None),
        EventKind::Modify(ModifyKind::Name(_)) => ("renamed", None),
        EventKind::Modify(ModifyKind::Metadata(_)) => ("attributes", None),
        EventKind::Modify(_) => ("modified", None),
        EventKind::Access(AccessKind::Close(AccessMode::Write)) => ("written", Some(false)),
        // Opens and reads are not watched; anything else is kept as `other`.
        EventKind::Access(_) => return None,
        EventKind::Any | EventKind::Other => ("other", None),
    })
}

fn spool_line(spool: &mut SpoolWriter, line: &Value) -> std::io::Result<()> {
    spool.append(Timestamp::now(), line.to_string().as_bytes())
}

/// Watch `roots` and spool every change until `stop` elapses (forever when
/// `None`). Returns the number of records spooled.
pub fn capture(roots: &Roots, spool: &Path, stop: Option<Duration>) -> std::io::Result<u64> {
    let (tx, rx) = mpsc::channel::<notify::Result<notify::Event>>();
    let mut watcher = notify::recommended_watcher(tx).map_err(std::io::Error::other)?;
    let mut writer = SpoolWriter::open(spool, Rotation::default())?;
    let mut count = 0u64;
    let limit = std::fs::read_to_string("/proc/sys/fs/inotify/max_user_watches")
        .ok()
        .map(|s| s.trim().to_owned());
    let mut watch_tree =
        |dir: &Path, writer: &mut SpoolWriter, count: &mut u64| -> std::io::Result<()> {
            let (dirs, unreadable) = directories(dir, &roots.skip);
            for d in dirs {
                if let Err(e) = watcher.watch(&d, RecursiveMode::NonRecursive) {
                    let note = match e.kind {
                        notify::ErrorKind::MaxFilesWatch => format!(
                            "inotify watch limit reached (fs.inotify.max_user_watches = {})",
                            limit.as_deref().unwrap_or("unknown")
                        ),
                        _ => e.to_string(),
                    };
                    spool_line(
                        writer,
                        &json!({"kind": "unwatched", "paths": [path_json(&d)], "note": note}),
                    )?;
                    *count += 1;
                }
            }
            for (d, why) in unreadable {
                spool_line(
                    writer,
                    &json!({"kind": "unwatched", "paths": [path_json(&d)], "note": why}),
                )?;
                *count += 1;
            }
            Ok(())
        };
    for root in &roots.roots {
        watch_tree(root, &mut writer, &mut count)?;
    }
    writer.sync()?;
    let deadline = stop.map(|d| Instant::now() + d);
    let mut last_sync = Instant::now();
    loop {
        let wait = match deadline {
            Some(d) => match d.checked_duration_since(Instant::now()) {
                Some(left) => left.min(Duration::from_secs(1)),
                None => break,
            },
            None => Duration::from_secs(1),
        };
        match rx.recv_timeout(wait) {
            Ok(Ok(event)) => {
                let Some((change, is_dir)) = change_of(&event.kind) else {
                    continue;
                };
                let paths: Vec<Value> = event.paths.iter().map(|p| path_json(p)).collect();
                spool_line(
                    &mut writer,
                    &json!({"kind": change, "paths": paths, "is_dir": is_dir}),
                )?;
                count += 1;
                // A new directory (made here or moved in) gets its watches.
                if matches!(change, "created" | "moved_to" | "renamed")
                    && let Some(path) = event.paths.last()
                    && path.is_dir()
                    && !path.is_symlink()
                    && !path.file_name().is_some_and(|name| {
                        roots.skip.iter().any(|s| s.as_bytes() == name.as_bytes())
                    })
                {
                    watch_tree(path, &mut writer, &mut count)?;
                }
            }
            Ok(Err(e)) => {
                spool_line(
                    &mut writer,
                    &json!({"kind": "other", "paths": [], "note": e.to_string()}),
                )?;
                count += 1;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
        if last_sync.elapsed() >= Duration::from_secs(1) {
            writer.sync()?;
            last_sync = Instant::now();
        }
    }
    writer.sync()?;
    Ok(count)
}

pub struct FileWatchParser;

fn split_path(value: Option<&Value>) -> (Option<String>, Option<String>) {
    match value {
        Some(Value::String(s)) => (Some(s.clone()), None),
        Some(Value::Object(o)) => (
            None,
            o.get("hex").and_then(Value::as_str).map(str::to_owned),
        ),
        _ => (None, None),
    }
}

impl Parser for FileWatchParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = FileChanged;

    fn parse(
        &self,
        record: &Map<String, Value>,
        _: &(),
    ) -> Result<(Vec<FileChanged>, ()), RecordError> {
        let (received_at, line) = live_stream::received(record).map_err(RecordError::new)?;
        let change: Map<String, Value> = serde_json::from_slice(&line)
            .map_err(|e| RecordError::new(format!("watch record is not JSON: {e}")))?;
        let kind = change
            .get("kind")
            .and_then(Value::as_str)
            .ok_or_else(|| RecordError::new("watch record has no kind"))?
            .to_owned();
        let paths = change
            .get("paths")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let is_dir = change.get("is_dir").and_then(Value::as_bool);
        let note = change
            .get("note")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let event = |path: Option<&Value>, to: Option<String>| {
            let (path, path_hex) = split_path(path);
            FileChanged {
                received_at,
                change: kind.clone(),
                path,
                path_hex,
                to,
                is_dir,
                note: note.clone(),
            }
        };
        let out = if kind == "renamed" && paths.len() == 2 {
            vec![event(paths.first(), split_path(paths.get(1)).0)]
        } else if paths.is_empty() {
            vec![event(None, None)]
        } else {
            paths.iter().map(|p| event(Some(p), None)).collect()
        };
        Ok((out, ()))
    }
}

pub fn source() -> Declared<FileWatchParser> {
    Declared {
        spec: SPEC,
        parser: FileWatchParser,
    }
}

pub fn driver(spool: PathBuf, host: String) -> LiveStream {
    LiveStream {
        spool,
        input: InputRef {
            kind: "inotify".into(),
            locator: "file-watch".into(),
            host,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp() -> PathBuf {
        let d = std::env::temp_dir().join(format!("sinex-fw-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn rec(v: Value) -> Map<String, Value> {
        serde_json::from_slice(&live_stream::frame(
            Timestamp::now(),
            v.to_string().as_bytes(),
        ))
        .unwrap()
    }

    #[test]
    fn directories_skip_declared_names_and_symlinks() {
        let root = temp();
        std::fs::create_dir_all(root.join("a/b")).unwrap();
        std::fs::create_dir_all(root.join(".git/objects")).unwrap();
        std::os::unix::fs::symlink("/", root.join("escape")).unwrap();
        let (dirs, _) = directories(&root, &[".git".to_owned()]);
        let mut rel: Vec<_> = dirs
            .iter()
            .map(|d| d.strip_prefix(&root).unwrap().to_owned())
            .collect();
        rel.sort();
        assert_eq!(
            rel,
            vec![PathBuf::from(""), PathBuf::from("a"), PathBuf::from("a/b")]
        );
    }

    #[test]
    fn renames_keep_both_ends_and_odd_paths_survive() {
        let (out, ()) = FileWatchParser
            .parse(
                &rec(json!({"kind": "renamed", "paths": ["/a/x", "/a/y"], "is_dir": null})),
                &(),
            )
            .unwrap();
        assert_eq!(out[0].path.as_deref(), Some("/a/x"));
        assert_eq!(out[0].to.as_deref(), Some("/a/y"));
        let (out, ()) = FileWatchParser
            .parse(
                &rec(json!({"kind": "created", "paths": [{"hex": "2fff"}], "is_dir": false})),
                &(),
            )
            .unwrap();
        assert_eq!(
            (out[0].path.clone(), out[0].path_hex.as_deref()),
            (None, Some("2fff"))
        );
        let (out, ()) = FileWatchParser
            .parse(&rec(json!({"kind": "unwatched", "paths": ["/big"], "note": "inotify watch limit reached"})), &())
            .unwrap();
        assert_eq!(out[0].note.as_deref(), Some("inotify watch limit reached"));
    }

    #[test]
    fn capture_sees_changes_including_in_new_directories() {
        let root = temp();
        let spool = temp().join("spool");
        let roots = Roots {
            roots: vec![root.clone()],
            skip: vec!["target".into()],
        };
        let r2 = root.clone();
        let writer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(300));
            std::fs::write(r2.join("f"), b"x").unwrap();
            std::fs::create_dir(r2.join("new")).unwrap();
            std::thread::sleep(Duration::from_millis(300));
            std::fs::write(r2.join("new/g"), b"y").unwrap();
            std::fs::create_dir(r2.join("target")).unwrap();
            std::thread::sleep(Duration::from_millis(300));
            std::fs::write(r2.join("target/ignored"), b"z").unwrap();
        });
        capture(&roots, &spool, Some(Duration::from_millis(1500))).unwrap();
        writer.join().unwrap();
        let acquired = driver(spool, "h".into()).poll(&Value::Null).unwrap();
        let m = acquired.material.unwrap();
        let mut seen = Vec::new();
        for (s, e) in &m.records {
            let r: Map<String, Value> = serde_json::from_slice(&m.bytes[*s..*e]).unwrap();
            for c in FileWatchParser.parse(&r, &()).unwrap().0 {
                seen.push((c.change, c.path.unwrap_or_default()));
            }
        }
        let has = |change: &str, suffix: &str| {
            seen.iter().any(|(c, p)| c == change && p.ends_with(suffix))
        };
        assert!(has("created", "/f"), "{seen:?}");
        assert!(has("created", "/new"), "{seen:?}");
        assert!(
            has("created", "/new/g"),
            "a file in a directory made after capture started: {seen:?}"
        );
        assert!(
            !seen.iter().any(|(_, p)| p.ends_with("target/ignored")),
            "{seen:?}"
        );
    }
}
