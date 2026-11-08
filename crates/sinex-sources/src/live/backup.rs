//! Copy producer state while publication and spool retirement are paused.
use super::*;
use notify::{RecursiveMode, Watcher};
use sinex_transport::backup::{Captured, capture_tree};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
};

/// Audio and screen capture publish one completed pair by renaming data last.
/// Latch only generations present now. Subsequent capture continues into new
/// generations, which cannot extend this wait indefinitely.
fn latch_media(
    bindings: &[Binding],
) -> Result<
    (
        notify::RecommendedWatcher,
        std::sync::mpsc::Receiver<notify::Result<notify::Event>>,
        BTreeMap<PathBuf, Vec<String>>,
    ),
    String,
> {
    let (send, receive) = std::sync::mpsc::sync_channel(1);
    let mut watcher = notify::recommended_watcher(move |event| {
        let _ = send.try_send(event);
    })
    .map_err(|e| e.to_string())?;
    let mut latched = BTreeMap::<PathBuf, Vec<String>>::new();
    for binding in bindings
        .iter()
        .filter(|binding| binding.source == "session.media")
    {
        watcher
            .watch(&binding.input, RecursiveMode::NonRecursive)
            .map_err(|e| e.to_string())?;
        let mut generations = Vec::new();
        for entry in std::fs::read_dir(&binding.input).map_err(|e| e.to_string())? {
            let entry = entry.map_err(|e| e.to_string())?;
            let name = entry
                .file_name()
                .to_str()
                .ok_or("non UTF-8 media generation")?
                .to_owned();
            if let Some(name) = name.strip_suffix(".partial") {
                // Audio's .<started>.wav/ogg.partial and screen's complete stem
                // retain the same started identity through their final rename.
                generations.push(
                    name.trim_start_matches('.')
                        .split('.')
                        .next()
                        .ok_or("missing media generation")?
                        .to_owned(),
                );
            }
        }
        generations.sort();
        generations.dedup();
        latched.insert(binding.input.clone(), generations);
    }
    Ok((watcher, receive, latched))
}
fn seal_media(bindings: &[Binding], cancelled: &AtomicBool) -> Result<(), String> {
    let (_watcher, receive, latched) = latch_media(bindings)?;
    wait_media(&latched, &receive, cancelled)
}
fn wait_media(
    latched: &BTreeMap<PathBuf, Vec<String>>,
    receive: &std::sync::mpsc::Receiver<notify::Result<notify::Event>>,
    cancelled: &AtomicBool,
) -> Result<(), String> {
    loop {
        if cancelled.load(Ordering::Acquire) {
            return Err("producer capture cancelled while sealing media".into());
        }
        let mut complete = true;
        for (spool, generations) in latched {
            let ready =
                sinex_source_core::drivers::media::complete(spool).map_err(|e| e.to_string())?;
            for generation in generations {
                if ready.iter().any(|(stem, _)| {
                    stem.starts_with(&format!("{generation}."))
                        || stem.starts_with(&format!("{generation}-"))
                }) {
                    continue;
                }
                complete = false;
                let present = std::fs::read_dir(spool)
                    .map_err(|e| e.to_string())?
                    .filter_map(Result::ok)
                    .any(|entry| {
                        let name = entry.file_name();
                        let name = name.to_string_lossy();
                        name.ends_with(".partial")
                            && name
                                .trim_start_matches('.')
                                .starts_with(&format!("{generation}."))
                    });
                if !present {
                    return Err(format!(
                        "media generation {generation} disappeared before sealing"
                    ));
                }
                let next_generation = std::fs::read_dir(spool)
                    .map_err(|e| e.to_string())?
                    .filter_map(Result::ok)
                    .any(|entry| {
                        let name = entry.file_name();
                        let name = name.to_string_lossy();
                        if !name.ends_with(".partial") {
                            return false;
                        }
                        let current = name
                            .trim_start_matches('.')
                            .split('.')
                            .next()
                            .unwrap_or_default();
                        !generations.iter().any(|latched| latched == current)
                    });
                if next_generation {
                    return Err(format!(
                        "media generation {generation} did not seal before capture advanced to another generation"
                    ));
                }
            }
        }
        if complete {
            return Ok(());
        }
        loop {
            if cancelled.load(Ordering::Acquire) {
                return Err("producer capture cancelled while sealing media".into());
            }
            match receive.recv_timeout(Duration::from_secs(1)) {
                Ok(Ok(_)) => break,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                Ok(Err(error)) => return Err(error.to_string()),
                Err(error) => return Err(error.to_string()),
            }
        }
    }
}

pub(super) fn capture(
    positions: Option<&Path>,
    bindings: &[Binding],
    cancelled: &AtomicBool,
) -> Result<Captured, String> {
    seal_media(bindings, cancelled)?;
    let directory = tempfile::tempdir().map_err(|e| e.to_string())?;
    let mut files = Vec::new();
    let mut add = |source: &Path, relative: &str| -> Result<(), String> {
        let mut copied = capture_tree(source, &directory.path().join(relative), cancelled)?;
        for file in &mut copied {
            file.path = format!("{relative}/{}", file.path);
        }
        files.extend(copied);
        Ok(())
    };
    if let Some(positions) = positions {
        std::fs::create_dir_all(positions).map_err(|e| e.to_string())?;
        add(positions, "positions")?;
    }
    for (index, binding) in bindings.iter().enumerate() {
        if cancelled.load(Ordering::Acquire) {
            return Err("producer capture cancelled".into());
        }
        let pinned = tempfile::tempdir_in(&binding.input).map_err(|e| e.to_string())?;
        // The acquisition cursor precedes the append-only bytes it covers.
        // Pin it before enumerating segments, so concurrent rotation can only
        // add a superset beyond this saved cursor. Publisher retirement is paused.
        let resume = binding.input.join("resume-cursor");
        match std::fs::hard_link(&resume, pinned.path().join("resume-cursor")) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.to_string()),
        }
        if binding.source == "session.media" {
            for (stem, path) in sinex_source_core::drivers::media::complete(&binding.input)
                .map_err(|e| e.to_string())?
            {
                if cancelled.load(Ordering::Acquire) {
                    return Err("producer capture cancelled while pinning media".into());
                }
                std::fs::hard_link(
                    &path,
                    pinned
                        .path()
                        .join(path.file_name().ok_or("media file has no name")?),
                )
                .map_err(|e| e.to_string())?;
                std::fs::hard_link(
                    binding.input.join(format!("{stem}.json")),
                    pinned.path().join(format!("{stem}.json")),
                )
                .map_err(|e| e.to_string())?;
            }
        } else {
            for entry in std::fs::read_dir(&binding.input).map_err(|e| e.to_string())? {
                if cancelled.load(Ordering::Acquire) {
                    return Err("producer capture cancelled while pinning source".into());
                }
                let entry = entry.map_err(|e| e.to_string())?;
                let name = entry.file_name();
                if name == "resume-cursor"
                    || name.to_string_lossy().ends_with(".tmp")
                    || entry.path() == pinned.path()
                {
                    continue;
                }
                if !entry.file_type().map_err(|e| e.to_string())?.is_file() {
                    return Err("unsupported native spool entry".into());
                }
                std::fs::hard_link(entry.path(), pinned.path().join(name))
                    .map_err(|e| e.to_string())?;
            }
        }
        add(pinned.path(), &format!("spool/{index}"))?;
    }
    let inventory = serde_json::to_vec(bindings).map_err(|e| e.to_string())?;
    use std::io::Write;
    let mut file =
        std::fs::File::create(directory.path().join("bindings.json")).map_err(|e| e.to_string())?;
    file.write_all(&inventory)
        .and_then(|()| file.sync_all())
        .map_err(|e| e.to_string())?;
    files.push(sinex_model::backup::File {
        path: "bindings.json".into(),
        bytes: inventory.len() as u64,
        blake3: blake3::hash(&inventory).to_hex().to_string(),
    });
    std::fs::File::open(directory.path())
        .and_then(|f| f.sync_all())
        .map_err(|e| e.to_string())?;
    Ok(Captured { directory, files })
}

#[cfg(test)]
mod tests;
