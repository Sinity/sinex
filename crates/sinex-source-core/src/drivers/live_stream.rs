//! `LiveStream`: something that only exists while it is happening (an IPC
//! socket, D-Bus, netlink, a `journalctl --follow` child). Nothing upstream
//! keeps it, so capture writes each line, with the time it was received, to a
//! spool on disk before anything else happens. The spool is a directory of
//! segment files; capture closes a segment and starts the next one at 100 MiB
//! or after an hour.
//!
//! Acquisition reads the spool like any growing file: each poll stores the
//! complete records appended since the cursor as a new part, linked to the
//! previous one across segments. A record is one JSON line,
//! `{"received_ns": "<ns>", "line": "<text>"}`, or `"line_hex"` when the
//! captured bytes are not UTF-8, so the stored material keeps exactly what
//! arrived.
use super::append_file::{AppendFile, AppendState, lines};
use super::{Acquired, DriverError};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sinex_model::{InputRef, MaterialId, RevisionChange, Timestamp};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// When capture closes a segment and starts the next.
#[derive(Clone, Copy, Debug)]
pub struct Rotation {
    pub max_bytes: u64,
    pub max_age: Duration,
}

impl Default for Rotation {
    fn default() -> Self {
        Self {
            max_bytes: 100 << 20,
            max_age: Duration::from_secs(3600),
        }
    }
}

/// The capture side: appends received lines to the current segment.
pub struct SpoolWriter {
    private: crate::private_mode::Gate,
    state: std::sync::Arc<std::sync::Mutex<SpoolState>>,
    pub dropped: u64,
    captured: u64,
    run_permit: Option<crate::private_mode::ReadPermit>,
}

struct SpoolState {
    dir: PathBuf,
    rotation: Rotation,
    index: u64,
    file: File,
    written: u64,
    opened: Instant,
    unsynced: usize,
}

const SEGMENT_SUFFIX: &str = ".ndjson";

fn segment_path(dir: &Path, index: u64) -> PathBuf {
    dir.join(format!("{index:012}{SEGMENT_SUFFIX}"))
}

/// Segment indexes present in `dir`, ascending.
pub fn segments(dir: &Path) -> std::io::Result<Vec<u64>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let name = entry?.file_name();
        let name = name.to_string_lossy();
        if let Some(stem) = name.strip_suffix(SEGMENT_SUFFIX)
            && let Ok(index) = stem.parse::<u64>()
        {
            out.push(index);
        }
    }
    out.sort_unstable();
    Ok(out)
}

/// One captured record as stored in the spool.
pub fn frame(received: Timestamp, line: &[u8]) -> Vec<u8> {
    let mut object = Map::new();
    object.insert(
        "received_ns".into(),
        Value::String(received.unix_ns().to_string()),
    );
    match std::str::from_utf8(line) {
        Ok(text) => object.insert("line".into(), Value::String(text.to_owned())),
        Err(_) => object.insert(
            "line_hex".into(),
            Value::String(line.iter().map(|b| format!("{b:02x}")).collect()),
        ),
    };
    let mut bytes = serde_json::to_vec(&Value::Object(object)).expect("JSON object serializes");
    bytes.push(b'\n');
    bytes
}

impl SpoolWriter {
    /// Open the spool, continuing after its last segment. A capture restart
    /// never appends to a segment an earlier capture may have left with a
    /// partial line.
    pub fn open(dir: impl Into<PathBuf>, rotation: Rotation) -> std::io::Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        let index = segments(&dir)?.last().map_or(0, |last| last + 1);
        let file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(segment_path(&dir, index))?;
        let state = std::sync::Arc::new(std::sync::Mutex::new(SpoolState {
            dir: dir.clone(),
            rotation,
            index,
            file,
            written: 0,
            opened: Instant::now(),
            unsynced: 0,
        }));
        let private = Self::gate(
            &dir,
            state.clone(),
            crate::private_mode::PrivateMode::default(),
        )?;
        Ok(Self {
            private,
            state,
            dropped: 0,
            captured: 0,
            run_permit: None,
        })
    }

    fn gate(
        dir: &Path,
        state: std::sync::Arc<std::sync::Mutex<SpoolState>>,
        mode: crate::private_mode::PrivateMode,
    ) -> std::io::Result<crate::private_mode::Gate> {
        crate::private_mode::Gate::open(dir, dir.display().to_string(), mode, move |gap| {
            let mut state = state.lock().unwrap_or_else(|e| e.into_inner());
            let mut bytes = serde_json::to_vec(&json!({"capture_gap": gap}))?;
            bytes.push(b'\n');
            state.write(&bytes)?;
            state.sync()
        })
    }

    /// Keep the writer and its gap watcher alive while acquisition is paused.
    pub fn capture(
        &mut self,
        parent: &crate::capture::CaptureStop,
        mut capture: impl FnMut(&mut Self, &crate::capture::CaptureStop) -> std::io::Result<u64> + Send,
    ) -> std::io::Result<u64> {
        let before = self.captured;
        let gate = self.private.access();
        while !parent.stopped() {
            if !gate.allowed()? {
                parent.wait(Duration::from_millis(20));
                continue;
            }
            let permit = gate.permit()?;
            self.run_permit = Some(permit);
            let result = gate.record_permit(parent, permit, |active| capture(self, active));
            self.run_permit = None;
            if result?.is_some() {
                break;
            }
        }
        Ok(self.captured - before)
    }

    pub fn capture_shared(
        writer: &std::sync::Mutex<Self>,
        parent: &crate::capture::CaptureStop,
        mut capture: impl FnMut(&crate::capture::CaptureStop) -> std::io::Result<u64> + Send,
    ) -> std::io::Result<u64> {
        let (gate, before) = {
            let writer = writer
                .lock()
                .map_err(|_| std::io::Error::other("spool lock poisoned"))?;
            (writer.private.access(), writer.captured)
        };
        while !parent.stopped() {
            if !gate.allowed()? {
                parent.wait(Duration::from_millis(20));
                continue;
            }
            let permit = gate.permit()?;
            writer
                .lock()
                .map_err(|_| std::io::Error::other("spool lock poisoned"))?
                .run_permit = Some(permit);
            let result = gate.record_permit(parent, permit, &mut capture);
            writer
                .lock()
                .map_err(|_| std::io::Error::other("spool lock poisoned"))?
                .run_permit = None;
            if result?.is_some() {
                break;
            }
        }
        Ok(writer
            .lock()
            .map_err(|_| std::io::Error::other("spool lock poisoned"))?
            .captured
            - before)
    }

    pub fn record<T: Send>(
        &self,
        stop: &crate::capture::CaptureStop,
        capture: impl FnOnce(&crate::capture::CaptureStop) -> std::io::Result<T> + Send,
    ) -> std::io::Result<Option<T>> {
        self.private.record(stop, capture)
    }

    pub fn gate_access(&self) -> crate::private_mode::GateAccess {
        self.private.access()
    }

    pub fn read_permit(&self) -> std::io::Result<crate::private_mode::ReadPermit> {
        self.private.permit()
    }

    pub fn append_read(
        &mut self,
        permit: crate::private_mode::ReadPermit,
        received: Timestamp,
        line: &[u8],
    ) -> std::io::Result<()> {
        if let Some(run) = self.run_permit
            && !self.private.accepts(run)?
        {
            self.dropped += 1;
            return Ok(());
        }
        let stored = self.private.store(permit, || {
            self.state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .write(&frame(received, line))
        })?;
        if stored.is_none() {
            self.dropped += 1;
        } else {
            self.captured += 1;
        }
        Ok(())
    }

    /// Append one received line (without its terminator).
    pub fn append(&mut self, received: Timestamp, line: &[u8]) -> std::io::Result<()> {
        let permit = self.private.permit()?;
        self.append_read(permit, received, line)
    }

    /// Make everything appended so far durable.
    pub fn sync(&mut self) -> std::io::Result<()> {
        self.private.allowed()?;
        self.state.lock().unwrap_or_else(|e| e.into_inner()).sync()
    }

    /// Use a configured switch. Tests use independent switch paths.
    pub fn with_private_mode(self, mode: crate::private_mode::PrivateMode) -> Self {
        self.private
            .set_switch(mode)
            .expect("capture switch can be configured");
        self
    }

    pub fn segment(&self) -> u64 {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).index
    }
}

impl SpoolState {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        if self.written >= self.rotation.max_bytes || self.opened.elapsed() >= self.rotation.max_age
        {
            self.rotate()?;
        }
        self.file.write_all(bytes)?;
        self.written += bytes.len() as u64;
        self.unsynced += 1;
        Ok(())
    }

    fn sync(&mut self) -> std::io::Result<()> {
        if self.unsynced > 0 {
            self.file.sync_data()?;
            self.unsynced = 0;
        }
        Ok(())
    }

    fn rotate(&mut self) -> std::io::Result<()> {
        self.sync()?;
        self.index += 1;
        self.file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(segment_path(&self.dir, self.index))?;
        self.written = 0;
        self.opened = Instant::now();
        Ok(())
    }
}

/// The same framing is used by a capture thread and shared socket writers.
pub trait LineWriter {
    fn permission(&self) -> std::io::Result<crate::private_mode::ReadPermit>;
    fn line(
        &mut self,
        first: crate::private_mode::ReadPermit,
        last: crate::private_mode::ReadPermit,
        received: Timestamp,
        bytes: &[u8],
    ) -> std::io::Result<bool>;
    fn flush(&mut self) -> std::io::Result<()>;
}

impl LineWriter for SpoolWriter {
    fn permission(&self) -> std::io::Result<crate::private_mode::ReadPermit> {
        self.read_permit()
    }
    fn line(
        &mut self,
        first: crate::private_mode::ReadPermit,
        last: crate::private_mode::ReadPermit,
        received: Timestamp,
        bytes: &[u8],
    ) -> std::io::Result<bool> {
        let dropped = self.dropped;
        if self.private.accepts(last)? {
            self.append_read(first, received, bytes)?;
        } else {
            self.dropped += 1;
        }
        Ok(self.dropped == dropped)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.sync()
    }
}

impl LineWriter for &std::sync::Mutex<SpoolWriter> {
    fn permission(&self) -> std::io::Result<crate::private_mode::ReadPermit> {
        self.lock()
            .map_err(|_| std::io::Error::other("spool lock poisoned"))?
            .permission()
    }
    fn line(
        &mut self,
        first: crate::private_mode::ReadPermit,
        last: crate::private_mode::ReadPermit,
        received: Timestamp,
        bytes: &[u8],
    ) -> std::io::Result<bool> {
        self.lock()
            .map_err(|_| std::io::Error::other("spool lock poisoned"))?
            .line(first, last, received, bytes)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.lock()
            .map_err(|_| std::io::Error::other("spool lock poisoned"))?
            .sync()
    }
}

/// Copy `reader`'s lines into the spool until it ends, syncing at least every
/// `sync_every`. Returns the number of lines captured. A final line without a
/// terminator is captured too: the stream ended, so it is complete.
pub fn capture_lines(
    mut reader: impl BufRead,
    spool: &mut impl LineWriter,
    sync_every: Duration,
) -> std::io::Result<u64> {
    let mut line = Vec::new();
    let mut line_permit = spool.permission()?;
    let mut count = 0;
    let mut last_sync = Instant::now();
    loop {
        let permit = spool.permission()?;
        let bytes = match reader.fill_buf() {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        if bytes.is_empty() {
            break;
        }
        let length = bytes.len();
        for piece in bytes.split_inclusive(|b| *b == b'\n') {
            if line.is_empty() {
                line_permit = permit;
            }
            line.extend_from_slice(piece);
            if line.ends_with(b"\n") {
                line.pop();
                count += u64::from(spool.line(line_permit, permit, Timestamp::now(), &line)?);
                line.clear();
            }
        }
        reader.consume(length);
        if last_sync.elapsed() >= sync_every {
            spool.flush()?;
            last_sync = Instant::now();
        }
    }
    if !line.is_empty() {
        count += u64::from(spool.line(line_permit, line_permit, Timestamp::now(), &line)?);
    }
    spool.flush()?;
    Ok(count)
}

/// Decode a spooled record: when it arrived and the bytes that arrived.
pub fn received(record: &Map<String, Value>) -> Result<(Timestamp, Vec<u8>), String> {
    let ns: i128 = record
        .get("received_ns")
        .and_then(Value::as_str)
        .and_then(|s| s.parse().ok())
        .ok_or("spooled record has no received_ns")?;
    let at = Timestamp::from_unix_ns(ns).map_err(|e| format!("received_ns out of range: {e}"))?;
    let bytes = match (record.get("line"), record.get("line_hex")) {
        (Some(Value::String(text)), _) => text.as_bytes().to_vec(),
        (_, Some(Value::String(hex))) => (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(hex.get(i..i + 2).unwrap_or("zz"), 16))
            .collect::<Result<_, _>>()
            .map_err(|_| "line_hex is not hex")?,
        _ => return Err("spooled record has no line".into()),
    };
    Ok((at, bytes))
}

/// The acquisition side.
pub struct LiveStream {
    pub spool: PathBuf,
    /// What is being captured, such as `{kind: "socket", locator:
    /// "hyprland:socket2"}`.
    pub input: InputRef,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    pub segment: u64,
    /// The `AppendFile` cursor within the segment.
    pub inner: Value,
    /// Parts stored from earlier segments, so part numbers keep counting.
    pub part_base: u32,
    /// The last part stored, so the first part of a new segment links to it.
    pub last_material: Option<MaterialId>,
}

impl LiveStream {
    pub fn poll(&self, cursor: &Value) -> Result<Acquired, DriverError> {
        let mut state: Cursor = if cursor.is_null() {
            Cursor::default()
        } else {
            serde_json::from_value(cursor.clone())
                .map_err(|e| DriverError::Cursor(e.to_string()))?
        };
        let present = match segments(&self.spool) {
            Ok(present) => present,
            // Capture has not created its spool yet.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => {
                return Err(DriverError::Io(format!(
                    "spool {}: {e}",
                    self.spool.display()
                )));
            }
        };
        let Some(first) = present.first() else {
            // Capture has not written anything yet: an unchanged look.
            return Ok(Acquired {
                input: self.input.clone(),
                observation: sinex_model::ObservationFacts {
                    observed_at: Some(Timestamp::now()),
                    changed: false,
                    ..Default::default()
                },
                revision: RevisionChange::Same,
                material: None,
                cursor_without_material: cursor.clone(),
            });
        };
        if cursor.is_null() {
            state.segment = *first;
        }
        loop {
            let file = AppendFile {
                path: segment_path(&self.spool, state.segment),
                host: self.input.host.clone(),
                framer: crate::Framer::incremental(lines),
            };
            // A spool only grows, so there is no rewrite to align against.
            let mut acquired = file.poll(&state.inner, &mut AppendState::default())?;
            acquired.input = self.input.clone();
            if let Some(material) = &mut acquired.material {
                material
                    .manifest
                    .details
                    .insert("capture_records".into(), json!(true));
                let inner_part = material.part_index;
                material.part_index = state.part_base + inner_part;
                if inner_part == 0 {
                    material.previous_part = state.last_material;
                }
                material
                    .manifest
                    .details
                    .insert("spool_segment".into(), json!(state.segment));
                // Offsets restart in each segment; the segment number keeps
                // later records newer. Recorded so a replay computes the same.
                let offset = material.manifest.file_offset.unwrap_or(0);
                let base = (state.segment << 40) | offset;
                material.manifest.order_base = Some(base as i64);
                material.reading_order = super::reading_order(&material.manifest);
                let inner_cursor =
                    std::mem::replace(&mut material.cursor_at, Box::new(|_| Ok(Value::Null)));
                let wrapped = state.clone();
                let id = material.id;
                material.cursor_at = Box::new(move |end| {
                    let inner = inner_cursor(end)?;
                    Ok(serde_json::to_value(Cursor {
                        inner,
                        last_material: Some(id),
                        ..wrapped.clone()
                    })
                    .expect("cursor serializes"))
                });
                // A live stream only grows; a segment rewritten under capture
                // would be a bug in capture, not a source revision.
                acquired.revision = RevisionChange::Appended;
                acquired.cursor_without_material = Value::Null;
                return Ok(acquired);
            }
            // Nothing new here. If capture has moved on to a later segment,
            // this one is closed: continue with the next.
            let Some(next) = present.iter().copied().find(|s| *s > state.segment) else {
                let inner = acquired.cursor_without_material.clone();
                acquired.input = self.input.clone();
                acquired.cursor_without_material =
                    serde_json::to_value(Cursor { inner, ..state }).expect("cursor serializes");
                return Ok(acquired);
            };
            let parts_here = state
                .inner
                .get("part_index")
                .and_then(Value::as_u64)
                .map_or(0, |p| p as u32 + 1);
            let had_material = state.inner.get("material").is_some_and(|m| !m.is_null());
            state = Cursor {
                segment: next,
                inner: Value::Null,
                part_base: state.part_base + if had_material { parts_here } else { 0 },
                last_material: state.last_material,
            };
        }
    }

    /// Remove closed segments that every committed cursor has passed. Called
    /// after the batch carrying `committed` is stored; the bytes are in the
    /// content store by then, so the spool copy is no longer needed.
    pub fn remove_passed(&self, committed: &Value) -> std::io::Result<usize> {
        let Ok(state) = serde_json::from_value::<Cursor>(committed.clone()) else {
            return Ok(0);
        };
        let mut removed = 0;
        for index in segments(&self.spool)? {
            if index < state.segment {
                std::fs::remove_file(segment_path(&self.spool, index))?;
                removed += 1;
            }
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests;
