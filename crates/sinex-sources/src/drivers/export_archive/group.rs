//! One forward archive traversal shared by compatible logical bindings.
use super::{Container, DriverError, ExportArchive, Framing, file_name, io, is_tar, records};
use crate::drivers::{Acquired, MaterialSink, NewMaterial, StoredBytes};
use crate::shared_pass::{Resume, StoredCopy};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sinex_model::{Class, MaterialId, ObservationFacts, RevisionChange};
use std::io::{BufRead, BufReader, Read, Seek};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

const VERSION: u8 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capture {
    pub class: Class,
    pub blob: String,
    pub length: u64,
    pub container: Container,
    pub reading_order_base: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Cursor {
    pub capture: Capture,
    pub size: u64,
    pub mtime_ns: i128,
    pub ctime_ns: Option<i128>,
    pub inode: u64,
    pub device: u64,
    /// Archive file member, then the next record within that member.
    pub member: usize,
    pub record: usize,
    /// Compact record bytes in preceding selected members, for stable reading order.
    pub order_offset: u64,
    pub material: Option<MaterialId>,
    pub part_index: u32,
    pub original_material: Option<MaterialId>,
    pub done: bool,
    /// The record of `member` the current part starts at.
    pub part_start: usize,
    /// How many bytes of the current part were admitted.
    pub through: usize,
}

impl crate::shared_pass::SharedPass for ExportArchive {
    fn path(&self) -> &std::path::Path {
        &self.path
    }

    fn resume(
        &self,
        cursor: &Value,
        facts: &ObservationFacts,
        class: Class,
    ) -> Result<Resume, DriverError> {
        let Some(cursor) =
            Cursor::parse(cursor)?.filter(|c| c.same_file(facts) && c.capture.class == class)
        else {
            return Ok(Resume::Read(None));
        };
        if cursor.done {
            return Ok(Resume::Finished);
        }
        Ok(Resume::Read(Some(StoredCopy {
            blob: cursor.capture.blob,
            length: cursor.capture.length,
            material: cursor.original_material,
        })))
    }

    fn route(&self, cursor: Value, class: Class) -> Route {
        Route {
            class,
            driver: self.clone(),
            cursor,
        }
    }
}

impl Cursor {
    fn parse(value: &Value) -> Result<Option<Self>, DriverError> {
        if value.is_null() {
            return Ok(None);
        }
        serde_json::from_value(value.clone())
            .map(Some)
            .map_err(|e| DriverError::Cursor(e.to_string()))
    }

    fn same_file(&self, facts: &ObservationFacts) -> bool {
        Some(self.size) == facts.size
            && Some(self.mtime_ns) == facts.mtime_ns
            && self.ctime_ns == facts.ctime_ns
            && Some(self.inode) == facts.inode
            && Some(self.device) == facts.device
    }
}

/// One binding's part in a pass.
pub struct Route {
    class: Class,
    driver: ExportArchive,
    cursor: Value,
}

#[derive(Debug, Default)]
pub struct Work {
    /// Bytes read from the decoded tar/gzip stream or selected ZIP members.
    pub decoded_bytes: u64,
    pub zip_staging_bytes: u64,
    pub member_staging_bytes: u64,
    pub record_bytes_stored: u64,
}

type Emit<'a> = dyn FnMut(usize, Result<Acquired, DriverError>) -> Result<bool, DriverError> + 'a;

/// Archive input keeps ZIP's real random-access requirement explicit.
pub trait ReadSeek: Read + Seek + Send {}
impl<T: Read + Seek + Send> ReadSeek for T {}
pub enum Input {
    Sequential(Box<dyn Read + Send>),
    Seekable(Box<dyn ReadSeek>),
}

/// Read already-stored bytes. The caller groups only compatible privacy/acquisition
/// contexts and installs the original blob once. Each callback is completed before reading
/// further, so admission supplies backpressure. A false result stops only that route.
#[allow(clippy::too_many_arguments)]
pub fn read_input(
    input: Input,
    blob: String,
    length: u64,
    facts: ObservationFacts,
    stop: Arc<AtomicBool>,
    routes: Vec<Route>,
    sink: &dyn MaterialSink,
    emit: &mut Emit<'_>,
) -> Result<Work, DriverError> {
    if routes.is_empty() {
        return Ok(Work::default());
    }
    if routes.iter().any(|r| r.class == Class::Sealed) && routes.len() != 1 {
        return Err(DriverError::InvalidMaterial(
            "sealed bindings require independent material keys and traversals".into(),
        ));
    }
    let input = match input {
        Input::Sequential(reader) => Input::Sequential(Box::new(Cancellable {
            reader,
            stop: Arc::clone(&stop),
        })),
        Input::Seekable(reader) => Input::Seekable(Box::new(Cancellable {
            reader,
            stop: Arc::clone(&stop),
        })),
    };
    let decoded = Arc::new(AtomicU64::new(0));
    let opened = Opened::new(input, Arc::clone(&decoded))?;
    if routes[0].class == Class::Sealed
        && opened.container == Container::Zip
        && opened.seekable.is_none()
    {
        return Err(DriverError::InvalidMaterial(
            "sealed ZIP needs a stored random-access reader; plaintext is never staged".into(),
        ));
    }
    let capture = Capture {
        class: routes[0].class,
        blob,
        length,
        container: opened.container,
        reading_order_base: i64::try_from(facts.mtime_ns.unwrap_or(0)).unwrap_or(i64::MAX),
    };
    let mut context = Context {
        capture,
        facts,
        routes: Vec::new(),
        sink,
        emit,
        work: Work::default(),
        stop: Arc::clone(&stop),
    };
    for route in routes {
        let previous = Cursor::parse(&route.cursor)?;
        let continuing = previous.as_ref().is_some_and(|c| {
            c.capture.class == context.capture.class
                && c.capture.blob == context.capture.blob
                && c.capture.length == context.capture.length
                && c.capture.container == context.capture.container
        });
        let mut cursor = if continuing {
            previous.expect("continuing")
        } else {
            Cursor {
                capture: context.capture.clone(),
                size: context.facts.size.unwrap_or(0),
                mtime_ns: context.facts.mtime_ns.unwrap_or(0),
                ctime_ns: context.facts.ctime_ns,
                inode: context.facts.inode.unwrap_or(0),
                device: context.facts.device.unwrap_or(0),
                member: 0,
                record: 0,
                order_offset: 0,
                material: None,
                part_index: 0,
                original_material: None,
                done: false,
                part_start: 0,
                through: 0,
            }
        };
        cursor.size = context.facts.size.unwrap_or(0);
        cursor.mtime_ns = context.facts.mtime_ns.unwrap_or(0);
        cursor.ctime_ns = context.facts.ctime_ns;
        cursor.inode = context.facts.inode.unwrap_or(0);
        cursor.device = context.facts.device.unwrap_or(0);
        let revision = if route.cursor.is_null() {
            RevisionChange::Snapshot
        } else {
            RevisionChange::Replaced
        };
        context.routes.push(State {
            driver: route.driver,
            cursor,
            active: true,
        });
        let index = context.routes.len() - 1;
        // A continuing route reads on from its cursor: records of a part only
        // partly admitted are read again from the stored archive into a new part.
        if !continuing {
            context.original(index, revision)?;
        }
    }
    let path = context.routes[0].driver.path.clone();
    let mut zip_staging = 0;
    opened
        .visit(
            &path,
            Arc::clone(&decoded),
            &mut |member, name, input| {
                if stop.load(Ordering::Relaxed) {
                    return Err(DriverError::Cancelled);
                }
                context.member(member, name, input)
            },
            &mut zip_staging,
        )
        .map_err(|error| {
            if stop.load(Ordering::Relaxed) {
                DriverError::Cancelled
            } else {
                error
            }
        })?;
    context.work.zip_staging_bytes = zip_staging;
    context.work.decoded_bytes = decoded.load(Ordering::Relaxed);
    for index in 0..context.routes.len() {
        if !context.routes[index].active {
            continue;
        }
        let state = &mut context.routes[index];
        state.cursor.done = true;
        let acquired = Acquired {
            input: state.driver.input(),
            observation: ObservationFacts {
                changed: false,
                ..context.facts.clone()
            },
            revision: RevisionChange::Same,
            material: None,
            cursor_without_material: json!(state.cursor),
        };
        (context.emit)(index, Ok(acquired))?;
    }
    Ok(context.work)
}

struct State {
    driver: ExportArchive,
    cursor: Cursor,
    active: bool,
}
struct Context<'a, 'b> {
    capture: Capture,
    facts: ObservationFacts,
    routes: Vec<State>,
    sink: &'a dyn MaterialSink,
    emit: &'a mut Emit<'b>,
    work: Work,
    stop: Arc<AtomicBool>,
}

impl Context<'_, '_> {
    fn original(&mut self, index: usize, revision: RevisionChange) -> Result<(), DriverError> {
        let state = &mut self.routes[index];
        let id = MaterialId::new();
        state.cursor.material = Some(id);
        state.cursor.original_material = Some(id);
        let after = json!(state.cursor);
        let manifest = json!({"role": "original-archive", "format_version": VERSION,
            "path": state.driver.path, "host": state.driver.host, "container": self.capture.container, "class": self.capture.class});
        let acquired = Acquired {
            input: state.driver.input(),
            observation: self.facts.clone(),
            revision,
            material: Some(NewMaterial {
                id,
                bytes: Vec::new(),
                stored: Some(StoredBytes::Blob {
                    name: self.capture.blob.clone(),
                    length: self.capture.length,
                }),
                records: Vec::new(),
                part_index: 0,
                previous_part: None,
                reading_order: crate::drivers::reading_order(&manifest),
                manifest,
                cursor_at: Box::new({
                    let cursor = move |_| after.clone();
                    move |end| Ok(cursor(end))
                }),
                aligned: false,
                removed: Vec::new(),
                memory: None,
            }),
            cursor_without_material: Value::Null,
        };
        state.active = (self.emit)(index, Ok(acquired))?;
        Ok(())
    }

    fn member(
        &mut self,
        member: usize,
        name: String,
        reader: &mut dyn Read,
    ) -> Result<(), DriverError> {
        let single = matches!(
            self.capture.container,
            Container::Gzip | Container::Stateless
        );
        let indices: Vec<usize> = self
            .routes
            .iter()
            .enumerate()
            .filter(|(_, s)| {
                s.active
                    && !s.cursor.done
                    && member >= s.cursor.member
                    && (single || (s.driver.select)(&name))
            })
            .map(|(i, _)| i)
            .collect();
        if indices.is_empty() {
            return Ok(());
        }
        let mut framings = Vec::new();
        for &index in &indices {
            let framing = self.routes[index].driver.framing;
            if !framings.contains(&framing) {
                framings.push(framing);
            }
        }
        if framings.len() == 1 {
            return self.framed(member, &name, reader, &indices, framings[0]);
        }
        // Only overlapping selectors with different framers need independent readers.
        // Keep one private, unlinked member file; its decoded bytes are never re-extracted.
        let mut staged = tempfile::tempfile().map_err(io)?;
        self.work.member_staging_bytes += std::io::copy(reader, &mut staged).map_err(io)?;
        for framing in framings {
            staged.rewind().map_err(io)?;
            let selected: Vec<usize> = indices
                .iter()
                .copied()
                .filter(|&i| self.routes[i].driver.framing == framing)
                .collect();
            self.framed(member, &name, &mut staged, &selected, framing)?;
        }
        Ok(())
    }

    fn framed(
        &mut self,
        member: usize,
        name: &str,
        reader: &mut dyn Read,
        indices: &[usize],
        framing: Framing,
    ) -> Result<(), DriverError> {
        let budget = indices
            .iter()
            .map(|&i| self.routes[i].driver.part_bytes.max(1))
            .min()
            .expect("selected route");
        let mut bytes = Vec::new();
        let mut ranges = Vec::new();
        let mut ordinal = 0;
        let mut start = 0;
        let mut byte_position = 0u64;
        let mut part_offset = 0u64;
        let result = records::visit(reader, framing, budget, &mut |record| {
            if self.stop.load(Ordering::Relaxed) {
                return Err(DriverError::Cancelled);
            }
            byte_position = byte_position
                .checked_add(record.len() as u64)
                .ok_or_else(|| DriverError::Cursor("archive reading order exceeds u64".into()))?;
            let begin = bytes.len();
            bytes.extend_from_slice(&record);
            ranges.push((begin, bytes.len()));
            ordinal += 1;
            if bytes.len() >= budget || ranges.len() >= crate::Limits::default().max_events {
                self.part(member, name, indices, start, part_offset, &bytes, &ranges)?;
                bytes.clear();
                ranges.clear();
                start = ordinal;
                part_offset = byte_position;
            }
            Ok(())
        });
        if let Err(error) = result {
            if matches!(error, DriverError::Cancelled) {
                return Err(error);
            }
            let reason = error.to_string();
            for &index in indices {
                if self.routes[index].active {
                    (self.emit)(index, Err(DriverError::Io(format!("{name}: {reason}"))))?;
                    self.routes[index].active = false;
                }
            }
            return Ok(());
        }
        if !ranges.is_empty() {
            self.part(member, name, indices, start, part_offset, &bytes, &ranges)?;
        }
        for &index in indices {
            if self.routes[index].active {
                self.routes[index].cursor.order_offset = self.routes[index]
                    .cursor
                    .order_offset
                    .checked_add(byte_position)
                    .ok_or_else(|| {
                        DriverError::Cursor("archive reading order exceeds u64".into())
                    })?;
                self.routes[index].cursor.member = member + 1;
                self.routes[index].cursor.record = 0;
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn part(
        &mut self,
        member: usize,
        name: &str,
        indices: &[usize],
        start: usize,
        part_offset: u64,
        bytes: &[u8],
        ranges: &[(usize, usize)],
    ) -> Result<(), DriverError> {
        let eligible: Vec<usize> = indices
            .iter()
            .copied()
            .filter(|&i| {
                let state = &self.routes[i];
                state.active
                    && (state.cursor.member != member || state.cursor.record < start + ranges.len())
            })
            .collect();
        if eligible.is_empty() {
            return Ok(());
        }
        let blob = self.sink.put(bytes).map_err(DriverError::Store)?;
        self.work.record_bytes_stored += bytes.len() as u64;
        for index in eligible {
            let state = &mut self.routes[index];
            let skip = if state.cursor.member == member {
                state.cursor.record.saturating_sub(start).min(ranges.len())
            } else {
                0
            };
            let records = ranges[skip..].to_vec();
            let id = MaterialId::new();
            let previous_part = state.cursor.material;
            let part_index = state.cursor.part_index + 1;
            let mut after = state.cursor.clone();
            after.material = Some(id);
            after.part_index = part_index;
            after.member = member;
            after.record = start + ranges.len();
            after.part_start = start + skip;
            after.through = bytes.len();
            let ends: Vec<usize> = records.iter().map(|&(_, end)| end).collect();
            let cursor_base = after.clone();
            let path = if self.capture.container == Container::Gzip {
                let name = file_name(&state.driver.path);
                name.strip_suffix(".gz").unwrap_or(&name).to_owned()
            } else {
                name.to_owned()
            };
            let manifest = json!({"role": "records", "format_version": VERSION, "container": "plain",
                "source_container": self.capture.container, "path": state.driver.path, "host": state.driver.host,
                "framing": state.driver.framing.name(), "records": records, "order_base": after.capture.reading_order_base.saturating_add(i64::try_from(after.order_offset.saturating_add(part_offset)).unwrap_or(i64::MAX)),
                "members": [{"path": path, "stream_offset": 0, "length": bytes.len(), "record_start": after.part_start}]});
            let acquired = Acquired {
                input: state.driver.input(),
                observation: ObservationFacts {
                    changed: false,
                    ..self.facts.clone()
                },
                revision: RevisionChange::Same,
                material: Some(NewMaterial {
                    id,
                    bytes: bytes.to_vec(),
                    stored: Some(StoredBytes::Blob {
                        name: blob.clone(),
                        length: bytes.len() as u64,
                    }),
                    records,
                    part_index,
                    previous_part,
                    reading_order: crate::drivers::reading_order(&manifest),
                    manifest,
                    cursor_at: Box::new({
                        let cursor = move |end| {
                            let mut cursor = cursor_base.clone();
                            cursor.record =
                                cursor.part_start + ends.partition_point(|&at| at <= end);
                            cursor.through = end;
                            json!(cursor)
                        };
                        move |end| Ok(cursor(end))
                    }),
                    aligned: false,
                    removed: Vec::new(),
                    memory: None,
                }),
                cursor_without_material: Value::Null,
            };
            if (self.emit)(index, Ok(acquired))? {
                state.cursor = after;
            } else {
                state.active = false;
            }
        }
        if !self
            .routes
            .iter()
            .any(|state| state.active && !state.cursor.done)
        {
            return Err(DriverError::Cancelled);
        }
        Ok(())
    }
}

struct Opened {
    container: Container,
    reader: Box<dyn Read + Send>,
    seekable: Option<Box<dyn ReadSeek>>,
}
impl Opened {
    fn new(input: Input, count: Arc<AtomicU64>) -> Result<Self, DriverError> {
        let reader = match input {
            Input::Sequential(reader) => reader,
            Input::Seekable(reader) => {
                let mut reader = BufReader::with_capacity(65536, reader);
                let head = reader.fill_buf().map_err(io)?;
                if head.starts_with(b"PK\x03\x04") || head.starts_with(b"PK\x05\x06") {
                    return Ok(Self {
                        container: Container::Zip,
                        reader: Box::new(std::io::empty()),
                        seekable: Some(Box::new(reader)),
                    });
                }
                Box::new(reader) as Box<dyn Read + Send>
            }
        };
        let mut reader = BufReader::with_capacity(65536, reader);
        let head = reader.fill_buf().map_err(io)?;
        let container = if head.starts_with(b"PK\x03\x04") || head.starts_with(b"PK\x05\x06") {
            Container::Zip
        } else if is_tar(head) {
            Container::Tar
        } else if head.starts_with(&[0x1f, 0x8b]) {
            Container::Gzip
        } else {
            Container::Stateless
        };
        if container == Container::Gzip {
            let mut decoded = BufReader::with_capacity(
                65536,
                Counted {
                    reader: flate2::read::MultiGzDecoder::new(reader),
                    count,
                },
            );
            let container = if is_tar(decoded.fill_buf().map_err(io)?) {
                Container::TarGzip
            } else {
                Container::Gzip
            };
            Ok(Self {
                container,
                seekable: None,
                reader: Box::new(decoded),
            })
        } else if container == Container::Zip {
            Ok(Self {
                container,
                seekable: None,
                reader: Box::new(reader),
            })
        } else {
            Ok(Self {
                container,
                seekable: None,
                reader: Box::new(Counted { reader, count }),
            })
        }
    }

    fn visit(
        self,
        path: &std::path::Path,
        count: Arc<AtomicU64>,
        visit: &mut dyn FnMut(usize, String, &mut dyn Read) -> Result<(), DriverError>,
        zip_staged: &mut u64,
    ) -> Result<(), DriverError> {
        let mut input = self.reader;
        match self.container {
            Container::Tar | Container::TarGzip => {
                let mut archive = tar::Archive::new(input);
                let mut member = 0;
                for entry in archive.entries().map_err(io)? {
                    let mut entry = entry.map_err(io)?;
                    if !entry.header().entry_type().is_file() {
                        continue;
                    }
                    let name = entry.path().map_err(io)?.to_string_lossy().into_owned();
                    visit(member, name, &mut entry)?;
                    member += 1;
                }
            }
            Container::Zip => {
                let reader: Box<dyn ReadSeek> = match self.seekable {
                    Some(reader) => reader,
                    None => {
                        let mut file = tempfile::tempfile().map_err(io)?;
                        *zip_staged += std::io::copy(&mut input, &mut file).map_err(io)?;
                        file.rewind().map_err(io)?;
                        Box::new(file)
                    }
                };
                let mut archive = zip::ZipArchive::new(reader).map_err(io)?;
                for member in 0..archive.len() {
                    let entry = archive.by_index(member).map_err(io)?;
                    if entry.is_dir() {
                        continue;
                    }
                    let name = entry.name().to_owned();
                    let mut entry = Counted {
                        reader: entry,
                        count: Arc::clone(&count),
                    };
                    visit(member, name, &mut entry)?;
                }
            }
            Container::Gzip | Container::Stateless => {
                visit(0, file_name(path), &mut input)?;
            }
        }
        Ok(())
    }
}
struct Counted<R> {
    reader: R,
    count: Arc<AtomicU64>,
}
impl<R: Read> Read for Counted<R> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        let n = self.reader.read(bytes)?;
        self.count.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }
}

#[cfg(test)]
mod tests;

struct Cancellable<R> {
    reader: R,
    stop: Arc<AtomicBool>,
}
impl<R: Read> Read for Cancellable<R> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        if self.stop.load(Ordering::Relaxed) {
            return Err(std::io::Error::other("archive read cancelled"));
        }
        self.reader.read(bytes)
    }
}

impl<R: Read + Seek> Seek for Cancellable<R> {
    fn seek(&mut self, position: std::io::SeekFrom) -> std::io::Result<u64> {
        if self.stop.load(Ordering::Relaxed) {
            return Err(std::io::Error::other("archive seek cancelled"));
        }
        self.reader.seek(position)
    }
}
