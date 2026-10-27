//! Live sources run by an outside producer (`sinex-session`, a satellite's
//! `sinex-capture`): each source's capture writes its spool on its own
//! thread, and one loop produces what the spools hold to `sinexd` over the
//! NATS ingress. Both programs build their sources from the registry's formats
//! and share this loop.
use crate::sources;
use serde_json::Value;
use sinex_source_core::Source;
use sinex_source_core::binding::{Binding, InputFormat, Reader, Settings};
use sinex_source_core::capture::CaptureStop;
use sinex_source_core::drivers::producer::{Error, Position, Producer, Store};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Connection settings, kept until capture has started so an unavailable bus
/// cannot prevent spooling.
pub struct Connection {
    pub url: String,
    pub password_file: Option<std::path::PathBuf>,
    pub subjects: sinex_protocol::Subjects,
    pub producer: String,
    pub upload: bool,
}

#[derive(Default)]
struct Retry {
    failure: Option<&'static str>,
    delay: Duration,
    after: Option<Instant>,
}

impl Retry {
    fn failed(&mut self, name: &str, state: &'static str, error: &dyn std::fmt::Display) {
        if self.failure != Some(state) {
            eprintln!("{name}: publishing {state}: {error}; capture continues");
        }
        self.failure = Some(state);
        self.delay = (self.delay * 2)
            .max(Duration::from_secs(1))
            .min(Duration::from_secs(30));
        self.after = Some(Instant::now() + self.delay);
    }

    fn ready(&self) -> bool {
        self.after.is_none_or(|after| Instant::now() >= after)
    }

    fn recovered(&mut self, name: &str) {
        if self.failure.take().is_some() {
            eprintln!("{name}: publishing recovered");
        }
        self.delay = Duration::ZERO;
        self.after = None;
    }
}

/// One live source being captured and produced.
struct Live {
    source: Box<dyn Source>,
    binding: Binding,
    reader: Box<dyn Reader>,
    capture: CaptureTask,
    retry: Retry,
    health: sinex_source_core::drivers::spool_health::Counter,
}

struct CaptureTask {
    stop: Arc<CaptureStop>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl CaptureTask {
    fn stop(&mut self) {
        self.stop.stop();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for CaptureTask {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The binding of the live format `format` on `spool`.
pub fn binding(
    format: &str,
    spool: &Path,
    host: &str,
    options: Value,
) -> Result<(&'static InputFormat, Binding), String> {
    let k = sources::format(format)
        .filter(|k| k.capture.is_some())
        .ok_or_else(|| format!("{format}: not a live source"))?;
    let settings = Settings {
        options,
        ..Settings::new(host)
    };
    let plan = k
        .plan
        .ok_or_else(|| format!("{format}: no spool binding"))?;
    let binding = plan(spool, &settings)?
        .into_iter()
        .next()
        .ok_or_else(|| format!("{format}: no binding"))?;
    Ok((k, binding))
}

fn check_spool(path: &Path) -> std::io::Result<()> {
    use std::io::Write;
    let mut probe = tempfile::tempfile_in(path)?;
    probe.write_all(b"spool write check")?;
    probe.sync_data()
}

/// Start capture independently of publishing, retrying source failures.
fn start(
    k: &'static InputFormat,
    binding: Binding,
    positions: &Path,
    failed: tokio::sync::mpsc::UnboundedSender<String>,
) -> Result<Live, String> {
    let capture = k.capture.expect("live formats capture");
    // An unwritable spool is a local fault; reconnecting cannot repair it.
    std::fs::create_dir_all(&binding.input).map_err(|e| e.to_string())?;
    check_spool(&binding.input).map_err(|e| format!("{} spool: {e}", binding.name))?;
    // Construct fallible readers before starting a capture that must be joined.
    let reader = (k
        .reader
        .ok_or_else(|| format!("{}: no spool reader", k.name))?)(&binding)?
    .into_polled()?;
    let source = (k.source)(&binding)?;
    let health = health::initialize(positions, &binding)?;
    let stop = Arc::new(CaptureStop::new(None));
    let b = binding.clone();
    let thread_stop = stop.clone();
    let thread = std::thread::Builder::new()
        .spawn(move || {
            let mut last_failure = None;
            let mut delay = Duration::from_secs(1);
            while !thread_stop.stopped() {
                let started = Instant::now();
                let error = match capture(&b, &thread_stop) {
                    _ if thread_stop.stopped() => return,
                    Ok(_) => "source ended".to_owned(),
                    Err(error) => error,
                };
                // An unwritable spool is unrecoverable here. Source socket
                // and child failures can instead be retried within capture.
                if let Err(error) = check_spool(&b.input) {
                    let _ = failed.send(format!("{} spool: {error}", b.name));
                    return;
                }
                if last_failure.as_ref() != Some(&error) {
                    eprintln!("{} capture: {error}; retrying", b.name);
                }
                last_failure = Some(error);
                if started.elapsed() >= Duration::from_secs(30) {
                    delay = Duration::from_secs(1);
                }
                if thread_stop.wait(delay) {
                    return;
                }
                delay = (delay * 2).min(Duration::from_secs(30));
            }
        })
        .map_err(|e| e.to_string())?;
    Ok(Live {
        source,
        health,
        binding,
        reader,
        retry: Retry::default(),
        capture: CaptureTask {
            stop,
            thread: Some(thread),
        },
    })
}

/// Produce what every spool holds, every `every`, until `until` (forever
/// otherwise). Private mode pauses capture and production until it is off.
pub async fn run(
    connection: Connection,
    store: &Store,
    positions: &Path,
    bindings: Vec<(&'static InputFormat, Binding)>,
    every: Duration,
    until: Option<Instant>,
) -> Result<(), String> {
    run_with_private_mode(
        connection,
        store,
        positions,
        bindings,
        every,
        until,
        sinex_transport::private_mode::PrivateMode::default(),
    )
    .await
}

async fn run_with_private_mode(
    connection: Connection,
    store: &Store,
    positions: &Path,
    bindings: Vec<(&'static InputFormat, Binding)>,
    every: Duration,
    until: Option<Instant>,
    private_mode: sinex_transport::private_mode::PrivateMode,
) -> Result<(), String> {
    // Register before any capture thread or child can start.
    let shutdown = sinex_source_core::capture::shutdown_signal().map_err(|e| e.to_string())?;
    let (failed, mut failures) = tokio::sync::mpsc::unbounded_channel();
    let state_positions = positions.to_owned();
    let mut lives = tokio::task::spawn_blocking(move || {
        bindings
            .into_iter()
            .map(|(format, binding)| start(format, binding, &state_positions, failed.clone()))
            .collect::<Result<Vec<_>, _>>()
    })
    .await
    .map_err(|e| e.to_string())??;
    let health_bindings: Vec<_> = lives
        .iter()
        .map(|live| (live.binding.clone(), live.health.clone()))
        .collect();
    let deadline = async {
        match until {
            Some(until) => {
                tokio::time::sleep(until.saturating_duration_since(Instant::now())).await
            }
            None => std::future::pending::<()>().await,
        }
    };
    let result = tokio::select! {
        result = produce_spools(connection, store, positions, &mut lives, every, private_mode) => result,
        result = health::report(positions, health_bindings) => result,
        Some(error) = failures.recv() => Err(error),
        _ = shutdown => Ok(()),
        _ = deadline => Ok(()),
    };
    // Interrupt every source before joining any one blocked reader.
    for live in &lives {
        live.capture.stop.stop();
    }
    for live in &mut lives {
        live.capture.stop();
    }
    result
}

async fn produce_spools(
    connection: Connection,
    store: &Store,
    positions: &Path,
    lives: &mut [Live],
    every: Duration,
    private: sinex_transport::private_mode::PrivateMode,
) -> Result<(), String> {
    let mut declaration = sinex_protocol::ProducerHello::default();
    for live in lives.iter() {
        declaration.class = declaration.class.max(live.source.spec().class);
        declaration.event_types.extend(
            live.source
                .spec()
                .event_types
                .iter()
                .map(|name| (*name).to_owned()),
        );
    }
    declaration.event_types.sort();
    declaration.event_types.dedup();
    let mut producer = None;
    let mut retry = Retry::default();
    loop {
        if private.on() || !retry.ready() {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        if producer.is_none() {
            match Producer::connect(
                &connection.url,
                connection.password_file.as_deref(),
                connection.subjects.clone(),
                &connection.producer,
                &declaration,
            )
            .await
            {
                Ok(connected) => {
                    producer = Some(if connection.upload {
                        connected.upload_bytes(store.clone())
                    } else {
                        connected
                    });
                    retry.recovered(&connection.producer);
                }
                Err(Error::Configuration(reason)) => {
                    for live in lives.iter() {
                        live.health.lock().unwrap().error = Some(reason.clone());
                    }
                    notifier::raise(&connection.producer, &reason).await;
                    return std::future::pending().await;
                }
                Err(error) => {
                    retry.failed(&connection.producer, "connecting", &error);
                    continue;
                }
            }
        }
        let producer = producer.as_mut().expect("connected above");
        for live in lives.iter() {
            producer.monitor(&live.binding.name, live.health.clone());
        }
        let mut had_material = false;
        for live in lives
            .iter_mut()
            .filter(|live| live.retry.ready() && live.health.lock().unwrap().error.is_none())
        {
            // A failed batch is replayed with its original frame IDs before
            // the reader may advance to newly captured material.
            match produce_spool(producer, store, positions, live).await {
                Ok(produced) => {
                    had_material |= produced;
                    live.retry.recovered(&live.binding.name);
                }
                Err(
                    Error::Refused {
                        reason,
                        kind:
                            sinex_model::FrameRefusal::Malformed | sinex_model::FrameRefusal::Content,
                        recorded: true,
                        ..
                    }
                    | Error::Configuration(reason),
                ) => {
                    live.health.lock().unwrap().error = Some(reason.clone());
                    notifier::raise(&live.binding.name, &reason).await;
                }
                Err(error @ (Error::Nats(_) | Error::NoAck(_) | Error::Refused { .. })) => {
                    let state = match error {
                        Error::NoAck(_) => "awaiting acknowledgment",
                        Error::Refused { .. } => "retrying bytes",
                        _ => "unavailable",
                    };
                    live.retry.failed(&live.binding.name, state, &error);
                }
                // Unwritable spool/state storage or an undecodable local
                // cursor cannot be repaired by reconnecting to the bus.
                Err(error) => return Err(error.to_string()),
            }
        }
        if had_material {
            // A backlog gets another turn without waiting for the next poll.
            tokio::task::yield_now().await;
        } else {
            let next_retry = lives.iter().filter_map(|live| live.retry.after).min();
            let wait = next_retry.map_or(every, |after| {
                every.min(after.saturating_duration_since(Instant::now()))
            });
            tokio::time::sleep(wait.max(Duration::from_millis(100))).await;
        }
    }
}

async fn produce_spool(
    producer: &mut Producer,
    store: &Store,
    positions: &Path,
    live: &mut Live,
) -> Result<bool, Error> {
    let recovering = Producer::unacknowledged_position(&live.binding.name, positions)?.is_some();
    let position = producer
        .position(&live.binding.name, positions, store)
        .await?;
    if recovering {
        remove_passed(live, &position)?;
        return Ok(true);
    }
    let acquired = live.reader.poll(&position.cursor)?;
    let had_material = acquired.material.is_some();
    producer
        .produce(
            live.source.as_ref(),
            &live.binding.name,
            acquired,
            store,
            positions,
            &live.binding.input,
        )
        .await?;
    let committed = Position::load(positions, &live.binding.name)?;
    remove_passed(live, &committed)?;
    Ok(had_material)
}

fn remove_passed(live: &mut Live, position: &Position) -> Result<(), Error> {
    live.health.lock().unwrap().capture_failures = position.capture_failures.len();
    let mut cursor = position.cursor.clone();
    if !position.capture_failures.is_empty() {
        cursor["keep_files"] = serde_json::to_value(
            position
                .capture_failures
                .iter()
                .flat_map(|failure| {
                    failure
                        .ranges
                        .iter()
                        .map(|range| range.path.as_os_str().as_bytes().to_vec())
                })
                .collect::<Vec<_>>(),
        )
        .expect("paths serialize");
    }
    live.reader.remove_passed(&cursor)?;
    Ok(())
}

mod health;
mod notifier;

/// `key=value` pairs as source options: numbers and booleans keep their types, a repeated key
/// collects a list, and directory lists (`roots`, `skip`) are lists even with
/// one entry.
pub fn options(pairs: &[String]) -> Result<Value, String> {
    let mut map = serde_json::Map::new();
    for pair in pairs {
        let (k, v) = pair
            .split_once('=')
            .ok_or_else(|| format!("option {pair}: expected key=value"))?;
        let value = match v {
            "true" => Value::Bool(true),
            "false" => Value::Bool(false),
            _ => v
                .parse::<u64>()
                .map(Value::from)
                .unwrap_or_else(|_| Value::from(v)),
        };
        match map.get_mut(k) {
            Some(Value::Array(list)) => list.push(value),
            Some(first) => *first = Value::Array(vec![first.take(), value]),
            None if matches!(k, "roots" | "skip") => {
                map.insert(k.to_owned(), Value::Array(vec![value]));
            }
            None => {
                map.insert(k.to_owned(), value);
            }
        }
    }
    Ok(if map.is_empty() {
        Value::Null
    } else {
        Value::Object(map)
    })
}

#[cfg(test)]
mod tests;
