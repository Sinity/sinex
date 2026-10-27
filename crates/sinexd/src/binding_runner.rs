//! Discover source inputs every minute, reuse persisted bindings and read them
//! continuously through the writer. An unavailable input is retried independently;
//! private mode pauses reading.
use crate::binding_reader::{self, BindingReader};
use crate::sealing::Sink;
use crate::writer::WriterHandle;
use sinex_source_core::{Binding, InputFormat, Settings};
use sinex_sources::sources;
use sinex_store::{Blobs, runs};
use sqlx::{Connection, PgConnection};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::sync::watch;

pub(crate) use sinex_sources::discovery::Configuration as Config;
use sinex_sources::discovery::{self, Availability};

pub(crate) fn plan(
    config: &Config,
    host: &str,
) -> Result<Vec<(&'static InputFormat, Binding)>, String> {
    let inputs = discovery::discover(&config.context(host))?;
    Ok(discovery::reconcile(
        inputs
            .into_iter()
            .flat_map(|input| {
                let format = sources::format(&input.format).expect("discovered format");
                input
                    .bindings
                    .into_iter()
                    .filter(move |_| !input.capture)
                    .map(move |binding| (format, binding))
            })
            .collect(),
        &[],
    )
    .bindings)
}

fn emitted(config: &Config) -> Result<Vec<&'static str>, String> {
    discovery::validate(&config.context(""))?;
    let mut types: Vec<_> = sources::specs()
        .into_iter()
        .filter(|spec| !config.exclude.get(spec.name).is_some_and(Option::is_some))
        .flat_map(|spec| spec.event_types.iter().copied())
        .collect();
    types.sort_unstable();
    types.dedup();
    Ok(types)
}

/// A saved name belongs to the source and input, independent of which current
/// parser discovers it. Reconciliation compares only the proposed parser.
async fn saved(
    conn: &mut PgConnection,
    database: &str,
    host: &str,
) -> Result<Vec<Binding>, String> {
    let rows = match sinex_store::bindings::all(conn).await {
        Ok(rows) => rows,
        Err(_) => {
            *conn = PgConnection::connect(database)
                .await
                .map_err(|error| error.to_string())?;
            sinex_store::bindings::all(conn)
                .await
                .map_err(|error| error.to_string())?
        }
    };
    let mut out = Vec::new();
    for row in rows {
        let settings: Settings = match serde_json::from_value(row.settings) {
            Ok(settings) => settings,
            Err(error) => {
                eprintln!("sinexd: binding {}: unavailable: {error}", row.binding);
                continue;
            }
        };
        if settings.host != host {
            continue;
        }
        let parsers: std::collections::BTreeSet<_> = sources::specs()
            .into_iter()
            .filter(|spec| spec.name == row.source)
            .map(|spec| spec.parser.to_owned())
            .collect();
        for parser in parsers {
            out.push(Binding {
                name: row.binding.clone(),
                source: row.source.clone(),
                input: std::path::PathBuf::from(&row.input),
                parser,
                settings: settings.clone(),
            });
        }
    }
    Ok(out)
}

/// The bindings being read, and what was last said about each path and
/// binding, so a path that stays empty or a binding that cannot start is
/// reported once, not every round.
#[derive(Default)]
struct Bindings {
    readers: Vec<BindingReader>,
    said: BTreeMap<String, String>,
}

impl Bindings {
    /// Plan `config` again and start the bindings not read yet: files a source
    /// writes later (a new day's file, a new recording) are read without a
    /// restart.
    fn extend(&mut self, config: &Config, host: &str, saved: &[Binding]) {
        let inputs = match discovery::status(&config.context(host)) {
            Ok(inputs) => inputs,
            Err(error) => {
                self.say("plan", format!("sinexd: sources: {error}"));
                return;
            }
        };
        let mut proposed = Vec::new();
        for input in inputs {
            let key = format!(
                "input {} {} {} {}",
                input.source,
                input.format,
                if input.capture { "capture" } else { "read" },
                input
                    .input
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_default()
            );
            if matches!(
                input.availability,
                Availability::Unavailable | Availability::WaitingForInput
            ) {
                self.say(
                    &key,
                    format!(
                        "sinexd: {}: {:?}: {}",
                        input.source,
                        input.availability,
                        input.reason.as_deref().unwrap_or("no input")
                    ),
                );
            } else {
                self.said.remove(&key);
            }
            let format = sources::format(&input.format).expect("discovered format");
            if !input.capture {
                proposed.extend(input.bindings.into_iter().map(|binding| (format, binding)));
            }
        }
        let planned = discovery::reconcile(proposed, saved);
        for (binding, error) in planned.unavailable {
            self.readers
                .retain(|reader| reader.binding.name != binding.name);
            self.say(
                &format!("binding {}", binding.name),
                format!("sinexd: {}: unavailable: {error}", binding.input.display()),
            );
        }
        for (format, binding) in planned.bindings {
            if let Some(old) = self.readers.iter_mut().find(|reader| {
                reader.binding.name == binding.name && reader.binding.parser == binding.parser
            }) {
                if old.binding.settings == binding.settings {
                    continue;
                }
                match BindingReader::new(format, binding) {
                    Ok(reader) => *old = reader,
                    Err(error) => self.say("plan", format!("sinexd: {error}")),
                }
                continue;
            }
            let name = binding.name.clone();
            match BindingReader::new(format, binding) {
                Ok(reader) => {
                    self.said.remove(&format!("binding {name}"));
                    self.readers.push(reader);
                }
                Err(error) => self.say(
                    &format!("binding {name}"),
                    format!("sinexd: binding {name}: {error}"),
                ),
            }
        }
    }

    fn say(&mut self, key: &str, message: String) {
        if self.said.get(key) != Some(&message) {
            eprintln!("{message}");
            self.said.insert(key.to_owned(), message);
        }
    }
}

/// How often the sources are planned again for new files.
const PLAN_EVERY: Duration = Duration::from_secs(60);

/// Read the bindings `config` enables, in rounds every `interval`, until
/// `stop`.
pub(crate) async fn run(
    connection: (PgConnection, String),
    blobs: Blobs,
    writer: WriterHandle,
    host: String,
    config: Config,
    interval: Duration,
    mut stop: watch::Receiver<bool>,
) -> Result<(), String> {
    let (mut read_conn, database) = connection;
    let emits = emitted(&config)?;
    let manifest = runs::Manifest {
        name: "sinexd bindings",
        kind: "source",
        version: env!("CARGO_PKG_VERSION"),
        build: option_env!("SINEX_BUILD").unwrap_or("dev"),
        emits: &emits,
    };
    let run = runs::start(&mut read_conn, &manifest, &host)
        .await
        .map_err(|e| e.to_string())?;
    let sink = Sink(blobs.clone());
    let private = sinex_transport::private_mode::PrivateMode::default();
    let mut bindings = Bindings::default();
    let mut planned_at: Option<std::time::Instant> = None;
    loop {
        if !private.on() {
            if planned_at.is_none_or(|at| at.elapsed() >= PLAN_EVERY) {
                let before = bindings.readers.len();
                match saved(&mut read_conn, &database, &host).await {
                    Ok(saved) => {
                        bindings.said.remove("saved");
                        bindings.extend(&config, &host, &saved);
                    }
                    Err(error) => bindings.say(
                        "saved",
                        format!("sinexd: saved bindings unavailable: {error}"),
                    ),
                }
                if bindings.readers.len() != before {
                    eprintln!("sinexd: reading {} bindings", bindings.readers.len());
                }
                planned_at = Some(std::time::Instant::now());
            }
            // Each failure is logged as it happens, once until it changes or
            // the binding reads again.
            let said = &mut bindings.said;
            binding_reader::read(
                &mut read_conn,
                &blobs,
                &writer,
                run,
                &sink,
                &mut bindings.readers,
                stop.clone(),
                binding_reader::Access::default(),
                &mut |binding, result| {
                    let key = format!("binding {binding}");
                    match result {
                        Err(e) => {
                            let message = format!("sinexd: binding {binding}: {e}");
                            if said.get(&key) != Some(&message) {
                                eprintln!("{message}");
                                said.insert(key, message);
                            }
                        }
                        Ok(_) => {
                            said.remove(&key);
                        }
                    }
                },
            )
            .await;
        }
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = stop.wait_for(|s| *s) => break,
        }
    }
    runs::end(&mut read_conn, run)
        .await
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests;
