//! Staging: Sinex is handed a file, a directory or a database to read once.
//! A stage is an operation: it plans every path ([`sinex_sources::stage`]),
//! records the inventory before any record is parsed, reads each planned file
//! through the same bindings, drivers, drain and admission the daemon uses,
//! stores what no parser reads as material with no parser, and ends with the
//! inventory and each product's accounting. The daemon runs it for
//! `sinexctl stage`, and `sinexd admin stage` runs the same command with its
//! own writer while the daemon is stopped.
//!
//! A staged input is read to its end and then finished: staging the same
//! unchanged path again reads nothing new, and a newer export is a new path.
//! An interrupted stage is read again from its recorded plan (`resume`): the
//! files it finished keep their accounting, files it stored are not stored
//! again, and every binding continues from its committed cursor with the
//! accounting committed with it ([`Committed`]), so the stage's accounting
//! covers everything read before and after the interruption.
use crate::material_access::{Access, Sink};
use crate::writer::{self, StageWrite, WriterHandle};
use serde_json::{Value, json};
use sinex_model::lifecycle::OperationResult;
use sinex_model::{Accounting, Class, InputRef, Manifest, MaterialId, OperationId, RunId};
use sinex_source_core::{Binding, MaterialSink, Settings, Source, drivers};
use sinex_sources::sources;
use sinex_sources::stage::{FileReading, Item, StagePlan};
use sinex_store::{Blobs, bindings, positions};
use sqlx::PgConnection;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::sync::watch;

/// What `stage` was asked to do: its scope, kept with the operation so a
/// resumed stage plans exactly the same.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Request {
    pub items: Vec<Item>,
    /// The account an export belongs to, recorded with every binding as the
    /// `account` option.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
    /// When the export was taken, as the operator states it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub export_date: Option<String>,
    pub host: String,
    /// The zone sources with local times were written in, recorded with each
    /// binding; the system zone when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zone: Option<String>,
}

/// The staged items for `paths` read with `parsers`: none (each file is
/// recognised by detection), one for every path, or one per path in order.
pub(crate) fn items(paths: &[String], parsers: &[String]) -> Result<Vec<Item>, String> {
    if !parsers.is_empty() && parsers.len() != 1 && parsers.len() != paths.len() {
        return Err(format!(
            "{} parsers for {} paths: name one parser for every path, or one per path",
            parsers.len(),
            paths.len()
        ));
    }
    Ok(paths
        .iter()
        .enumerate()
        .map(|(index, path)| Item {
            path: PathBuf::from(path),
            parser: match parsers.len() {
                0 => None,
                1 => Some(parsers[0].clone()),
                _ => Some(parsers[index].clone()),
            },
        })
        .collect())
}

/// The bindings the daemon's stages are reading now, by name: a binding has
/// one reader at a time, or two would race its cursor.
pub(crate) type Running = std::sync::Arc<std::sync::Mutex<BTreeSet<String>>>;

/// A stage's hold on its bindings, released when it ends, however it ends.
pub(crate) struct Claim {
    running: Running,
    names: Vec<String>,
}

impl Claim {
    /// Hold every binding of `plan`, or none when one is being read.
    pub(crate) fn take(running: &Running, plan: &StagePlan) -> Result<Self, String> {
        let names: Vec<String> = plan.bindings().map(|(_, b)| b.name.clone()).collect();
        let mut held = running
            .lock()
            .expect("the running stages are never poisoned");
        if let Some(busy) = names.iter().find(|name| held.contains(*name)) {
            return Err(format!("{busy} is being read by another stage now"));
        }
        held.extend(names.iter().cloned());
        Ok(Self {
            running: running.clone(),
            names,
        })
    }
}

impl Drop for Claim {
    fn drop(&mut self) {
        let mut held = self
            .running
            .lock()
            .expect("the running stages are never poisoned");
        for name in &self.names {
            held.remove(name);
        }
    }
}

/// An agent stages only paths under `roots` (the declared source locations
/// and the operator's staging roots); any other path needs the operator.
/// The refusal names the roots.
pub(crate) fn within_roots(items: &[Item], roots: &[PathBuf]) -> Result<(), String> {
    let resolve =
        |path: &std::path::Path| std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let resolved: Vec<PathBuf> = roots.iter().map(|root| resolve(root)).collect();
    for item in items {
        let path = resolve(&item.path);
        if !resolved.iter().any(|root| path.starts_with(root)) {
            let listed: Vec<String> = roots.iter().map(|r| r.display().to_string()).collect();
            return Err(format!(
                "{} is outside the places an agent may stage from; staging it needs the operator. An agent may stage under: {}",
                item.path.display(),
                if listed.is_empty() {
                    "nothing on this host".to_owned()
                } else {
                    listed.join(", ")
                }
            ));
        }
    }
    Ok(())
}

/// Refuse a plan any of whose bindings the daemon reads continuously, by
/// source and resolved input: that input is staged already, and a second
/// reader would race its cursor.
pub(crate) fn not_continuous(
    plan: &StagePlan,
    continuous: &[(&'static sinex_source_core::InputFormat, Binding)],
) -> Result<(), String> {
    let resolved = |b: &Binding| {
        (
            b.source.clone(),
            std::fs::canonicalize(&b.input).unwrap_or_else(|_| b.input.clone()),
        )
    };
    let read: BTreeSet<_> = continuous.iter().map(|(_, b)| resolved(b)).collect();
    for (_, binding) in plan.bindings() {
        if read.contains(&resolved(binding)) {
            return Err(format!(
                "{} is read continuously by {} from its canonical location; it needs no stage",
                binding.input.display(),
                binding.source
            ));
        }
    }
    Ok(())
}

impl Request {
    pub(crate) fn scope(&self) -> Value {
        serde_json::to_value(self).expect("stage requests serialize")
    }

    pub(crate) fn from_scope(scope: &Value) -> Result<Self, String> {
        serde_json::from_value(scope.clone()).map_err(|e| format!("stored stage scope: {e}"))
    }

    fn settings(&self) -> Settings {
        let mut settings = Settings::new(self.host.clone());
        settings.zone.clone_from(&self.zone);
        if let Some(account) = &self.account {
            settings.options = json!({ "account": account });
        }
        settings
    }

    /// Plan every path. Reads only listings and the few bytes detection
    /// tests, but blocks on the file system, so callers run it off the
    /// async workers.
    pub(crate) fn plan(&self) -> Result<StagePlan, String> {
        for item in &self.items {
            if !item.path.is_absolute() {
                return Err(format!(
                    "{}: a staged path is absolute, so the daemon and the record agree on it",
                    item.path.display()
                ));
            }
        }
        sinex_sources::stage::plan(sources::formats(), &self.items, &self.settings())
    }
}

/// How far one product of one staged file has been read.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProductState {
    #[default]
    Queued,
    Read,
    Failed,
}

/// One product (a source) of one staged file, as reading goes.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
struct Product {
    state: ProductState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    accounting: Accounting,
}

/// A stage's progress: the plan, each file's products, the material a file
/// with no parser was stored as, and what each binding's committed batches
/// accounted for before the stage was interrupted.
struct Progress {
    plan: StagePlan,
    products: Vec<BTreeMap<String, Product>>,
    stored: Vec<Option<MaterialId>>,
    checkpoints: BTreeMap<String, Accounting>,
}

impl Progress {
    fn new(plan: StagePlan) -> Self {
        let products = plan
            .files
            .iter()
            .map(|file| {
                file.bindings
                    .iter()
                    .map(|(_, b)| (b.source.clone(), Product::default()))
                    .collect()
            })
            .collect();
        let stored = vec![None; plan.files.len()];
        Self {
            plan,
            products,
            stored,
            checkpoints: BTreeMap::new(),
        }
    }

    /// The progress a stage saved before it was interrupted: what each
    /// file's products read, which files it stored, and each binding's
    /// checkpoint. A file whose products all read is not read again; a
    /// product read in part continues from its bindings' committed cursors,
    /// starting from the accounting committed with them.
    fn resumed(plan: StagePlan, saved: &Value) -> Result<Self, String> {
        let mut progress = Self::new(plan);
        if let Some(checkpoints) = saved["checkpoints"].as_object() {
            for (binding, checkpoint) in checkpoints {
                let accounting: Accounting =
                    serde_json::from_value(checkpoint["accounting"].clone())
                        .map_err(|e| format!("saved stage checkpoint of {binding}: {e}"))?;
                progress.checkpoints.insert(binding.clone(), accounting);
            }
        }
        let saved_files = saved["files"].as_array().cloned().unwrap_or_default();
        for (index, file) in progress.plan.files.iter().enumerate() {
            let Some(before) = saved_files.iter().find(|f| f["path"] == json!(file.path)) else {
                continue;
            };
            if let Some(material) = before.get("material").filter(|m| !m.is_null()) {
                progress.stored[index] = Some(
                    serde_json::from_value(material.clone())
                        .map_err(|e| format!("saved stage progress: {e}"))?,
                );
            }
            let products: BTreeMap<String, Product> =
                serde_json::from_value(before["products"].clone())
                    .map_err(|e| format!("saved stage progress: {e}"))?;
            for (source, product) in products {
                if product.state == ProductState::Read
                    && let Some(current) = progress.products[index].get_mut(&source)
                {
                    *current = product;
                }
            }
        }
        // A product read in part starts from what its bindings committed.
        for (file, products) in progress.plan.files.iter().zip(&mut progress.products) {
            for (_, binding) in &file.bindings {
                if let Some(product) = products.get_mut(&binding.source)
                    && product.state != ProductState::Read
                    && let Some(committed) = progress.checkpoints.get(&binding.name)
                {
                    product.accounting.add(committed);
                }
            }
        }
        Ok(progress)
    }

    /// Whether every product of file `index` has been read.
    fn file_read(&self, index: usize) -> bool {
        self.products[index]
            .values()
            .all(|p| p.state == ProductState::Read)
    }

    /// The inventory as the operations log and `stage-show` hold it.
    fn inventory(&self) -> Value {
        let files: Vec<Value> = self
            .plan
            .files
            .iter()
            .zip(&self.products)
            .zip(&self.stored)
            .map(|((file, products), stored)| {
                let mut value = serde_json::to_value(file).expect("staged files serialize");
                value["products"] = json!(products);
                if let Some(material) = stored {
                    value["material"] = json!(material);
                }
                value
            })
            .collect();
        json!({ "files": files })
    }

    /// The whole stage's accounting.
    fn total(&self) -> Accounting {
        let mut total = Accounting::default();
        for product in self.products.iter().flat_map(BTreeMap::values) {
            total.add(&product.accounting);
        }
        total
    }
}

/// How a stage ended, for its caller.
pub(crate) struct Outcome {
    pub operation: OperationId,
    pub result: OperationResult,
    /// The operation, its inventory, its accounting and the totals per
    /// source.
    pub report: Value,
    pub failures: Vec<String>,
}

/// What a stage reads and writes through.
pub(crate) struct Staging<'a> {
    pub read_conn: &'a mut PgConnection,
    pub blobs: &'a Blobs,
    pub writer: &'a WriterHandle,
    pub access: Access<'a>,
    /// The daemon's shutdown: the bindings stop reading, the stage ends
    /// `stopped_early`, and `resume` reads it again later.
    pub stop: watch::Receiver<bool>,
}

impl Staging<'_> {
    /// Record a new stage with its inventory, then read it. `begun` is told
    /// the operation and its inventory once the stage is recorded, before
    /// anything is read.
    pub(crate) async fn run(
        &mut self,
        operation: OperationId,
        actor: &str,
        request: &Request,
        plan: StagePlan,
        begun: tokio::sync::oneshot::Sender<Value>,
    ) -> Result<Outcome, String> {
        let progress = Progress::new(plan);
        self.writer
            .stage(StageWrite::Begin {
                operation,
                actor: actor.to_owned(),
                scope: request.scope(),
                inventory: progress.inventory(),
            })
            .await
            .map_err(|e| e.to_string())?;
        self.read(operation, request, progress, begun).await
    }

    /// Read again a stage that did not end, from its recorded plan, its saved
    /// progress and its bindings' committed cursors.
    pub(crate) async fn resume(
        &mut self,
        operation: OperationId,
        request: &Request,
        plan: StagePlan,
        saved: &Value,
        begun: tokio::sync::oneshot::Sender<Value>,
    ) -> Result<Outcome, String> {
        let progress = Progress::resumed(plan, saved)?;
        self.writer
            .stage(StageWrite::Reopen { operation })
            .await
            .map_err(|e| e.to_string())?;
        self.read(operation, request, progress, begun).await
    }

    async fn read(
        &mut self,
        operation: OperationId,
        request: &Request,
        mut progress: Progress,
        begun: tokio::sync::oneshot::Sender<Value>,
    ) -> Result<Outcome, String> {
        // The caller may have stopped waiting; the stage reads on.
        let _ = begun.send(json!({
            "operation": operation,
            "result": Value::Null,
            "inventory": progress.inventory(),
        }));
        let mut failures = Vec::new();
        let mut per_source: Vec<(String, Totals)> = Vec::new();
        // One run per source, parser version and host for the whole stage.
        let mut rounds = crate::binding_reader::Rounds::default();
        for index in 0..progress.plan.files.len() {
            let path = progress.plan.files[index].path.clone();
            let planned = progress.plan.files[index].bindings.clone();
            if planned.is_empty() {
                if progress.stored[index].is_some() {
                    continue;
                }
                match self.store_unparsed(request, index, &progress).await {
                    Ok(material) => progress.stored[index] = Some(material),
                    Err(error) => failures.push(format!("{}: {error}", path.display())),
                }
            } else {
                if progress.file_read(index) {
                    continue;
                }
                let mut readers = Vec::new();
                for (format, binding) in planned {
                    match crate::binding_reader::BindingReader::new(format, binding.clone()) {
                        Ok(mut reader) => {
                            reader.committed = Some(Committed {
                                operation,
                                accounting: progress
                                    .checkpoints
                                    .get(&binding.name)
                                    .cloned()
                                    .unwrap_or_default(),
                            });
                            readers.push(reader);
                        }
                        Err(error) => {
                            fail(&mut progress, index, &binding.source, &error, &mut failures);
                        }
                    }
                }
                let results = crate::binding_reader::read(
                    &mut *self.read_conn,
                    self.blobs,
                    self.writer,
                    &Sink(self.blobs.clone()),
                    &mut readers,
                    &mut rounds,
                    self.stop.clone(),
                    self.access,
                    &mut |_, _| {},
                )
                .await;
                for (source, result) in results {
                    match result {
                        Ok(totals) => {
                            let product =
                                progress.products[index].entry(source.clone()).or_default();
                            // Another binding of the same source may have failed.
                            if product.state != ProductState::Failed {
                                product.state = ProductState::Read;
                            }
                            product.accounting.add(&totals.accounting);
                            if let Err(imbalance) = product.accounting.balanced() {
                                let error = format!("accounting does not balance: {imbalance}");
                                fail(&mut progress, index, &source, &error, &mut failures);
                            }
                            match per_source.iter_mut().find(|(s, _)| *s == source) {
                                Some((_, t)) => t.add(totals),
                                None => per_source.push((source, totals)),
                            }
                        }
                        Err(error) => fail(&mut progress, index, &source, &error, &mut failures),
                    }
                }
            }
            self.writer
                .stage(StageWrite::Progress {
                    operation,
                    inventory: progress.inventory(),
                })
                .await
                .map_err(|e| e.to_string())?;
        }
        if let Err(error) = rounds.end(&mut *self.read_conn).await {
            failures.push(error);
        }
        let result = if failures.is_empty() {
            OperationResult::Ok
        } else {
            OperationResult::StoppedEarly
        };
        let total = progress.total();
        let counts = json!({
            "files": progress.plan.files.len(),
            "stored_without_parser": progress.stored.iter().flatten().count(),
            "accounting": total,
        });
        let inventory = progress.inventory();
        self.writer
            .stage(StageWrite::End {
                operation,
                result,
                counts: counts.clone(),
                inventory: inventory.clone(),
                refusals: json!(failures),
            })
            .await
            .map_err(|e| e.to_string())?;
        let sources: serde_json::Map<String, Value> = per_source
            .iter()
            .map(|(s, t)| (s.clone(), t.to_json()))
            .collect();
        Ok(Outcome {
            operation,
            result,
            report: json!({
                "operation": operation,
                "result": result,
                "inventory": inventory,
                "counts": counts,
                "sources": sources,
                "failures": failures,
            }),
            failures,
        })
    }

    /// Store a file no parser reads, byte for byte, as material with no
    /// parser. Each stage of it is a new material of the same input. A file
    /// a sealed source would read is not stored in the clear: it is staged
    /// with that source's parser named.
    async fn store_unparsed(
        &mut self,
        request: &Request,
        index: usize,
        progress: &Progress,
    ) -> Result<MaterialId, String> {
        let file = &progress.plan.files[index];
        let candidates = match &file.reading {
            FileReading::Unparsed { candidates } => candidates.clone(),
            FileReading::Parsed { .. } => vec![],
        };
        if let Some(sealed) = candidates.iter().find(|name| {
            sources::format(name).is_some_and(|format| {
                (format.specs)()
                    .iter()
                    .any(|spec| spec.class == Class::Sealed)
            })
        }) {
            return Err(format!(
                "{sealed} reads sealed records, so the file is not stored unsealed without a parser; stage it with --parser"
            ));
        }
        let path = file.path.clone();
        let sink = Sink(self.blobs.clone());
        let (blob, observation) = tokio::task::spawn_blocking(move || {
            let observation = drivers::file_facts(&path).map_err(|e| e.to_string())?;
            let blob = sink.put_file(&path)?;
            Ok::<_, String>((blob, observation))
        })
        .await
        .map_err(|e| e.to_string())??;
        let locator = file.path.display().to_string();
        let mut details = serde_json::Map::new();
        details.insert("host".into(), json!(request.host));
        details.insert("candidates".into(), json!(candidates));
        if let Some(account) = &request.account {
            details.insert("account".into(), json!(account));
        }
        if let Some(date) = &request.export_date {
            details.insert("export_date".into(), json!(date));
        }
        let manifest = Manifest {
            path: Some(locator.clone()),
            container: file
                .container
                .map(|c| serde_json::to_value(c).expect("containers serialize"))
                .and_then(|c| c.as_str().map(str::to_owned)),
            details,
            ..Manifest::default()
        };
        let id = MaterialId::new();
        self.writer
            .stage(StageWrite::Unparsed(Box::new(
                sinex_store::stages::Unparsed {
                    id,
                    binding: format!("stage:{locator}@{}", request.host),
                    input: InputRef {
                        kind: "file".into(),
                        locator,
                        host: request.host.clone(),
                    },
                    length: observation.size.unwrap_or(0),
                    observation,
                    blob,
                    manifest,
                },
            )))
            .await
            .map_err(|e| e.to_string())?;
        Ok(id)
    }
}

/// `source`'s binding of file `index` failed with `error`.
fn fail(
    progress: &mut Progress,
    index: usize,
    source: &str,
    error: &str,
    failures: &mut Vec<String>,
) {
    let file = &mut progress.plan.files[index];
    file.mark_failed(source, error);
    let product = progress.products[index]
        .entry(source.to_owned())
        .or_default();
    product.state = ProductState::Failed;
    product.error = Some(error.to_owned());
    failures.push(format!("{} ({source}): {error}", file.path.display()));
}

/// Stage `request` with this process's own writer and wait for the end, as
/// `sinexd admin stage` does, for tests that stage in process.
#[cfg(test)]
pub(crate) async fn local(
    database: &str,
    store: &std::path::Path,
    request: Request,
    time_settings: sinex_model::time_settings::TimeSettings,
) -> Result<Outcome, String> {
    let mut ctx =
        crate::reader_context::ReaderContext::open(database, store, time_settings).await?;
    let planned = request.clone();
    let plan = tokio::task::spawn_blocking(move || planned.plan())
        .await
        .map_err(|e| e.to_string())??;
    let (begun, _) = tokio::sync::oneshot::channel();
    let (_running, stop) = watch::channel(false);
    let outcome = Staging {
        read_conn: &mut ctx.read_conn,
        blobs: &ctx.blobs,
        writer: &ctx.writer,
        access: Access::default(),
        stop,
    }
    .run(OperationId::new(), "operator", &request, plan, begun)
    .await;
    ctx.close().await?;
    outcome
}

/// Totals for one binding or source while it is read: the accounting plus
/// what the pipeline spent.
#[derive(Default)]
pub(crate) struct Totals {
    pub(crate) accounting: Accounting,
    polls: usize,
    materials: usize,
    material_bytes: usize,
    batches: usize,
    dirty_keys: usize,
    admit_time: Duration,
    poll_time: Duration,
    parse_time: Duration,
    queue_time: Duration,
    service_time: Duration,
    wall: Duration,
    files_stored: u64,
    pass_decoded_bytes: u64,
    pass_record_bytes: u64,
    /// A staged binding's accounting as it is committed, with its cursor.
    pub(crate) committed: Option<Committed>,
}

impl Totals {
    pub(crate) fn committed(committed: Option<Committed>) -> Self {
        Self {
            committed,
            ..Self::default()
        }
    }
}

/// What a staged binding's committed batches account for in the stage, saved
/// with each batch's cursor in the same transaction ([`writer::StageCheckpoint`]).
/// A resumed stage starts each binding from it, so the accounting covers what
/// was read before the interruption as well as after.
///
/// A material being read counts its committed records as framed: a resumed
/// driver frames only the records after the committed cursor. Once all of a
/// material is committed, the driver's own count replaces them in the
/// binding's later checkpoints. The stage's reported accounting comes from
/// the driver's counts of what each run read, so its identity compares the
/// driver with the drain; only the part of a material committed before an
/// interruption is counted by its committed records.
#[derive(Clone, Debug)]
pub(crate) struct Committed {
    pub(crate) operation: OperationId,
    pub(crate) accounting: Accounting,
}

/// What admission decided for a batch's readings. Admission counts a reading
/// that replaced the live one among those it admitted, and offers a removed
/// reading for each live reading of a record the source dropped.
pub(crate) fn decided(a: &mut Accounting, out: &sinex_store::Outcome) {
    a.readings += out.removed_at_source as u64;
    a.new += out.admitted.saturating_sub(out.replaced) as u64;
    a.revised += out.replaced as u64;
    a.same += out.repeats as u64;
    a.older += out.older as u64;
    a.superseded += out.superseded as u64;
}

/// Store `b`'s source, input and settings under its name.
pub(crate) async fn record(read_conn: &mut PgConnection, b: &Binding) -> Result<(), String> {
    bindings::save(
        read_conn,
        &bindings::Stored {
            binding: b.name.clone(),
            source: b.source.clone(),
            input: b.input.display().to_string(),
            settings: serde_json::to_value(&b.settings).map_err(|e| e.to_string())?,
        },
    )
    .await
    .map_err(|e| e.to_string())
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn drive(
    read_conn: &mut PgConnection,
    handle: &writer::WriterHandle,
    run: RunId,
    source: std::sync::Arc<dyn Source>,
    binding: &str,
    host: &str,
    sink: &Sink,
    access: crate::material_access::Access<'_>,
    minimum_class: sinex_model::Class,
    reader: &mut crate::binding_reader::polled::PolledReader,
    stop: tokio::sync::watch::Receiver<bool>,
    committed: Option<Committed>,
) -> Result<Totals, String> {
    let mut totals = Totals::committed(committed);
    let started = Instant::now();
    // The source's class: its declaration, raised by the operator's floor.
    // A sealed source seals each material and its events as they are read.
    let floor = sinex_store::privacy::load(read_conn, std::iter::once(source.spec().name))
        .await
        .map_err(|e| e.to_string())?
        .floors
        .get(source.spec().name)
        .copied()
        .unwrap_or_default();
    let effective_class = source.spec().class.max(floor).max(minimum_class);
    let material =
        crate::material_access::MaterialAccess::new(sink.0.clone(), effective_class, access)
            .map_err(|e| format!("{}: {e}", source.spec().name))?;
    let checked_stop = stop.clone();
    let check: std::sync::Arc<dyn Fn() -> Result<(), String> + Send + Sync> =
        std::sync::Arc::new(move || {
            if *checked_stop.borrow() {
                return Err("capture cancelled".into());
            }
            Ok(())
        });
    loop {
        if *stop.borrow() {
            return Err("capture cancelled".into());
        }
        let (cursor, carry) = positions::load(read_conn, binding)
            .await
            .map_err(|e| e.to_string())?
            .unwrap_or((serde_json::Value::Null, serde_json::Value::Null));
        let poll_started = Instant::now();
        let mut acquired = reader.acquire(cursor.clone()).await?;
        totals.poll_time += poll_started.elapsed();
        drivers::note_previous_through(&cursor, &mut acquired);
        let carry = material.carry(read_conn, binding, carry, &acquired).await?;
        if let Some(reason) = &acquired.observation.unavailable {
            return Err(format!("{binding}: {reason}"));
        }
        let had_material = acquired.material.is_some();
        check()?;
        let registered = material
            .reopen(read_conn, source.as_ref(), &mut acquired)
            .await?;
        crate::intake::Intake {
            host: Some(host),
            writer: handle,
            source: source.clone(),
            binding,
            run,
            material: material.clone(),
            check: check.clone(),
            priority: crate::writer::Priority::Bulk,
            atomic: false,
            registered,
        }
        .submit(acquired, carry, &mut totals)
        .await?;
        if !had_material {
            break;
        }
    }
    totals.wall = started.elapsed();
    Ok(totals)
}

impl Totals {
    pub(crate) fn poll(&mut self, length: Option<usize>) {
        self.polls += 1;
        if let Some(length) = length {
            self.materials += 1;
            self.material_bytes += length;
        }
    }

    pub(crate) fn parse(&mut self, elapsed: Duration) {
        self.parse_time += elapsed;
    }
    pub(crate) fn writer(&mut self, queue: Duration, service: Duration) {
        self.queue_time += queue;
        self.service_time += service;
    }
    pub(crate) fn batch(&mut self, readings: usize, elapsed: Duration) {
        self.batches += 1;
        self.accounting.readings += readings as u64;
        self.admit_time += elapsed;
    }

    /// The records a material's driver framed, and those its input no
    /// longer holds.
    pub(crate) fn framed(&mut self, framed: u64, removed: u64) {
        self.accounting.framed += framed;
        self.accounting.removed += removed;
    }

    /// What the parser made of each record of a batch.
    pub(crate) fn records(&mut self, batch: &sinex_model::Batch) {
        self.accounting.records(&batch.records);
    }

    /// What admission decided for a batch's readings.
    pub(crate) fn add_outcome(&mut self, out: &sinex_store::Outcome) {
        decided(&mut self.accounting, out);
        self.dirty_keys += out.dirty_keys;
    }

    pub(crate) fn finish(&mut self, started: Instant) {
        self.wall = started.elapsed();
    }

    pub(crate) fn pass_work(
        &mut self,
        work: &sinex_source_core::shared_pass::ArchivePassStats,
        captured: bool,
    ) {
        self.files_stored += u64::from(captured);
        self.pass_decoded_bytes += work.decoded_bytes;
        self.pass_record_bytes += work.record_bytes_stored;
    }
    pub(crate) fn to_json(&self) -> serde_json::Value {
        let per_s = |n: u64, d: Duration| n as f64 / d.as_secs_f64().max(1e-9);
        serde_json::json!({
            "polls": self.polls,
            "files_stored": self.files_stored,
            "pass_decoded_bytes": self.pass_decoded_bytes,
            "pass_record_bytes": self.pass_record_bytes,
            "materials": self.materials,
            "material_bytes": self.material_bytes,
            "batches": self.batches,
            "accounting": self.accounting,
            "dirty_keys": self.dirty_keys,
            "wall_s": self.wall.as_secs_f64(),
            "admission_s": self.admit_time.as_secs_f64(),
            "poll_s": self.poll_time.as_secs_f64(),
            "parse_s": self.parse_time.as_secs_f64(),
            "queue_s": self.queue_time.as_secs_f64(),
            "service_s": self.service_time.as_secs_f64(),
            "admission_readings_per_s": per_s(self.accounting.readings, self.admit_time),
            "readings_per_s": per_s(self.accounting.readings, self.wall),
            "mb_per_s": self.material_bytes as f64 / 1e6 / self.wall.as_secs_f64().max(1e-9),
        })
    }

    pub(crate) fn add(&mut self, o: Totals) {
        self.polls += o.polls;
        self.files_stored += o.files_stored;
        self.pass_decoded_bytes += o.pass_decoded_bytes;
        self.pass_record_bytes += o.pass_record_bytes;
        self.materials += o.materials;
        self.material_bytes += o.material_bytes;
        self.batches += o.batches;
        self.accounting.add(&o.accounting);
        self.dirty_keys += o.dirty_keys;
        self.admit_time += o.admit_time;
        self.poll_time += o.poll_time;
        self.parse_time += o.parse_time;
        self.queue_time += o.queue_time;
        self.service_time += o.service_time;
        self.wall += o.wall;
    }
}

#[cfg(test)]
mod tests;
