//! The cloneable handle that sends commands to the writer task and awaits each reply.
use crate::synthetic::{MaterialEvent, Receipt};
use crate::{
    AtuinStep, BrowserBegin, BrowserPending, BrowserStep, Error, PrivacyRule, Result, ZshStep,
};
use serde_json::Value;
use sinex_model::acquisition::{AtuinReadStart, PollSchedule};
use sinex_model::browser::BrowserReadStart;
use std::path::PathBuf;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

type Reply<T> = oneshot::Sender<Result<T>>;

pub enum ReplayCommand {
    Plan(sinex_model::replay::Selection, Vec<PrivacyRule>),
    Start(sinex_model::replay::Start),
    Cancel(sinex_model::replay::Operation),
}

pub(crate) enum Command {
    BrowserBegin(BrowserBegin, Reply<BrowserReadStart>),
    BrowserStep(BrowserStep, Reply<Value>),
    BrowserPending(Uuid, i64, Option<Uuid>, Reply<Vec<BrowserPending>>),
    BrowserKnown(String, Uuid, Vec<String>, Reply<Vec<bool>>),
    BrowserSnapshot(Uuid, Uuid, PathBuf, Reply<Value>),
    Exclude(
        sinex_model::exclusion::Action,
        sinex_model::exclusion::Request,
        Reply<Value>,
    ),
    ShellEnable(Reply<()>),
    SessionEnable(Reply<()>),
    ReplayProfile(Option<Vec<PrivacyRule>>, oneshot::Sender<()>),
    Replay(ReplayCommand, Reply<Value>),
    Admit(MaterialEvent, Reply<Receipt>),
    SourceSchedule(Uuid, PollSchedule, Reply<()>),
    AtuinReadStart(String, Reply<AtuinReadStart>),
    AtuinUnfinished(Uuid, i64, Option<Uuid>, Reply<Vec<(Uuid, String)>>),
    Atuin(AtuinStep, Reply<Value>),
    Zsh(ZshStep, Reply<Value>),
    Shutdown(oneshot::Sender<()>),
}

#[derive(Clone)]
pub struct Admission {
    pub(crate) sender: mpsc::Sender<Command>,
    pub(crate) observe_atuin_queue: bool,
}
impl Admission {
    /// Sends one command; a full queue waits for capacity. Success means the writer
    /// finished the command, so for writes the transaction committed.
    async fn call<T>(&self, command: impl FnOnce(Reply<T>) -> Command) -> Result<T> {
        let (tx, rx) = oneshot::channel();
        self.send(command(tx)).await?;
        rx.await.map_err(|_| Error::Stopped)?
    }
    pub(crate) async fn send(&self, command: Command) -> Result<()> {
        self.sender.send(command).await.map_err(|_| Error::Stopped)
    }

    pub async fn browser_begin(&self, begin: BrowserBegin) -> Result<BrowserReadStart> {
        self.call(|tx| Command::BrowserBegin(begin, tx)).await
    }
    pub async fn browser_step(&self, step: BrowserStep) -> Result<Value> {
        self.call(|tx| Command::BrowserStep(step, tx)).await
    }
    pub async fn browser_pending(
        &self,
        input: Uuid,
        snapshot_seq: i64,
        after: Option<Uuid>,
    ) -> Result<Vec<BrowserPending>> {
        self.call(|tx| Command::BrowserPending(input, snapshot_seq, after, tx))
            .await
    }
    pub async fn browser_known_downloads(
        &self,
        profile: String,
        generation: Uuid,
        keys: Vec<String>,
    ) -> Result<Vec<bool>> {
        self.call(|tx| Command::BrowserKnown(profile, generation, keys, tx))
            .await
    }
    pub async fn browser_snapshot(
        &self,
        input: Uuid,
        observation: Uuid,
        path: PathBuf,
    ) -> Result<Value> {
        self.call(|tx| Command::BrowserSnapshot(input, observation, path, tx))
            .await
    }
    pub async fn atuin_read_start(&self, locator: String) -> Result<AtuinReadStart> {
        self.call(|tx| Command::AtuinReadStart(locator, tx)).await
    }
    pub async fn atuin_unfinished_page(
        &self,
        input: Uuid,
        snapshot_seq: i64,
        after: Option<Uuid>,
    ) -> Result<Vec<(Uuid, String)>> {
        self.call(|tx| Command::AtuinUnfinished(input, snapshot_seq, after, tx))
            .await
    }
    pub async fn submit_atuin(&self, step: AtuinStep) -> Result<Value> {
        // Test hook only. Logs key hashes and row ids, never captured keys.
        let notice = self.observe_atuin_queue.then(|| serde_json::json!({
            "component":"sinex-store",
            "test_phase":"atuin-step-enqueued",
            "observation":step.observation,
            "sqlite_rowids":step.rows.iter().filter_map(|row| row["_rowid"].as_i64()).collect::<Vec<_>>(),
            "native_key_hashes":step.rows.iter().filter_map(|row| row["id"].as_str()).map(|key| blake3::hash(key.as_bytes()).to_hex().to_string()).collect::<Vec<_>>(),
            "source_eof":step.complete
        }));
        let (tx, rx) = oneshot::channel();
        self.send(Command::Atuin(step, tx)).await?;
        if let Some(notice) = notice {
            // Printed after the send succeeds and before the reply, so a test can see
            // the step queued while the writer is still busy with earlier ones.
            eprintln!("{notice}");
        }
        rx.await.map_err(|_| Error::Stopped)?
    }
    pub async fn schedule_source(&self, observation: Uuid, schedule: PollSchedule) -> Result<()> {
        schedule
            .validate()
            .map_err(|_| Error::Invalid("invalid acquisition schedule"))?;
        self.call(|tx| Command::SourceSchedule(observation, schedule, tx))
            .await
    }
    pub async fn submit_zsh(&self, step: ZshStep) -> Result<Value> {
        self.call(|tx| Command::Zsh(step, tx)).await
    }

    pub async fn replay(&self, command: ReplayCommand) -> Result<Value> {
        let cancelled = match &command {
            ReplayCommand::Cancel(p) => Some(p.operation_id),
            _ => None,
        };
        let (tx, rx) = oneshot::channel();
        self.send(Command::Replay(command, tx)).await?;
        if let Some(id) = cancelled {
            eprintln!(
                "{}",
                serde_json::json!({"component":"sinex-store","phase":"replay_cancel_enqueued","operation_id":id})
            );
        }
        rx.await.map_err(|_| Error::Stopped)?
    }
    pub async fn exclude(
        &self,
        action: sinex_model::exclusion::Action,
        request: sinex_model::exclusion::Request,
    ) -> Result<Value> {
        self.call(|tx| Command::Exclude(action, request, tx)).await
    }
    pub async fn set_replay_profile(&self, rules: Option<Vec<PrivacyRule>>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.send(Command::ReplayProfile(rules, tx)).await?;
        rx.await.map_err(|_| Error::Stopped)
    }
    pub async fn enable_shell(&self) -> Result<()> {
        self.call(Command::ShellEnable).await
    }
    pub async fn enable_sessions(&self) -> Result<()> {
        self.call(Command::SessionEnable).await
    }
    pub fn is_available(&self) -> bool {
        !self.sender.is_closed()
    }
    pub async fn submit_synthetic(&self, event: MaterialEvent) -> Result<Receipt> {
        self.call(|tx| Command::Admit(event, tx)).await
    }
}
