//! Requests and replies: one call as the API and MCP carry it, built from a
//! command's arguments, and what it returns.
use crate::commands::{Access, COMMANDS, command};
use command_surface::{Failure, FailureKind};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// CLI and streaming spelling of the shared numeric-or-null wire budget.
pub fn deadline(value: &str) -> Result<command_surface::Deadline, Failure> {
    if value == "none" {
        Ok(command_surface::Deadline::None)
    } else {
        value
            .parse()
            .map(command_surface::Deadline::Milliseconds)
            .map_err(|_| {
                Failure::new(
                    FailureKind::BadRequest,
                    "deadline must be milliseconds or none",
                )
            })
    }
}

/// A command and its execution budget. The flat wire spelling stays the
/// command's strict tagged object, with shared `deadline` metadata beside it.
#[derive(Clone, Debug, PartialEq)]
pub struct Call {
    pub request: Request,
    pub deadline: command_surface::Deadline,
}
impl From<Request> for Call {
    fn from(request: Request) -> Self {
        Self {
            request,
            deadline: command_surface::Deadline::Default,
        }
    }
}
impl Call {
    pub fn from_args(command: &str, mut args: Map<String, Value>) -> Result<Self, Failure> {
        let deadline = args
            .remove("deadline")
            .map(serde_json::from_value)
            .transpose()
            .map_err(|error| Failure::new(FailureKind::BadRequest, format!("deadline: {error}")))?
            .unwrap_or_default();
        Ok(Self {
            request: Request::from_args(command, args)?,
            deadline,
        })
    }
}
impl Serialize for Call {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut value = serde_json::to_value(&self.request).map_err(serde::ser::Error::custom)?;
        if self.deadline != command_surface::Deadline::Default {
            value.as_object_mut().expect("request object").insert(
                "deadline".into(),
                serde_json::to_value(self.deadline).map_err(serde::ser::Error::custom)?,
            );
        }
        value.serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for Call {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let mut args = Map::<String, Value>::deserialize(deserializer)?;
        let method = args
            .remove("method")
            .ok_or_else(|| serde::de::Error::custom("missing method"))?;
        let method = method
            .as_str()
            .ok_or_else(|| serde::de::Error::custom("method must be a string"))?;
        Self::from_args(method, args).map_err(serde::de::Error::custom)
    }
}

/// One call, as the API and MCP carry it. Built from a command's arguments
/// with [`Request::from_args`], so every surface makes the same request.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "method", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Status,
    Backup {
        action: String,
        #[serde(default)]
        hold: Option<String>,
        #[serde(default)]
        output: Option<String>,
        #[serde(default)]
        page: Option<u64>,
        #[serde(default)]
        cursor: Option<String>,
    },
    #[serde(rename = "sources-discover")]
    SourcesDiscover,
    Signal {
        action: String,
        name: Option<String>,
        rule: Option<String>,
        page: Option<u64>,
        cursor: Option<String>,
    },
    Kinds {
        kind: Option<String>,
        page: Option<u64>,
        cursor: Option<String>,
    },
    Fields {
        kind: Option<String>,
        page: Option<u64>,
        cursor: Option<String>,
    },
    #[serde(rename = "forecast-issue")]
    ForecastIssue {
        target: String,
        probability: String,
        criteria: String,
        horizon: String,
        cutoff: String,
        #[serde(default)]
        information: Vec<String>,
    },
    #[serde(rename = "forecast-resolve")]
    ForecastResolve {
        forecast: String,
        outcome: String,
        #[serde(default)]
        at: Option<String>,
        evidence: Vec<String>,
    },
    #[serde(rename = "forecast-report")]
    ForecastReport,

    #[serde(rename = "query-save")]
    QuerySave {
        name: String,
        query: String,
        #[serde(default)]
        parameter: Vec<String>,
    },
    #[serde(rename = "query-list")]
    QueryList,
    #[serde(rename = "query-complete")]
    QueryComplete {
        query: String,
    },
    Context {
        #[serde(default)]
        since: Option<String>,
        #[serde(default)]
        until: Option<String>,
        #[serde(default)]
        page: Option<u64>,
        #[serde(default)]
        cursor: Option<String>,
    },
    Timeline {
        #[serde(default)]
        query: Option<String>,
        #[serde(default)]
        page: Option<u64>,
        #[serde(default)]
        cursor: Option<String>,
    },
    Query {
        query: String,
        #[serde(default)]
        page: Option<u64>,
        #[serde(default)]
        cursor: Option<String>,
    },
    Search {
        words: String,
        #[serde(default)]
        page: Option<u64>,
        #[serde(default)]
        cursor: Option<String>,
    },
    Recall {
        #[serde(default)]
        around: Option<String>,
        #[serde(default)]
        cwd: Option<String>,
        #[serde(default)]
        page: Option<u64>,
        #[serde(default)]
        cursor: Option<String>,
    },
    Sql {
        sql: String,
        #[serde(default)]
        page: Option<u64>,
        #[serde(default)]
        cursor: Option<String>,
    },
    Show {
        reference: String,
    },
    Trace {
        reference: String,
        #[serde(default)]
        page: Option<u64>,
        #[serde(default)]
        cursor: Option<String>,
    },
    Follow {
        query: String,
    },
    Stage {
        path: Vec<String>,
        #[serde(default)]
        parser: Vec<String>,
        #[serde(default)]
        account: Option<String>,
        #[serde(default, rename = "export-date")]
        export_date: Option<String>,
        #[serde(default)]
        follow: Option<bool>,
        #[serde(default, rename = "batch-events")]
        batch_events: Option<u64>,
    },
    #[serde(rename = "stage-list")]
    StageList {
        #[serde(default)]
        active: Option<bool>,
    },
    #[serde(rename = "stage-show")]
    StageShow {
        operation: String,
    },
    Replay {
        #[serde(default)]
        sealed: Option<bool>,
        #[serde(default)]
        material: Vec<String>,
        #[serde(default)]
        binding: Option<String>,
        #[serde(default, rename = "event-type")]
        event_type: Vec<String>,
        #[serde(default)]
        from: Option<String>,
        #[serde(default)]
        until: Option<String>,
        #[serde(default)]
        compare: Option<bool>,
        #[serde(default)]
        all: Option<bool>,
        #[serde(default, rename = "batch-events")]
        batch_events: Option<u64>,
    },
    Recompute {
        derivation: String,
        #[serde(default)]
        compare: Option<bool>,
        #[serde(default)]
        switch: Option<i64>,
        #[serde(default)]
        from: Option<String>,
        #[serde(default)]
        until: Option<String>,
        #[serde(default)]
        examples: Option<u64>,
    },
    Comparisons {
        #[serde(default)]
        derivation: Option<String>,
        #[serde(default)]
        limit: Option<u64>,
    },
    Crawl {
        #[serde(default)]
        allow: Vec<String>,
        #[serde(default)]
        url: Vec<String>,
        #[serde(default)]
        limit: Option<u64>,
        #[serde(default)]
        pause: Option<u64>,
    },
    Archive {
        #[serde(default)]
        target: Option<String>,
        #[serde(default)]
        material: Vec<String>,
        #[serde(default)]
        run: Option<String>,
        #[serde(default)]
        source: Option<String>,
        #[serde(default)]
        after: Option<String>,
        #[serde(default)]
        before: Option<String>,
        #[serde(default)]
        compare: Option<bool>,
    },
    Restore {
        #[serde(default)]
        target: Option<String>,
        #[serde(default)]
        material: Vec<String>,
        #[serde(default)]
        run: Option<String>,
        #[serde(default)]
        source: Option<String>,
        #[serde(default)]
        after: Option<String>,
        #[serde(default)]
        before: Option<String>,
        #[serde(default)]
        operation: Option<String>,
        #[serde(default)]
        compare: Option<bool>,
    },
    Tombstone {
        #[serde(default)]
        target: Option<String>,
        #[serde(default)]
        material: Vec<String>,
        #[serde(default)]
        run: Option<String>,
        #[serde(default)]
        source: Option<String>,
        #[serde(default)]
        after: Option<String>,
        #[serde(default)]
        before: Option<String>,
        #[serde(default)]
        operation: Option<String>,
        #[serde(default)]
        compare: Option<bool>,
    },
    Undo {
        operation: String,
        #[serde(default)]
        compare: Option<bool>,
    },
    Resume {
        operation: String,
    },
    Same {
        a: String,
        b: String,
    },
    Distinct {
        a: String,
        b: String,
    },
    #[serde(rename = "withdraw-decision")]
    WithdrawDecision {
        a: String,
        b: String,
    },
    Mark {
        #[serde(default)]
        text: Option<String>,
        #[serde(default)]
        at: Option<String>,
        #[serde(default)]
        cwd: Option<String>,
        #[serde(default)]
        command: Option<String>,
        #[serde(default)]
        app: Option<String>,
        #[serde(default)]
        title: Option<String>,
        #[serde(default)]
        command_record: Option<String>,
        #[serde(default)]
        focus_record: Option<String>,
    },
    #[serde(rename = "privacy-rules")]
    PrivacyRules {
        #[serde(default)]
        history: Option<bool>,
    },
    #[serde(rename = "privacy-rule")]
    PrivacyRule {
        name: String,
        source: String,
        #[serde(default, rename = "type")]
        event_type: Option<String>,
        #[serde(default)]
        field: Option<String>,
        #[serde(default)]
        matcher: Option<String>,
        #[serde(default)]
        pattern: Option<String>,
        #[serde(default)]
        class: Option<String>,
        #[serde(default)]
        action: Option<String>,
    },
    #[serde(rename = "privacy-end")]
    PrivacyEnd {
        name: String,
    },
    #[serde(rename = "privacy-class")]
    PrivacyClass {
        source: String,
        class: String,
    },
    #[serde(rename = "privacy-verify")]
    PrivacyVerify,
    Unlock {
        identity: String,
        #[serde(default)]
        minutes: Option<u64>,
    },
    Lock,
    #[serde(rename = "analysis-catalog")]
    AnalysisCatalog,
    #[serde(rename = "analysis-run")]
    AnalysisRun {
        recipe: String,
        from: String,
        to: String,
        #[serde(default)]
        partial: Option<String>,
    },
    #[serde(rename = "analysis-panel")]
    AnalysisPanel {
        from: String,
        to: String,
        #[serde(default)]
        metric: Option<Vec<String>>,
        #[serde(default)]
        by: Option<String>,
    },
    Gaps {
        from: String,
        to: String,
        #[serde(default)]
        source: Option<Vec<String>>,
    },
    #[serde(rename = "analysis-why")]
    AnalysisWhy {
        metric: String,
        day: String,
    },
    Brief {
        #[serde(default)]
        day: Option<String>,
        #[serde(default)]
        budget: Option<u64>,
    },
    #[serde(rename = "library-add")]
    LibraryAdd {
        path: String,
        #[serde(default)]
        reference: Option<bool>,
    },
    #[serde(rename = "library-item")]
    LibraryItem {
        item: String,
    },
    #[serde(rename = "library-curate")]
    LibraryCurate {
        /// The caller's ID for this request, reused on retry so it is
        /// written once; a new one is chosen when absent.
        #[serde(default)]
        request: Option<String>,
        writes: Vec<sinex_model::event_types::library::Act>,
    },
    #[serde(rename = "library-collection")]
    LibraryCollection {
        #[serde(default)]
        collection: Option<String>,
    },
    #[serde(rename = "library-rank")]
    LibraryRank {
        scale: String,
        #[serde(default)]
        judges: Option<String>,
        #[serde(default)]
        collection: Option<String>,
        #[serde(default)]
        k: Option<u64>,
        #[serde(default)]
        rule: Option<String>,
    },
    #[serde(rename = "library-open")]
    LibraryOpen {
        item: String,
        #[serde(default)]
        rendition: Option<String>,
    },
    #[serde(rename = "library-viewed")]
    LibraryViewed {
        item: String,
        #[serde(default)]
        seconds: Option<String>,
        #[serde(default)]
        player: Option<String>,
    },
    #[serde(rename = "library-preview")]
    LibraryPreview {
        item: String,
    },
    #[serde(rename = "library-import")]
    LibraryImport {
        path: String,
    },
}

impl Request {
    /// The request for `command` with `args` as a JSON object, checked
    /// against the command table.
    pub fn from_args(command: &str, args: Map<String, Value>) -> Result<Request, Failure> {
        let spec = self::command(command).ok_or_else(|| {
            let names: Vec<String> = COMMANDS.iter().map(|c| c.name.to_owned()).collect();
            Failure::new(FailureKind::BadRequest, format!("no command `{command}`"))
                .with_candidates(names)
        })?;
        for name in args.keys() {
            if !spec.args.iter().any(|arg| arg.name == name) {
                return Err(Failure::new(
                    FailureKind::BadRequest,
                    format!("`{command}` has no argument `{name}`"),
                ));
            }
        }
        for arg in spec.args {
            if arg.required && args.get(arg.name).is_none_or(Value::is_null) {
                return Err(Failure::new(
                    FailureKind::BadRequest,
                    format!("`{command}` needs `{}`: {}", arg.name, arg.help),
                ));
            }
        }
        let mut object = args;
        object.insert("method".into(), Value::String(command.into()));
        serde_json::from_value(Value::Object(object))
            .map_err(|e| Failure::new(FailureKind::BadRequest, format!("`{command}`: {e}")))
    }

    pub fn method(&self) -> &'static str {
        match self {
            Request::Status => "status",
            Request::Backup { .. } => "backup",
            Request::SourcesDiscover => "sources-discover",
            Request::Signal { .. } => "signal",
            Request::Kinds { .. } => "kinds",
            Request::Fields { .. } => "fields",
            Request::ForecastIssue { .. } => "forecast-issue",
            Request::ForecastResolve { .. } => "forecast-resolve",
            Request::ForecastReport => "forecast-report",
            Request::Query { .. } => "query",
            Request::QuerySave { .. } => "query-save",
            Request::QueryList => "query-list",
            Request::QueryComplete { .. } => "query-complete",
            Request::Context { .. } => "context",
            Request::Timeline { .. } => "timeline",
            Request::Search { .. } => "search",
            Request::Recall { .. } => "recall",
            Request::Sql { .. } => "sql",
            Request::Show { .. } => "show",
            Request::Trace { .. } => "trace",
            Request::Follow { .. } => "follow",
            Request::Stage { .. } => "stage",
            Request::StageList { .. } => "stage-list",
            Request::StageShow { .. } => "stage-show",
            Request::Replay { .. } => "replay",
            Request::Recompute { .. } => "recompute",
            Request::Comparisons { .. } => "comparisons",
            Request::Crawl { .. } => "crawl",
            Request::Archive { .. } => "archive",
            Request::Restore { .. } => "restore",
            Request::Tombstone { .. } => "tombstone",
            Request::Undo { .. } => "undo",
            Request::Resume { .. } => "resume",
            Request::Same { .. } => "same",
            Request::Distinct { .. } => "distinct",
            Request::WithdrawDecision { .. } => "withdraw-decision",
            Request::Mark { .. } => "mark",
            Request::PrivacyRules { .. } => "privacy-rules",
            Request::PrivacyRule { .. } => "privacy-rule",
            Request::PrivacyEnd { .. } => "privacy-end",
            Request::PrivacyClass { .. } => "privacy-class",
            Request::PrivacyVerify => "privacy-verify",
            Request::Unlock { .. } => "unlock",
            Request::Lock => "lock",
            Request::AnalysisCatalog => "analysis-catalog",
            Request::AnalysisRun { .. } => "analysis-run",
            Request::AnalysisPanel { .. } => "analysis-panel",
            Request::Gaps { .. } => "gaps",
            Request::AnalysisWhy { .. } => "analysis-why",
            Request::Brief { .. } => "brief",
            Request::LibraryAdd { .. } => "library-add",
            Request::LibraryItem { .. } => "library-item",
            Request::LibraryCurate { .. } => "library-curate",
            Request::LibraryCollection { .. } => "library-collection",
            Request::LibraryRank { .. } => "library-rank",
            Request::LibraryOpen { .. } => "library-open",
            Request::LibraryViewed { .. } => "library-viewed",
            Request::LibraryPreview { .. } => "library-preview",
            Request::LibraryImport { .. } => "library-import",
        }
    }

    /// Advertised access comes from the table. Wire requests outside the table
    /// keep explicit access so decoding one cannot bypass authorization or
    /// panic.
    pub fn access(&self) -> Access {
        command(self.method())
            .expect("implemented request is in the command table")
            .access
    }
}

/// What a call returns. A failure is never an empty success.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum Reply {
    Ok {
        result: Value,
    },
    /// A multi-batch operation moved some batches and then failed. Never
    /// reported as ok: the failure (kind `stopped_early`) says why it
    /// stopped, the result names what moved, and `resume` continues it once
    /// the cause is cleared.
    StoppedEarly {
        failure: Failure,
        result: Value,
    },
    Failed {
        failure: Failure,
    },
}

impl Reply {
    pub fn failed(failure: impl Into<Failure>) -> Self {
        Self::Failed {
            failure: failure.into(),
        }
    }

    /// The failure this reply carries, if it is not ok.
    pub fn failure(&self) -> Option<&Failure> {
        match self {
            Self::Ok { .. } => None,
            Self::StoppedEarly { failure, .. } | Self::Failed { failure } => Some(failure),
        }
    }
}

/// An operation that stopped after moving some batches: the cause's message,
/// code and retryability under the kind `stopped_early`.
pub fn stopped_early(cause: Failure) -> Failure {
    let mut failure = Failure::new(FailureKind::StoppedEarly, cause.message);
    failure.code = cause.code.or_else(|| Some(cause.kind.name().to_owned()));
    failure.hint = cause.hint;
    failure.retryable = cause.retryable;
    failure
}
