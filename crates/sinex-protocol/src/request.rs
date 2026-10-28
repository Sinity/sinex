//! Requests and replies: one call as the API and MCP carry it, built from a
//! command's arguments, and what it returns.
use crate::commands::{Access, COMMANDS, command};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// One call, as the API and MCP carry it. Built from a command's arguments
/// with [`Request::from_args`], so every surface makes the same request.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "method", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Status,
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
        path: String,
        #[serde(default)]
        parser: Option<String>,
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
        resume: Option<String>,
        #[serde(default)]
        all: Option<bool>,
    },
    Recompute {
        derivation: String,
        #[serde(default)]
        compare: Option<bool>,
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
    Hide {
        reference: String,
    },
    Unhide {
        reference: String,
    },
    Excise {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reference: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        material: Option<String>,
        #[serde(default, rename = "dry-run", skip_serializing_if = "Option::is_none")]
        dry_run: Option<bool>,
    },
    Same {
        a: String,
        b: String,
    },
    Distinct {
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
    #[serde(rename = "library-tag")]
    LibraryTag {
        item: String,
        tag: String,
    },
    #[serde(rename = "library-untag")]
    LibraryUntag {
        item: String,
        tag: String,
    },
    #[serde(rename = "library-set")]
    LibrarySet {
        item: String,
        field: String,
        #[serde(default)]
        value: Option<String>,
        #[serde(default)]
        empty: Option<bool>,
    },
    #[serde(rename = "library-collect")]
    LibraryCollect {
        collection: String,
        item: String,
    },
    #[serde(rename = "library-uncollect")]
    LibraryUncollect {
        collection: String,
        item: String,
    },
    #[serde(rename = "library-collection")]
    LibraryCollection {
        #[serde(default)]
        collection: Option<String>,
    },
    #[serde(rename = "library-rate")]
    LibraryRate {
        item: String,
        value: String,
        #[serde(default)]
        scale: Option<String>,
        #[serde(default)]
        of: Option<String>,
    },
    #[serde(rename = "library-compare")]
    LibraryCompare {
        winner: String,
        loser: String,
        #[serde(default)]
        scale: Option<String>,
    },
    #[serde(rename = "library-rank")]
    LibraryRank {
        #[serde(default)]
        scale: Option<String>,
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
    pub fn from_args(command: &str, args: Map<String, Value>) -> Result<Request, String> {
        let spec = self::command(command).ok_or_else(|| {
            let names: Vec<&str> = COMMANDS.iter().map(|c| c.name).collect();
            format!("no command `{command}`; commands: {}", names.join(", "))
        })?;
        for arg in spec.args {
            if arg.required && args.get(arg.name).is_none_or(Value::is_null) {
                return Err(format!("`{command}` needs `{}`: {}", arg.name, arg.help));
            }
        }
        let mut object = args;
        object.insert("method".into(), Value::String(command.into()));
        serde_json::from_value(Value::Object(object)).map_err(|e| format!("`{command}`: {e}"))
    }

    pub fn method(&self) -> &'static str {
        match self {
            Request::Status => "status",
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
            Request::Replay { .. } => "replay",
            Request::Recompute { .. } => "recompute",
            Request::Comparisons { .. } => "comparisons",
            Request::Crawl { .. } => "crawl",
            Request::Hide { .. } => "hide",
            Request::Unhide { .. } => "unhide",
            Request::Excise { .. } => "excise",
            Request::Same { .. } => "same",
            Request::Distinct { .. } => "distinct",
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
            Request::LibraryTag { .. } => "library-tag",
            Request::LibraryUntag { .. } => "library-untag",
            Request::LibrarySet { .. } => "library-set",
            Request::LibraryCollect { .. } => "library-collect",
            Request::LibraryUncollect { .. } => "library-uncollect",
            Request::LibraryCollection { .. } => "library-collection",
            Request::LibraryRate { .. } => "library-rate",
            Request::LibraryCompare { .. } => "library-compare",
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
        match self {
            Self::Stage { .. } | Self::Same { .. } | Self::Distinct { .. } => Access::Agent,
            _ => {
                command(self.method())
                    .expect("implemented request is in the command table")
                    .access
            }
        }
    }
}

/// What a call returns. A refusal is never an empty success.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum Reply {
    Ok {
        result: Value,
    },
    Refused {
        reason: String,
        /// The request asks for something Sinex does not build yet.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        not_built: bool,
    },
}
