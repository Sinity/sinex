use crate::error::SinexError;
use crate::domain::{EventSource, EventType, HostName};
use crate::query::{EventQuery, Pagination, PayloadFilter, SortDirection, TimeRange};
use crate::temporal::Timestamp;
use crate::views::{CaveatView, SinexObjectKind, SinexObjectRef};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use winnow::Parser;
use winnow::ascii::multispace0;
use winnow::combinator::{alt, delimited, opt};
use winnow::prelude::ModalResult;
use winnow::token::{one_of, take_until, take_while};

/// Stable query unit names shared by CLI, TUI, MCP, and gateway read surfaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum QueryUnitId {
    Events,
    SourceDrivers,
    SourceMaterials,
    Debt,
    Operations,
    RuntimeHealth,
}

impl QueryUnitId {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Events => "events",
            Self::SourceDrivers => "source-drivers",
            Self::SourceMaterials => "source-materials",
            Self::Debt => "debt",
            Self::Operations => "operations",
            Self::RuntimeHealth => "runtime-health",
        }
    }
}

impl std::fmt::Display for QueryUnitId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for QueryUnitId {
    type Err = SinexError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            "events" => Ok(Self::Events),
            "source-drivers" => Ok(Self::SourceDrivers),
            "source-materials" => Ok(Self::SourceMaterials),
            "debt" => Ok(Self::Debt),
            "operations" => Ok(Self::Operations),
            "runtime-health" => Ok(Self::RuntimeHealth),
            other => Err(SinexError::parse(format!(
                "unknown query unit `{other}`; supported units: {}",
                query_unit_descriptors()
                    .iter()
                    .map(|descriptor| descriptor.unit.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum QueryFieldType {
    Text,
    Integer,
    Boolean,
    Timestamp,
    Duration,
    Enum,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum QueryOperator {
    Eq,
    NotEq,
    Contains,
    StartsWith,
    GreaterThan,
    GreaterThanOrEq,
    LessThan,
    LessThanOrEq,
    Exists,
}

impl QueryOperator {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::NotEq => "!=",
            Self::Contains => "contains",
            Self::StartsWith => "starts_with",
            Self::GreaterThan => ">",
            Self::GreaterThanOrEq => ">=",
            Self::LessThan => "<",
            Self::LessThanOrEq => "<=",
            Self::Exists => "exists",
        }
    }
}

impl std::str::FromStr for QueryOperator {
    type Err = SinexError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            "=" | "==" => Ok(Self::Eq),
            "!=" => Ok(Self::NotEq),
            "contains" => Ok(Self::Contains),
            "starts_with" => Ok(Self::StartsWith),
            ">" => Ok(Self::GreaterThan),
            ">=" => Ok(Self::GreaterThanOrEq),
            "<" => Ok(Self::LessThan),
            "<=" => Ok(Self::LessThanOrEq),
            "exists" => Ok(Self::Exists),
            other => Err(SinexError::parse(format!(
                "unknown query operator `{other}`"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
pub struct QueryFieldDescriptor {
    pub name: &'static str,
    pub field_type: QueryFieldType,
    pub operators: &'static [QueryOperator],
    pub enum_values: &'static [&'static str],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
pub struct QuerySortDescriptor {
    pub key: &'static str,
    pub default_descending: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, JsonSchema)]
pub struct QueryUnitDescriptor {
    pub unit: QueryUnitId,
    pub object_kind: SinexObjectKind,
    pub default_limit: i64,
    pub max_limit: i64,
    pub supports_aggregation: bool,
    pub fields: &'static [QueryFieldDescriptor],
    pub sort_keys: &'static [QuerySortDescriptor],
    pub backing_rpc_methods: &'static [&'static str],
    pub disclosure_context: &'static str,
}

impl QueryUnitDescriptor {
    pub fn field(&self, name: &str) -> Result<&QueryFieldDescriptor, SinexError> {
        self.fields
            .iter()
            .find(|field| field.name == name)
            .ok_or_else(|| {
                SinexError::validation(format!(
                    "query unit `{}` does not support field `{name}`; supported fields: {}",
                    self.unit,
                    self.fields
                        .iter()
                        .map(|field| field.name)
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
            })
    }

    pub fn validate_operator(
        &self,
        field: &QueryFieldDescriptor,
        operator: QueryOperator,
    ) -> Result<(), SinexError> {
        if field.operators.contains(&operator) {
            return Ok(());
        }

        Err(SinexError::validation(format!(
            "query unit `{}` field `{}` does not support operator `{}`; supported operators: {}",
            self.unit,
            field.name,
            operator.as_str(),
            field
                .operators
                .iter()
                .map(|op| op.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )))
    }

    #[must_use]
    pub fn pagination(&self, limit: Option<i64>, offset: Option<i64>) -> Pagination {
        Pagination::with_bounds(limit, offset, self.default_limit, self.max_limit)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct QueryPagination {
    pub limit: i64,
    pub offset: i64,
}

impl QueryPagination {
    #[must_use]
    pub fn from_pagination(pagination: Pagination) -> Self {
        Self {
            limit: pagination.limit(),
            offset: pagination.offset(),
        }
    }

    #[must_use]
    pub fn as_pagination(&self) -> Pagination {
        Pagination::new(Some(self.limit), Some(self.offset))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum QueryValue {
    String(String),
    Integer(i64),
    Boolean(bool),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SinexQueryPredicate {
    Compare {
        field: String,
        operator: QueryOperator,
        value: QueryValue,
    },
    Has {
        field: String,
    },
    And {
        predicates: Vec<SinexQueryPredicate>,
    },
    Or {
        predicates: Vec<SinexQueryPredicate>,
    },
    Not {
        predicate: Box<SinexQueryPredicate>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SinexQuerySort {
    pub key: String,
    pub descending: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SinexQuery {
    pub unit: QueryUnitId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub predicate: Option<SinexQueryPredicate>,
    #[serde(default)]
    pub sort: Vec<SinexQuerySort>,
    pub pagination: QueryPagination,
}

pub const SINEX_QUERY_RESULT_LIST_SCHEMA_VERSION: &str = "sinex.query-result-list/v1";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SinexQueryResultRow {
    pub unit: QueryUnitId,
    pub object_kind: SinexObjectKind,
    #[serde(rename = "ref", skip_serializing_if = "Option::is_none")]
    pub ref_: Option<SinexObjectRef>,
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub fields: BTreeMap<String, Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caveats: Vec<CaveatView>,
    pub payload: Value,
}

impl SinexQueryResultRow {
    #[must_use]
    pub fn new(
        unit: QueryUnitId,
        object_kind: SinexObjectKind,
        title: impl Into<String>,
        payload: Value,
    ) -> Self {
        Self {
            unit,
            object_kind,
            ref_: None,
            title: title.into(),
            summary: None,
            fields: BTreeMap::new(),
            caveats: Vec::new(),
            payload,
        }
    }

    #[must_use]
    pub fn with_ref(mut self, ref_: SinexObjectRef) -> Self {
        self.ref_ = Some(ref_);
        self
    }

    #[must_use]
    pub fn with_summary(mut self, summary: impl Into<String>) -> Self {
        self.summary = Some(summary.into());
        self
    }

    #[must_use]
    pub fn with_field(mut self, key: impl Into<String>, value: impl Serialize) -> Self {
        let value = serde_json::to_value(value).unwrap_or(Value::Null);
        self.fields.insert(key.into(), value);
        self
    }

    #[must_use]
    pub fn with_caveats(mut self, caveats: Vec<CaveatView>) -> Self {
        self.caveats = caveats;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SinexQueryResultListView {
    pub schema_version: String,
    pub query: SinexQuery,
    pub count: usize,
    pub rows: Vec<SinexQueryResultRow>,
}

impl SinexQueryResultListView {
    #[must_use]
    pub fn new(query: SinexQuery, rows: Vec<SinexQueryResultRow>) -> Self {
        let count = rows.len();
        Self {
            schema_version: SINEX_QUERY_RESULT_LIST_SCHEMA_VERSION.to_string(),
            query,
            count,
            rows,
        }
    }
}

impl SinexQuery {
    pub fn new(unit: QueryUnitId, limit: Option<i64>, offset: Option<i64>) -> Self {
        let descriptor = query_unit_descriptor(unit);
        Self {
            unit,
            predicate: None,
            sort: Vec::new(),
            pagination: QueryPagination::from_pagination(descriptor.pagination(limit, offset)),
        }
    }

    pub fn validate(&self) -> Result<(), SinexError> {
        let descriptor = query_unit_descriptor(self.unit);
        if let Some(predicate) = &self.predicate {
            validate_predicate(descriptor, predicate)?;
        }
        for sort in &self.sort {
            if !descriptor.sort_keys.iter().any(|key| key.key == sort.key) {
                return Err(SinexError::validation(format!(
                    "query unit `{}` does not support sort key `{}`; supported sort keys: {}",
                    self.unit,
                    sort.key,
                    descriptor
                        .sort_keys
                        .iter()
                        .map(|key| key.key)
                        .collect::<Vec<_>>()
                        .join(", ")
                )));
            }
        }
        Ok(())
    }
}

/// Lower a Sinex-native `events` query expression into the composable event
/// query request used by the gateway and database layer.
pub fn event_query_from_sinex_query(query: &SinexQuery) -> Result<EventQuery, SinexError> {
    if query.unit != QueryUnitId::Events {
        return Err(SinexError::validation(format!(
            "cannot lower query unit `{}` to EventQuery",
            query.unit
        )));
    }

    query.validate()?;
    let mut request = EventQuery {
        limit: query.pagination.limit + query.pagination.offset,
        direction: SortDirection::Desc,
        ..Default::default()
    };

    if let Some(predicate) = &query.predicate {
        apply_event_query_predicate(predicate, &mut request)?;
    }

    Ok(request)
}

fn apply_event_query_predicate(
    predicate: &SinexQueryPredicate,
    request: &mut EventQuery,
) -> Result<(), SinexError> {
    match predicate {
        SinexQueryPredicate::Compare {
            field,
            operator,
            value,
        } => lower_event_compare(field, *operator, value, request),
        SinexQueryPredicate::And { predicates } => {
            for child in predicates {
                apply_event_query_predicate(child, request)?;
            }
            Ok(())
        }
        other => Err(SinexError::validation(format!(
            "events query predicate `{other:?}` cannot lower to EventQuery; only comparison predicates joined by `and` are supported"
        ))),
    }
}

fn lower_event_compare(
    field: &str,
    operator: QueryOperator,
    value: &QueryValue,
    request: &mut EventQuery,
) -> Result<(), SinexError> {
    match field {
        "source" if operator == QueryOperator::Eq => {
            request.sources.push(EventSource::new(query_value_string(value)?)?);
            Ok(())
        }
        "event_type" if operator == QueryOperator::Eq => {
            request
                .event_types
                .push(EventType::new(query_value_string(value)?)?);
            Ok(())
        }
        "host" if operator == QueryOperator::Eq => {
            request.hosts.push(HostName::new(query_value_string(value)?)?);
            Ok(())
        }
        "scope_key" if operator == QueryOperator::Eq => {
            request.scope_key = Some(query_value_string(value)?.to_string());
            Ok(())
        }
        "equivalence_key" if operator == QueryOperator::Eq => {
            request.equivalence_key = Some(query_value_string(value)?.to_string());
            Ok(())
        }
        "text" if matches!(operator, QueryOperator::Eq | QueryOperator::Contains) => {
            merge_payload_filter(
                &mut request.payload,
                PayloadFilter::TextSearch {
                    text: query_value_string(value)?.to_string(),
                },
            );
            Ok(())
        }
        "ts_orig"
            if matches!(
                operator,
                QueryOperator::GreaterThan
                    | QueryOperator::GreaterThanOrEq
                    | QueryOperator::LessThan
                    | QueryOperator::LessThanOrEq
            ) =>
        {
            apply_time_bound(request, operator, parse_query_timestamp(value)?)?;
            Ok(())
        }
        "has_lineage" if operator == QueryOperator::Eq => {
            request.has_lineage = Some(query_value_bool(value)?);
            Ok(())
        }
        other => Err(SinexError::validation(format!(
            "events query field `{other}` with operator `{}` is descriptor-valid but cannot lower to EventQuery",
            operator.as_str()
        ))),
    }
}

fn merge_payload_filter(slot: &mut Option<PayloadFilter>, filter: PayloadFilter) {
    match slot.take() {
        None => *slot = Some(filter),
        Some(existing) => {
            *slot = Some(PayloadFilter::And {
                filters: vec![existing, filter],
            });
        }
    }
}

fn apply_time_bound(
    request: &mut EventQuery,
    operator: QueryOperator,
    timestamp: Timestamp,
) -> Result<(), SinexError> {
    let (start, end) = request
        .time_range
        .map(|range| (range.start(), range.end()))
        .unwrap_or((None, None));
    let (start, end) = match operator {
        QueryOperator::GreaterThan | QueryOperator::GreaterThanOrEq => (Some(timestamp), end),
        QueryOperator::LessThan | QueryOperator::LessThanOrEq => (start, Some(timestamp)),
        _ => unreachable!("operator prechecked by caller"),
    };
    request.time_range = Some(TimeRange::new(start, end)?);
    Ok(())
}

fn query_value_string(value: &QueryValue) -> Result<&str, SinexError> {
    match value {
        QueryValue::String(value) => Ok(value),
        other => Err(SinexError::validation(format!(
            "expected string query value, got {other:?}"
        ))),
    }
}

fn query_value_bool(value: &QueryValue) -> Result<bool, SinexError> {
    match value {
        QueryValue::Boolean(value) => Ok(*value),
        other => Err(SinexError::validation(format!(
            "expected boolean query value, got {other:?}"
        ))),
    }
}

fn parse_query_timestamp(value: &QueryValue) -> Result<Timestamp, SinexError> {
    let value = query_value_string(value)?;
    Timestamp::parse_rfc3339(value).map_err(|error| {
        SinexError::parse(format!(
            "event query timestamp `{value}` must be RFC3339: {error}"
        ))
    })
}

fn validate_predicate(
    descriptor: &QueryUnitDescriptor,
    predicate: &SinexQueryPredicate,
) -> Result<(), SinexError> {
    match predicate {
        SinexQueryPredicate::Compare {
            field,
            operator,
            value,
        } => {
            let field_descriptor = descriptor.field(field)?;
            descriptor.validate_operator(field_descriptor, *operator)?;
            validate_value(field_descriptor, value)
        }
        SinexQueryPredicate::Has { field } => {
            let field_descriptor = descriptor.field(field)?;
            descriptor.validate_operator(field_descriptor, QueryOperator::Exists)
        }
        SinexQueryPredicate::And { predicates } | SinexQueryPredicate::Or { predicates } => {
            if predicates.is_empty() {
                return Err(SinexError::validation(
                    "compound query predicate must contain at least one child",
                ));
            }
            for child in predicates {
                validate_predicate(descriptor, child)?;
            }
            Ok(())
        }
        SinexQueryPredicate::Not { predicate } => validate_predicate(descriptor, predicate),
    }
}

fn validate_value(field: &QueryFieldDescriptor, value: &QueryValue) -> Result<(), SinexError> {
    match (field.field_type, value) {
        (QueryFieldType::Integer, QueryValue::Integer(_))
        | (QueryFieldType::Boolean, QueryValue::Boolean(_))
        | (
            QueryFieldType::Text | QueryFieldType::Timestamp | QueryFieldType::Duration,
            QueryValue::String(_),
        ) => Ok(()),
        (QueryFieldType::Enum, QueryValue::String(value)) => {
            if field.enum_values.is_empty() || field.enum_values.contains(&value.as_str()) {
                Ok(())
            } else {
                Err(SinexError::validation(format!(
                    "field `{}` does not allow enum value `{value}`; supported values: {}",
                    field.name,
                    field.enum_values.join(", ")
                )))
            }
        }
        _ => Err(SinexError::validation(format!(
            "field `{}` expects {:?}, got {value:?}",
            field.name, field.field_type
        ))),
    }
}

const EXACT: &[QueryOperator] = &[QueryOperator::Eq];
const EQ: &[QueryOperator] = &[QueryOperator::Eq, QueryOperator::NotEq];
const TEXT: &[QueryOperator] = &[
    QueryOperator::Eq,
    QueryOperator::NotEq,
    QueryOperator::Contains,
    QueryOperator::StartsWith,
];
const ORDERED: &[QueryOperator] = &[
    QueryOperator::Eq,
    QueryOperator::NotEq,
    QueryOperator::GreaterThan,
    QueryOperator::GreaterThanOrEq,
    QueryOperator::LessThan,
    QueryOperator::LessThanOrEq,
];
const RANGE: &[QueryOperator] = &[
    QueryOperator::GreaterThan,
    QueryOperator::GreaterThanOrEq,
    QueryOperator::LessThan,
    QueryOperator::LessThanOrEq,
];

const EVENT_FIELDS: &[QueryFieldDescriptor] = &[
    QueryFieldDescriptor {
        name: "source",
        field_type: QueryFieldType::Text,
        operators: EXACT,
        enum_values: &[],
    },
    QueryFieldDescriptor {
        name: "event_type",
        field_type: QueryFieldType::Text,
        operators: EXACT,
        enum_values: &[],
    },
    QueryFieldDescriptor {
        name: "host",
        field_type: QueryFieldType::Text,
        operators: EXACT,
        enum_values: &[],
    },
    QueryFieldDescriptor {
        name: "scope_key",
        field_type: QueryFieldType::Text,
        operators: EXACT,
        enum_values: &[],
    },
    QueryFieldDescriptor {
        name: "equivalence_key",
        field_type: QueryFieldType::Text,
        operators: EXACT,
        enum_values: &[],
    },
    QueryFieldDescriptor {
        name: "text",
        field_type: QueryFieldType::Text,
        operators: &[QueryOperator::Eq, QueryOperator::Contains],
        enum_values: &[],
    },
    QueryFieldDescriptor {
        name: "ts_orig",
        field_type: QueryFieldType::Timestamp,
        operators: RANGE,
        enum_values: &[],
    },
    QueryFieldDescriptor {
        name: "has_lineage",
        field_type: QueryFieldType::Boolean,
        operators: EXACT,
        enum_values: &[],
    },
];
const SOURCE_DRIVER_FIELDS: &[QueryFieldDescriptor] = &[
    QueryFieldDescriptor {
        name: "source_id",
        field_type: QueryFieldType::Text,
        operators: TEXT,
        enum_values: &[],
    },
    QueryFieldDescriptor {
        name: "family",
        field_type: QueryFieldType::Text,
        operators: TEXT,
        enum_values: &[],
    },
    QueryFieldDescriptor {
        name: "readiness",
        field_type: QueryFieldType::Enum,
        operators: EQ,
        enum_values: &["ready", "degraded", "blocked", "missing"],
    },
    QueryFieldDescriptor {
        name: "enabled",
        field_type: QueryFieldType::Boolean,
        operators: EQ,
        enum_values: &[],
    },
];
const MATERIAL_FIELDS: &[QueryFieldDescriptor] = &[
    QueryFieldDescriptor {
        name: "material_id",
        field_type: QueryFieldType::Text,
        operators: EQ,
        enum_values: &[],
    },
    QueryFieldDescriptor {
        name: "source_identifier",
        field_type: QueryFieldType::Text,
        operators: TEXT,
        enum_values: &[],
    },
    QueryFieldDescriptor {
        name: "material_kind",
        field_type: QueryFieldType::Text,
        operators: TEXT,
        enum_values: &[],
    },
    QueryFieldDescriptor {
        name: "status",
        field_type: QueryFieldType::Text,
        operators: EQ,
        enum_values: &[],
    },
];
const DEBT_FIELDS: &[QueryFieldDescriptor] = &[
    QueryFieldDescriptor {
        name: "kind",
        field_type: QueryFieldType::Enum,
        operators: EQ,
        enum_values: &["capture", "admission", "projection"],
    },
    QueryFieldDescriptor {
        name: "severity",
        field_type: QueryFieldType::Enum,
        operators: EQ,
        enum_values: &["info", "warning", "error", "critical"],
    },
    QueryFieldDescriptor {
        name: "source",
        field_type: QueryFieldType::Text,
        operators: TEXT,
        enum_values: &[],
    },
    QueryFieldDescriptor {
        name: "age",
        field_type: QueryFieldType::Duration,
        operators: ORDERED,
        enum_values: &[],
    },
];
const OPERATION_FIELDS: &[QueryFieldDescriptor] = &[
    QueryFieldDescriptor {
        name: "operation_id",
        field_type: QueryFieldType::Text,
        operators: EQ,
        enum_values: &[],
    },
    QueryFieldDescriptor {
        name: "operation_type",
        field_type: QueryFieldType::Text,
        operators: TEXT,
        enum_values: &[],
    },
    QueryFieldDescriptor {
        name: "status",
        field_type: QueryFieldType::Enum,
        operators: EQ,
        enum_values: &["planned", "running", "completed", "failed", "cancelled"],
    },
    QueryFieldDescriptor {
        name: "started_at",
        field_type: QueryFieldType::Timestamp,
        operators: ORDERED,
        enum_values: &[],
    },
];
const RUNTIME_FIELDS: &[QueryFieldDescriptor] = &[
    QueryFieldDescriptor {
        name: "module",
        field_type: QueryFieldType::Text,
        operators: TEXT,
        enum_values: &[],
    },
    QueryFieldDescriptor {
        name: "role",
        field_type: QueryFieldType::Text,
        operators: TEXT,
        enum_values: &[],
    },
    QueryFieldDescriptor {
        name: "state",
        field_type: QueryFieldType::Enum,
        operators: EQ,
        enum_values: &["healthy", "stale", "missing", "degraded"],
    },
    QueryFieldDescriptor {
        name: "stale_after",
        field_type: QueryFieldType::Integer,
        operators: ORDERED,
        enum_values: &[],
    },
];

const SOURCE_DRIVER_SORT: &[QuerySortDescriptor] = &[
    QuerySortDescriptor {
        key: "source_id",
        default_descending: false,
    },
    QuerySortDescriptor {
        key: "family",
        default_descending: false,
    },
    QuerySortDescriptor {
        key: "readiness",
        default_descending: false,
    },
    QuerySortDescriptor {
        key: "enabled",
        default_descending: true,
    },
];
const SOURCE_MATERIAL_SORT: &[QuerySortDescriptor] = &[
    QuerySortDescriptor {
        key: "material_id",
        default_descending: false,
    },
    QuerySortDescriptor {
        key: "source_identifier",
        default_descending: false,
    },
    QuerySortDescriptor {
        key: "material_kind",
        default_descending: false,
    },
    QuerySortDescriptor {
        key: "status",
        default_descending: false,
    },
];
const DEBT_SORT: &[QuerySortDescriptor] = &[
    QuerySortDescriptor {
        key: "kind",
        default_descending: false,
    },
    QuerySortDescriptor {
        key: "severity",
        default_descending: true,
    },
    QuerySortDescriptor {
        key: "source",
        default_descending: false,
    },
    QuerySortDescriptor {
        key: "age",
        default_descending: true,
    },
];
const OPERATION_SORT: &[QuerySortDescriptor] = &[
    QuerySortDescriptor {
        key: "operation_id",
        default_descending: false,
    },
    QuerySortDescriptor {
        key: "operation_type",
        default_descending: false,
    },
    QuerySortDescriptor {
        key: "status",
        default_descending: false,
    },
];
const RUNTIME_SORT: &[QuerySortDescriptor] = &[
    QuerySortDescriptor {
        key: "module",
        default_descending: false,
    },
    QuerySortDescriptor {
        key: "role",
        default_descending: false,
    },
    QuerySortDescriptor {
        key: "state",
        default_descending: false,
    },
    QuerySortDescriptor {
        key: "active_count",
        default_descending: true,
    },
    QuerySortDescriptor {
        key: "inactive_count",
        default_descending: true,
    },
    QuerySortDescriptor {
        key: "stale_after",
        default_descending: true,
    },
];

static QUERY_UNITS: &[QueryUnitDescriptor] = &[
    QueryUnitDescriptor {
        unit: QueryUnitId::Events,
        object_kind: SinexObjectKind::Event,
        default_limit: 100,
        max_limit: 1000,
        supports_aggregation: true,
        fields: EVENT_FIELDS,
        sort_keys: &[],
        backing_rpc_methods: &["events.cards", "events.query"],
        disclosure_context: "view",
    },
    QueryUnitDescriptor {
        unit: QueryUnitId::SourceDrivers,
        object_kind: SinexObjectKind::SourceDriver,
        default_limit: 50,
        max_limit: 250,
        supports_aggregation: false,
        fields: SOURCE_DRIVER_FIELDS,
        sort_keys: SOURCE_DRIVER_SORT,
        backing_rpc_methods: &["sources.status_view", "sources.coverage"],
        disclosure_context: "view",
    },
    QueryUnitDescriptor {
        unit: QueryUnitId::SourceMaterials,
        object_kind: SinexObjectKind::SourceMaterial,
        default_limit: 50,
        max_limit: 250,
        supports_aggregation: false,
        fields: MATERIAL_FIELDS,
        sort_keys: SOURCE_MATERIAL_SORT,
        backing_rpc_methods: &["sources.show"],
        disclosure_context: "view",
    },
    QueryUnitDescriptor {
        unit: QueryUnitId::Debt,
        object_kind: SinexObjectKind::DebtRow,
        default_limit: 50,
        max_limit: 500,
        supports_aggregation: false,
        fields: DEBT_FIELDS,
        sort_keys: DEBT_SORT,
        backing_rpc_methods: &[
            "dlq.list",
            "sources.coverage",
            "sources.list",
            "sources.show",
            "automata.derivation_debt",
        ],
        disclosure_context: "view",
    },
    QueryUnitDescriptor {
        unit: QueryUnitId::Operations,
        object_kind: SinexObjectKind::Operation,
        default_limit: 50,
        max_limit: 500,
        supports_aggregation: false,
        fields: OPERATION_FIELDS,
        sort_keys: OPERATION_SORT,
        backing_rpc_methods: &["ops.list", "ops.get"],
        disclosure_context: "view",
    },
    QueryUnitDescriptor {
        unit: QueryUnitId::RuntimeHealth,
        object_kind: SinexObjectKind::RuntimeModule,
        default_limit: 50,
        max_limit: 250,
        supports_aggregation: false,
        fields: RUNTIME_FIELDS,
        sort_keys: RUNTIME_SORT,
        backing_rpc_methods: &[
            "system.health",
            "coordination.instance_health",
            "coordination.list_instances",
        ],
        disclosure_context: "view",
    },
];

#[must_use]
pub fn query_unit_descriptors() -> &'static [QueryUnitDescriptor] {
    QUERY_UNITS
}

#[must_use]
pub fn query_unit_descriptor(unit: QueryUnitId) -> &'static QueryUnitDescriptor {
    QUERY_UNITS
        .iter()
        .find(|descriptor| descriptor.unit == unit)
        .expect("every QueryUnitId must have a descriptor")
}

pub fn parse_sinex_query(input: &str) -> Result<SinexQuery, SinexError> {
    let parsed = parse_query_tokens
        .parse(input)
        .map_err(|error| SinexError::parse(format!("invalid Sinex query: {error}")))?;
    lower_tokens(parsed)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedQueryTokens {
    unit: String,
    predicates: Vec<ParsedPredicateToken>,
    connectors: Vec<ParsedConnector>,
    sorts: Vec<ParsedSortToken>,
    limit: Option<i64>,
    offset: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedPredicateToken {
    field: String,
    operator: String,
    value: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParsedConnector {
    And,
    Or,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedSortToken {
    key: String,
    descending: Option<bool>,
}

fn parse_query_tokens(input: &mut &str) -> ModalResult<ParsedQueryTokens> {
    skip_ws(input)?;
    let unit = identifier.parse_next(input)?.to_string();
    let mut predicates = Vec::new();
    let mut connectors = Vec::new();
    let mut sorts = Vec::new();

    if opt(ws("where")).parse_next(input)?.is_some() {
        predicates.push(predicate_token.parse_next(input)?);
        while let Some(connector) = opt(ws(connector_token)).parse_next(input)? {
            connectors.push(connector);
            predicates.push(predicate_token.parse_next(input)?);
        }
    }

    let mut limit = None;
    let mut offset = None;
    loop {
        if opt(ws("sort")).parse_next(input)?.is_some() {
            sorts.push(sort_token.parse_next(input)?);
            continue;
        }
        if opt(ws("limit")).parse_next(input)?.is_some() {
            limit = Some(number.parse_next(input)?);
            continue;
        }
        if opt(ws("offset")).parse_next(input)?.is_some() {
            offset = Some(number.parse_next(input)?);
            continue;
        }
        break;
    }
    skip_ws(input)?;

    Ok(ParsedQueryTokens {
        unit,
        predicates,
        connectors,
        sorts,
        limit,
        offset,
    })
}

fn lower_tokens(tokens: ParsedQueryTokens) -> Result<SinexQuery, SinexError> {
    let unit = tokens.unit.parse::<QueryUnitId>()?;
    let mut query = SinexQuery::new(unit, tokens.limit, tokens.offset);
    let descriptor = query_unit_descriptor(unit);

    let mut lowered = Vec::new();
    for token in tokens.predicates {
        let field = descriptor.field(&token.field)?;
        let operator = token.operator.parse::<QueryOperator>()?;
        descriptor.validate_operator(field, operator)?;
        let predicate = if operator == QueryOperator::Exists {
            SinexQueryPredicate::Has { field: token.field }
        } else {
            let value = token.value.ok_or_else(|| {
                SinexError::parse(format!(
                    "query field `{}` operator `{}` requires a value",
                    token.field,
                    operator.as_str()
                ))
            })?;
            let value = parse_query_value(field, value)?;
            SinexQueryPredicate::Compare {
                field: token.field,
                operator,
                value,
            }
        };
        lowered.push(predicate);
    }

    query.predicate = fold_predicates(lowered, tokens.connectors)?;
    query.sort = lower_sorts(descriptor, tokens.sorts)?;
    query.validate()?;
    Ok(query)
}

fn lower_sorts(
    descriptor: &QueryUnitDescriptor,
    sorts: Vec<ParsedSortToken>,
) -> Result<Vec<SinexQuerySort>, SinexError> {
    let mut lowered = Vec::with_capacity(sorts.len());
    for sort in sorts {
        let Some(sort_descriptor) = descriptor
            .sort_keys
            .iter()
            .find(|descriptor| descriptor.key == sort.key)
        else {
            return Err(SinexError::validation(format!(
                "query unit `{}` does not support sort key `{}`; supported sort keys: {}",
                descriptor.unit,
                sort.key,
                descriptor
                    .sort_keys
                    .iter()
                    .map(|key| key.key)
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        };
        lowered.push(SinexQuerySort {
            key: sort.key,
            descending: sort
                .descending
                .unwrap_or(sort_descriptor.default_descending),
        });
    }
    Ok(lowered)
}

fn parse_query_value(
    field: &QueryFieldDescriptor,
    value: String,
) -> Result<QueryValue, SinexError> {
    match field.field_type {
        QueryFieldType::Integer => value
            .parse::<i64>()
            .map(QueryValue::Integer)
            .map_err(|error| SinexError::parse(format!("invalid integer `{value}`: {error}"))),
        QueryFieldType::Boolean => value
            .parse::<bool>()
            .map(QueryValue::Boolean)
            .map_err(|error| SinexError::parse(format!("invalid boolean `{value}`: {error}"))),
        QueryFieldType::Text
        | QueryFieldType::Timestamp
        | QueryFieldType::Duration
        | QueryFieldType::Enum => Ok(QueryValue::String(value)),
    }
}

fn fold_predicates(
    mut predicates: Vec<SinexQueryPredicate>,
    connectors: Vec<ParsedConnector>,
) -> Result<Option<SinexQueryPredicate>, SinexError> {
    if predicates.is_empty() {
        return Ok(None);
    }
    if connectors.len() + 1 != predicates.len() {
        return Err(SinexError::parse(
            "query predicate connector count does not match predicate count",
        ));
    }

    let mut current = predicates.remove(0);
    for (connector, next) in connectors.into_iter().zip(predicates.into_iter()) {
        current = match connector {
            ParsedConnector::And => SinexQueryPredicate::And {
                predicates: vec![current, next],
            },
            ParsedConnector::Or => SinexQueryPredicate::Or {
                predicates: vec![current, next],
            },
        };
    }
    Ok(Some(current))
}

fn predicate_token(input: &mut &str) -> ModalResult<ParsedPredicateToken> {
    let field = ws(identifier).parse_next(input)?.to_string();
    let operator = ws(operator_token).parse_next(input)?.to_string();
    let value = if operator == "exists" {
        None
    } else {
        Some(ws(value_token).parse_next(input)?.to_string())
    };
    Ok(ParsedPredicateToken {
        field,
        operator,
        value,
    })
}

fn connector_token(input: &mut &str) -> ModalResult<ParsedConnector> {
    alt((
        "and".value(ParsedConnector::And),
        "or".value(ParsedConnector::Or),
    ))
    .parse_next(input)
}

fn sort_token(input: &mut &str) -> ModalResult<ParsedSortToken> {
    let key = ws(identifier).parse_next(input)?.to_string();
    let descending = opt(ws(sort_direction_token)).parse_next(input)?;
    Ok(ParsedSortToken { key, descending })
}

fn sort_direction_token(input: &mut &str) -> ModalResult<bool> {
    alt(("desc".value(true), "asc".value(false))).parse_next(input)
}

fn operator_token<'input>(input: &mut &'input str) -> ModalResult<&'input str> {
    alt((
        alt(("starts_with", "contains", "exists", "!=")),
        alt((">=", "<=", "==", "=", ">", "<")),
    ))
    .parse_next(input)
}

fn value_token<'input>(input: &mut &'input str) -> ModalResult<&'input str> {
    alt((quoted_string, bare_value)).parse_next(input)
}

fn quoted_string<'input>(input: &mut &'input str) -> ModalResult<&'input str> {
    delimited('"', take_until(0.., "\""), '"').parse_next(input)
}

fn bare_value<'input>(input: &mut &'input str) -> ModalResult<&'input str> {
    take_while(1.., |c: char| {
        !c.is_whitespace() && c != '"' && c != '(' && c != ')'
    })
    .parse_next(input)
}

fn number(input: &mut &str) -> ModalResult<i64> {
    take_while(1.., |c: char| c.is_ascii_digit())
        .try_map(str::parse::<i64>)
        .parse_next(input)
}

fn identifier<'input>(input: &mut &'input str) -> ModalResult<&'input str> {
    (
        one_of(|c: char| c.is_ascii_alphabetic() || c == '_'),
        take_while(0.., |c: char| {
            c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.'
        }),
    )
        .take()
        .parse_next(input)
}

fn skip_ws(input: &mut &str) -> ModalResult<()> {
    let _: &str = multispace0.parse_next(input)?;
    Ok(())
}

fn ws<'input, O, P>(
    mut parser: P,
) -> impl Parser<&'input str, O, winnow::error::ErrMode<winnow::error::ContextError>>
where
    P: Parser<&'input str, O, winnow::error::ErrMode<winnow::error::ContextError>>,
{
    move |input: &mut &'input str| {
        skip_ws(input)?;
        let output = parser.parse_next(input)?;
        skip_ws(input)?;
        Ok(output)
    }
}

#[cfg(test)]
#[path = "query_units_test.rs"]
mod tests;
