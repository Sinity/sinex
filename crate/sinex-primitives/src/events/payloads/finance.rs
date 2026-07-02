//! Finance-domain event payloads.
//!
//! Hosts personal-finance observations from hledger/ledger journals and
//! bank exports. Per the parser how-to, group by domain not provider —
//! future sources (Beancount, YNAB, mBank CSV) land here too.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

use crate::Timestamp;

/// One account posting within a transaction (debit or credit leg).
///
/// In hledger format, a transaction may have two or more postings that
/// must balance to zero. Each posting names an account hierarchy and
/// optionally an explicit amount + currency. When the last posting omits
/// the amount, hledger infers it by balance; we surface the inferred
/// amount as `None`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct LedgerPosting {
    /// Full account name, colon-delimited hierarchy (e.g.
    /// `"Expenses:Consumable:Food"`).
    pub account: String,

    /// Amount as a decimal string (e.g. `"52.97"`). `None` when the
    /// posting amount is implicit (hledger balances it to zero).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub amount: Option<String>,

    /// Currency or commodity code (e.g. `"PLN"`, `"USD"`, `"ETH"`).
    /// `None` when amount is implicit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub currency: Option<String>,
}

/// One hledger/ledger-format transaction from a personal journal file
/// (#1074).
///
/// Emitted once per transaction block. A block starts with a
/// `YYYY-MM-DD <payee>|<description>` header line (the pipe separator
/// is a personal convention used in this journal to split payee from
/// narration) followed by two or more posting lines.
///
/// Privacy tier is Sensitive: the journal contains merchant names,
/// income amounts, and account hierarchy that reveals purchasing and
/// salary patterns.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "ledger", event_type = "transaction.posted")]
pub struct LedgerTransactionPayload {
    /// Transaction date as recorded in the journal (`YYYY-MM-DD`).
    pub date: Timestamp,

    /// Primary label on the transaction header line. When the journal
    /// uses `Payee|Narration` format this is the payee; otherwise it is
    /// the full description string.
    pub description: String,

    /// Narration text after the pipe separator (personal convention in
    /// this journal). `None` when the header has no pipe separator.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub narration: Option<String>,

    /// Account postings — at least two for a balanced transaction.
    pub postings: Vec<LedgerPosting>,

    /// Inline comment on the transaction header line (`;` prefix),
    /// stripped of the leading semicolon. `None` when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

#[cfg(test)]
#[path = "finance_test.rs"]
mod tests;
