//! Journal transactions with exact written amounts and civil dates.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Posting {
    pub account: String,
    /// Decimal text as written; an implicit amount stays absent.
    pub amount: Option<String>,
    pub currency: Option<String>,
    pub comment: Option<String>,
    pub status: Option<String>,
    /// Parentheses are unbalanced virtual postings; brackets are balanced virtual postings.
    pub virtual_account: Option<String>,
    pub price: Option<PostingAmount>,
    pub assertion: Option<PostingAmount>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PostingAmount {
    pub operator: String,
    pub amount: String,
    pub currency: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct IncludedFrom {
    pub path: String,
    pub block_hash: String,
    pub duplicate: u64,
    pub parent: Option<Box<IncludedFrom>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct IncludeTarget {
    pub path: String,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "transaction.posted", key = ["host", "journal", "member", "member_duplicate", "block_hash", "duplicate", "included_from"], replace = repeat)]
pub struct TransactionPosted {
    pub host: String,
    pub journal: String,
    pub member: Option<String>,
    pub member_duplicate: u64,
    /// Digest of the complete source block, including every posting.
    pub block_hash: String,
    /// Zero-based number among equal blocks in the member.
    pub duplicate: u64,
    /// A civil date does not identify an instant.
    pub date: String,
    pub effective_date: Option<String>,
    /// Resolved only when a full date or an authored default year is available.
    pub resolved_date: Option<String>,
    pub resolved_effective_date: Option<String>,
    pub default_year: Option<i16>,
    pub code: Option<String>,
    pub included_from: Option<IncludedFrom>,
    pub status: Option<String>,
    pub description: String,
    pub narration: Option<String>,
    pub comment: Option<String>,
    pub postings: Vec<Posting>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "journal.directive", key = ["host", "journal", "member", "member_duplicate", "block_hash", "duplicate", "included_from"], replace = repeat)]
pub struct JournalDirective {
    pub host: String,
    pub journal: String,
    pub member: Option<String>,
    pub member_duplicate: u64,
    pub block_hash: String,
    pub duplicate: u64,
    pub directive: String,
    pub text: String,
    pub included_from: Option<IncludedFrom>,
    pub includes: Vec<IncludeTarget>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "journal.imported", key = ["host", "journal", "member", "member_duplicate", "block_hash", "duplicate", "included_from"], replace = repeat)]
pub struct JournalImported {
    pub host: String,
    pub journal: String,
    pub member: Option<String>,
    pub member_duplicate: u64,
    pub block_hash: String,
    pub duplicate: u64,
    pub included_from: Option<IncludedFrom>,
    pub format: String,
    pub text: String,
}

crate::payload_enum! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum JournalRecord {
        Transaction(TransactionPosted),
        Directive(JournalDirective),
        Imported(JournalImported),
    }
}

#[cfg(test)]
mod tests;
