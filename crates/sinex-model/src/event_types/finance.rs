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
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "transaction.posted", key = ["host", "journal", "member", "member_duplicate", "block_hash", "duplicate"], replace = repeat)]
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
    pub status: Option<String>,
    pub description: String,
    pub narration: Option<String>,
    pub comment: Option<String>,
    pub postings: Vec<Posting>,
}

#[cfg(test)]
mod tests;
