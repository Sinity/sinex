//! Actual receipt accounting without asserting a verified model request. The
//! caller must have the separate operator capability before writer admission.
use super::{Budget, Policy, nonempty};
use crate::event_types::model_call::Charge;
use crate::{Timestamp, json_identity};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BudgetBasis {
    pub scope: String,
    pub currency: String,
    pub unit: String,
    pub period_start: Timestamp,
    pub period_end: Timestamp,
}
impl BudgetBasis {
    pub fn validate(&self) -> Result<(), String> {
        nonempty(&self.scope, "receipt accounting scope")?;
        nonempty(&self.currency, "receipt currency")?;
        nonempty(&self.unit, "receipt unit")?;
        if self.period_start >= self.period_end {
            return Err("receipt accounting period must have positive duration".into());
        }
        Ok(())
    }
    pub fn bucket(&self) -> String {
        json_identity::hash(
            &serde_json::to_value((
                &self.scope,
                &self.currency,
                &self.unit,
                self.period_start,
                self.period_end,
            ))
            .expect("budget basis serializes"),
        )
    }
}
impl From<&Budget> for BudgetBasis {
    fn from(budget: &Budget) -> Self {
        Self {
            scope: budget.scope.clone(),
            currency: budget.currency.clone(),
            unit: budget.unit.clone(),
            period_start: budget.period_start,
            period_end: budget.period_end,
        }
    }
}

/// Every accounting field is supplied evidence. There is no default amount,
/// currency, provider account, accounting period, policy version or request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReceiptOnly {
    pub provider: String,
    pub provider_account: String,
    pub provider_receipt: String,
    pub policy_name: String,
    pub policy_version: String,
    pub operation: String,
    pub basis: BudgetBasis,
    pub actual: Charge,
}
impl ReceiptOnly {
    pub fn identity(&self) -> String {
        receipt_identity(
            &self.provider,
            &self.provider_account,
            &self.provider_receipt,
        )
    }
    pub fn validate(&self) -> Result<(), String> {
        for (value, name) in [
            (&self.provider, "receipt provider"),
            (&self.provider_account, "receipt provider account"),
            (&self.provider_receipt, "native receipt identity"),
            (&self.policy_name, "receipt policy name"),
            (&self.policy_version, "receipt policy version"),
            (&self.operation, "receipt operation"),
        ] {
            nonempty(value, name)?;
        }
        self.basis.validate()?;
        if self.actual.currency != self.basis.currency || self.actual.unit != self.basis.unit {
            return Err(
                "actual receipt charge differs from its accounting currency or unit".into(),
            );
        }
        Ok(())
    }
    pub fn matches(&self, policy: &Policy) -> Result<(), String> {
        self.validate()?;
        policy.validate()?;
        let route = policy.route(&self.operation)?;
        if self.policy_name != policy.name
            || self.policy_version != policy.version
            || self.basis != BudgetBasis::from(&policy.budget)
            || self.provider != route.provider
            || self.provider_account != route.provider_account
        {
            return Err("actual receipt differs from its authorized policy version, route or accounting basis".into());
        }
        Ok(())
    }
    /// Same native receipt cannot be charged again under another policy,
    /// period or amount. Observation time is deliberately absent.
    pub fn consistent_with(&self, previous: &Self) -> Result<(), String> {
        self.validate()?;
        previous.validate()?;
        if self != previous {
            return Err("contradictory actual receipt requires operator review".into());
        }
        Ok(())
    }
}

pub fn receipt_identity(provider: &str, account: &str, receipt: &str) -> String {
    json_identity::hash(&serde_json::json!([
        "model-provider-receipt",
        provider,
        account,
        receipt
    ]))
}
