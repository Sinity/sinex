pub mod configuration;
pub mod database;
pub mod resources;
pub mod services;
pub mod verification;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum VerificationStatus {
    Pass,
    Warning,
    Fail,
}
