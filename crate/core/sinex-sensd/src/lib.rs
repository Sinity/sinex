#![doc = include_str!("../doc/README.md")]
#![doc = include_str!("../../../../docs/architecture/Core_Architecture.md")]
#![doc = include_str!("../../../lib/sinex-satellite-sdk/doc/overview.md")]
#![allow(unexpected_cfgs, unused_imports, unused_variables, dead_code)]

//! **DEPRECATED**: Legacy sensor management daemon
//!
//! This crate is deprecated in favor of the JetStream-first architecture where satellites
//! publish source material and events directly to NATS using `sinex-satellite-sdk`'s
//! `NatsPublisher` and `AcquisitionManager`.
//!
//! Modern deployments should:
//! - Use satellites with `--nats-url` flag to publish directly to NATS
//! - Use `sinex-ingestd` as the universal archiver consuming from JetStream
//! - Avoid gRPC ingestion path which is legacy/fallback only
//!
//! This crate is kept for backward compatibility and migration purposes but will be
//! removed in a future version.
//!
//! Core modules for source material acquisition and temporal ledger management (LEGACY).

pub mod config;
pub mod grpc_server;
pub mod integration_test;
pub mod job_manager;
pub mod material_rotation;
pub mod material_stream;
pub mod sensors;
pub mod service;
pub mod temporal_ledger;

pub use config::SensdConfig;
pub use service::SensdService;
