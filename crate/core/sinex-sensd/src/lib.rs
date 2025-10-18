#![doc = include_str!("../doc/README.md")]
#![doc = include_str!("../../../../docs/architecture/Core_Architecture.md")]
#![doc = include_str!("../../../../docs/architecture/satellite-implementation.md")]
#![allow(unexpected_cfgs, unused_imports, unused_variables, dead_code)]

//! Core modules for source material acquisition and temporal ledger management.

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
