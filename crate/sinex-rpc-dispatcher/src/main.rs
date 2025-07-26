//! Main entry point for RPC Dispatcher using unified StatefulStreamProcessor

mod lib;

use lib::RpcDispatcherProcessor;

// Use the processor_main! macro for standardized CLI and lifecycle
sinex_satellite_sdk::processor_main!(RpcDispatcherProcessor);
