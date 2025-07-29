//! Main entry point for RPC Dispatcher using unified StatefulStreamProcessor

use sinex_rpc_dispatcher::RpcDispatcherProcessor;

// Use the processor_main! macro for standardized CLI and lifecycle
sinex_satellite_sdk::processor_main!(RpcDispatcherProcessor);
