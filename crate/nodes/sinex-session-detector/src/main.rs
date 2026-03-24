//! Main for Session Detector

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use sinex_node_sdk::node_entrypoint;
use sinex_session_detector::SessionDetectorNode;

node_entrypoint!(SessionDetectorNode);
