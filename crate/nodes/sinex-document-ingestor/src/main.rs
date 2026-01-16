//! Main entry point for Document Ingestor using unified Node

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;
use sinex_document_ingestor::DocumentProcessor;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

// Use the processor_main! macro for standardized CLI and lifecycle
sinex_processor_runtime::processor_main!(DocumentProcessor);
