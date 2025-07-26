//! Main entry point for Document Ingestor using unified StatefulStreamProcessor

use sinex_document_ingestor::DocumentProcessor;

// Use the processor_main! macro for standardized CLI and lifecycle
sinex_satellite_sdk::processor_main!(DocumentProcessor);
