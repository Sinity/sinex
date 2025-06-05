mod cli;
mod config;
mod error;
mod event_listener;
mod logging;
mod shutdown;
mod ingestor;

use ingestor::HyprlandIngestor;

sinex_shared::ingestor_main!(HyprlandIngestor);