mod cli;
mod config;
mod filesystem_watcher;
mod ingestor;

use ingestor::FilesystemIngestor;

sinex_shared::ingestor_main!(FilesystemIngestor);