mod cli;
mod config;
mod kitty_listener;
mod ingestor;

use ingestor::KittyIngestor;

sinex_shared::ingestor_main!(KittyIngestor);