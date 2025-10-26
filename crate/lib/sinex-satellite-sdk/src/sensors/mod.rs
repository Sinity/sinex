//! Sensor implementations for different acquisition strategies

pub mod append_stream;
pub mod tree_watch;

pub use append_stream::{AppendStreamConfig, AppendStreamSensor};
pub use tree_watch::{TreeWatchConfig, TreeWatchSensor};
