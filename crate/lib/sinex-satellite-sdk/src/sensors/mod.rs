//! Sensor implementations for different acquisition strategies

pub mod append_stream;
pub mod patterns;
pub mod tree_watch;

pub use append_stream::{AppendStreamConfig, AppendStreamSensor};
pub use patterns::{BatchedPullSensor, MultiFileSensor, ReplaceSnapshotSensor};
pub use tree_watch::{TreeWatchConfig, TreeWatchSensor};
