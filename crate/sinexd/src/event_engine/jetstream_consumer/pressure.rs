//! Stream capacity pressure inspection and telemetry sampling for `JetStreamConsumer`.

use super::*;

impl JetStreamConsumer {
    /// Check stream capacity and log warnings if approaching limits
    pub(super) async fn check_stream_capacity(&self, stream_name: &str) {
        match self.js.get_stream(stream_name).await {
            Ok(mut stream) => {
                match stream.info().await {
                    Ok(info) => {
                        let state = info.state.clone();
                        let config = info.config.clone();

                        // Emit stream stats via self-observer
                        if let Some(ref observer) = self.observer
                            && let Err(error) = observer
                                .emit_stream_stats(
                                    stream_name,
                                    state.messages,
                                    config.max_messages as u64,
                                    state.bytes,
                                    config.max_bytes as u64,
                                    state.consumer_count as u32,
                                    state.first_sequence,
                                    state.last_sequence,
                                )
                                .await
                        {
                            Self::log_observer_error(&self.stats, "event_engine.stream", &error);
                        }

                        let pressure = StreamPressureSnapshot::from_limits(
                            state.messages,
                            config.max_messages as u64,
                            state.bytes,
                            config.max_bytes as u64,
                        );
                        if let Some(pressure_sample_total) = self
                            .record_stream_pressure_sample(stream_name, pressure)
                            .await
                        {
                            warn!(
                                stream = %stream_name,
                                pressure_level = ?pressure.pressure_level,
                                limiting_dimension = ?pressure.limiting_dimension,
                                pressure_sample_total,
                                messages = state.messages,
                                max_messages = config.max_messages,
                                bytes = state.bytes,
                                max_bytes = config.max_bytes,
                                fill_percent = format!("{:.1}%", pressure.fill_pct),
                                message_fill_percent = format!("{:.1}%", pressure.message_fill_pct),
                                byte_fill_percent = format!("{:.1}%", pressure.byte_fill_pct),
                                "Stream capacity pressure detected"
                            );
                        }
                    }
                    Err(error) => {
                        debug!(
                            stream = %stream_name,
                            error = %error,
                            "Failed to inspect stream capacity"
                        );
                    }
                }
            }
            Err(e) => {
                debug!("Failed to check stream capacity for {}: {}", stream_name, e);
            }
        }
    }

    pub(super) async fn record_stream_pressure_sample(
        &self,
        stream_name: &str,
        pressure: StreamPressureSnapshot,
    ) -> Option<u64> {
        let mut state = self.stream_pressure_warning_state.lock().await;
        record_stream_pressure_warning_sample(&mut state, stream_name, pressure)
    }
}
