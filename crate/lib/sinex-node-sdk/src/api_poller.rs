use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{debug, info};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ApiPollerState {
    pub cursor: Option<String>,
    pub last_poll_at: Option<String>,
    pub total_items_fetched: u64,
    pub consecutive_empty_polls: u32,
}

impl ApiPollerState {
    pub fn advance_cursor(&mut self, new_cursor: Option<String>, items_fetched: u64) {
        if new_cursor.is_some() {
            self.cursor = new_cursor;
        }
        self.total_items_fetched += items_fetched;
        self.last_poll_at = Some(
            time::OffsetDateTime::now_utc()
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_default(),
        );

        if items_fetched == 0 {
            self.consecutive_empty_polls += 1;
        } else {
            self.consecutive_empty_polls = 0;
        }
    }

    #[must_use]
    pub fn backoff_duration(&self, base_interval: Duration) -> Duration {
        if self.consecutive_empty_polls == 0 {
            return base_interval;
        }
        let multiplier = 2u32.saturating_pow(self.consecutive_empty_polls.min(6));
        let backed_off = base_interval.saturating_mul(multiplier);
        let max = Duration::from_hours(1);
        backed_off.min(max)
    }
}

#[derive(Debug, Clone)]
pub struct ApiPollerConfig {
    pub base_url: String,
    pub poll_interval: Duration,
    pub max_items_per_poll: u32,
    pub auth_header: Option<(String, String)>,
}

impl ApiPollerConfig {
    pub fn new(base_url: impl Into<String>, poll_interval: Duration) -> Self {
        Self {
            base_url: base_url.into(),
            poll_interval,
            max_items_per_poll: 100,
            auth_header: None,
        }
    }

    pub fn with_auth(
        mut self,
        header_name: impl Into<String>,
        header_value: impl Into<String>,
    ) -> Self {
        self.auth_header = Some((header_name.into(), header_value.into()));
        self
    }

    #[must_use]
    pub fn with_max_items(mut self, max: u32) -> Self {
        self.max_items_per_poll = max;
        self
    }
}

pub struct PollResult<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

impl<T> PollResult<T> {
    #[must_use]
    pub fn empty() -> Self {
        Self {
            items: Vec::new(),
            next_cursor: None,
            has_more: false,
        }
    }

    #[must_use]
    pub fn with_items(items: Vec<T>, next_cursor: Option<String>) -> Self {
        let has_more = next_cursor.is_some();
        Self {
            items,
            next_cursor,
            has_more,
        }
    }
}

pub fn should_poll(state: &ApiPollerState, config: &ApiPollerConfig) -> bool {
    let interval = state.backoff_duration(config.poll_interval);

    if let Some(last_poll) = &state.last_poll_at
        && let Ok(last) =
            time::OffsetDateTime::parse(last_poll, &time::format_description::well_known::Rfc3339)
    {
        let elapsed = time::OffsetDateTime::now_utc() - last;
        let needed = time::Duration::new(interval.as_secs() as i64, 0);
        if elapsed < needed {
            debug!(
                next_poll_in_secs = (needed - elapsed).whole_seconds(),
                consecutive_empty = state.consecutive_empty_polls,
                "Skipping poll (backoff active)"
            );
            return false;
        }
    }

    true
}

pub fn log_poll_result<T>(state: &ApiPollerState, items_count: usize) {
    if items_count > 0 {
        info!(
            items = items_count,
            total = state.total_items_fetched,
            "API poll returned new items"
        );
    } else if state.consecutive_empty_polls > 3 {
        debug!(
            consecutive_empty = state.consecutive_empty_polls,
            "API poll returned no new items (backing off)"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_state_has_no_cursor() {
        let state = ApiPollerState::default();
        assert!(state.cursor.is_none());
        assert_eq!(state.total_items_fetched, 0);
        assert_eq!(state.consecutive_empty_polls, 0);
    }

    #[test]
    fn advance_cursor_tracks_fetched_items() {
        let mut state = ApiPollerState::default();
        state.advance_cursor(Some("page2".to_string()), 50);
        assert_eq!(state.cursor.as_deref(), Some("page2"));
        assert_eq!(state.total_items_fetched, 50);
        assert_eq!(state.consecutive_empty_polls, 0);
        assert!(state.last_poll_at.is_some());
    }

    #[test]
    fn empty_polls_increment_counter() {
        let mut state = ApiPollerState::default();
        state.advance_cursor(None, 0);
        assert_eq!(state.consecutive_empty_polls, 1);
        state.advance_cursor(None, 0);
        assert_eq!(state.consecutive_empty_polls, 2);
        state.advance_cursor(Some("c".to_string()), 5);
        assert_eq!(state.consecutive_empty_polls, 0);
    }

    #[test]
    fn backoff_doubles_on_empty_polls() {
        let mut state = ApiPollerState::default();
        let base = Duration::from_mins(1);

        assert_eq!(state.backoff_duration(base), base);

        state.consecutive_empty_polls = 1;
        assert_eq!(state.backoff_duration(base), Duration::from_mins(2));

        state.consecutive_empty_polls = 2;
        assert_eq!(state.backoff_duration(base), Duration::from_mins(4));

        state.consecutive_empty_polls = 3;
        assert_eq!(state.backoff_duration(base), Duration::from_mins(8));
    }

    #[test]
    fn backoff_capped_at_one_hour() {
        let mut state = ApiPollerState::default();
        state.consecutive_empty_polls = 20;
        let base = Duration::from_mins(1);
        assert_eq!(state.backoff_duration(base), Duration::from_hours(1));
    }

    #[test]
    fn poll_result_empty() {
        let result: PollResult<String> = PollResult::empty();
        assert!(result.items.is_empty());
        assert!(result.next_cursor.is_none());
        assert!(!result.has_more);
    }

    #[test]
    fn poll_result_with_items() {
        let result = PollResult::with_items(
            vec!["a".to_string(), "b".to_string()],
            Some("next".to_string()),
        );
        assert_eq!(result.items.len(), 2);
        assert_eq!(result.next_cursor.as_deref(), Some("next"));
        assert!(result.has_more);
    }

    #[test]
    fn config_builder() {
        let config = ApiPollerConfig::new("https://api.example.com", Duration::from_mins(5))
            .with_auth("Authorization", "Bearer token123")
            .with_max_items(50);

        assert_eq!(config.base_url, "https://api.example.com");
        assert_eq!(config.poll_interval, Duration::from_mins(5));
        assert_eq!(config.max_items_per_poll, 50);
        assert!(config.auth_header.is_some());
    }
}
