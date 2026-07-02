use super::strategies::*;
use crate::Timestamp;
use proptest::prelude::*;
use xtask::sandbox::sinex_proptest;

sinex_proptest! {
    fn test_sanitized_path_strategy(path in sanitized_path()) {
        prop_assert!(path.as_str().starts_with('/'), "path should start with /");
        prop_assert!(!path.as_str().is_empty(), "path should not be empty");
        Ok(())
    }

    fn test_timestamp_strategy(ts in timestamp()) {
        let now = Timestamp::now();
        prop_assert!(ts.unix_timestamp() <= now.unix_timestamp(), "timestamp should be in the past");
        Ok(())
    }

    fn test_file_created_payload_strategy(payload in file_created_payload()) {
        prop_assert!(!payload.path.as_str().is_empty(), "path should not be empty");
        Ok(())
    }

    fn test_file_created_event_strategy(event in file_created_event()) {
        prop_assert!(!event.source.as_str().is_empty(), "source should not be empty");
        prop_assert!(!event.event_type.as_str().is_empty(), "event_type should not be empty");
        Ok(())
    }
}
