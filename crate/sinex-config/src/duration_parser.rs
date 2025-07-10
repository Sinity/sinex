//! Duration parsing utilities

use std::time::Duration;

pub fn parse_duration(s: &str) -> Result<Duration, String> {
    // Simple duration parser - accepts format like "30s", "5m", "2h"
    if s.is_empty() {
        return Err("Empty duration string".to_string());
    }
    
    let s = s.trim();
    
    if s.ends_with('s') {
        let num_str = &s[..s.len()-1];
        let seconds: u64 = num_str.parse().map_err(|_| "Invalid number format")?;
        Ok(Duration::from_secs(seconds))
    } else if s.ends_with('m') {
        let num_str = &s[..s.len()-1];
        let minutes: u64 = num_str.parse().map_err(|_| "Invalid number format")?;
        Ok(Duration::from_secs(minutes * 60))
    } else if s.ends_with('h') {
        let num_str = &s[..s.len()-1];
        let hours: u64 = num_str.parse().map_err(|_| "Invalid number format")?;
        Ok(Duration::from_secs(hours * 3600))
    } else {
        // Try parsing as raw seconds
        let seconds: u64 = s.parse().map_err(|_| "Invalid duration format")?;
        Ok(Duration::from_secs(seconds))
    }
}