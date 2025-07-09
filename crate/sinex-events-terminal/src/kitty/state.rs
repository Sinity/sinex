/// State definitions for Kitty terminal structures
/// 
/// This module defines all the data structures that represent Kitty's state:
/// windows, tabs, processes, and their relationships.

use serde::{Deserialize, Serialize};
use schemars::JsonSchema;
use std::collections::HashMap;
use std::time::SystemTime;

/// Kitty process information
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct KittyProcessInfo {
    pub pid: u32,
    pub name: String,
    pub cmdline: Option<String>,
    pub parent_pid: Option<u32>,
}

/// Internal process representation from Kitty API
#[derive(Debug, Deserialize)]
pub struct KittyProcess {
    pub pid: u32,
    pub name: String,
}

/// Kitty window (pane) information from API
#[derive(Debug, Deserialize)]
pub struct KittyWindow {
    pub id: i64,
    pub cwd: Option<String>,
    pub foreground_processes: Vec<KittyProcess>,
    pub last_cmd_exit_status: Option<i32>,
    pub parent_tab_id: String,
}

/// Internal window state tracking
#[derive(Debug, Clone)]
pub struct KittyWindowState {
    pub tab_id: String,
    pub last_command: Option<String>,
    pub last_prompt_time: Option<SystemTime>,
}

/// State manager for tracking Kitty windows and processes
#[derive(Debug)]
pub struct KittyStateManager {
    pub window_states: HashMap<String, KittyWindowState>,
    pub last_scrollback_line_counts: HashMap<String, u32>,
    pub last_focused_tab: Option<String>,
    pub process_states: HashMap<String, KittyProcessInfo>,
}

impl KittyStateManager {
    pub fn new() -> Self {
        Self {
            window_states: HashMap::new(),
            last_scrollback_line_counts: HashMap::new(),
            last_focused_tab: None,
            process_states: HashMap::new(),
        }
    }

    /// Get or create window state for a given window ID
    pub fn get_or_create_window_state(&mut self, window_id: String, tab_id: String) -> &mut KittyWindowState {
        self.window_states
            .entry(window_id)
            .or_insert_with(|| KittyWindowState {
                tab_id,
                last_command: None,
                last_prompt_time: None,
            })
    }

    /// Check if a process has changed for a window
    pub fn has_process_changed(&self, window_id: &str, current_process: &KittyProcessInfo) -> bool {
        self.process_states
            .get(window_id)
            .map(|prev| prev.pid != current_process.pid || prev.name != current_process.name)
            .unwrap_or(true)
    }

    /// Update stored process state for a window
    pub fn update_process_state(&mut self, window_id: String, process: KittyProcessInfo) {
        self.process_states.insert(window_id, process);
    }

    /// Get previous process state for a window
    pub fn get_previous_process(&self, window_id: &str) -> Option<KittyProcessInfo> {
        self.process_states.get(window_id).cloned()
    }

    /// Check if tab focus has changed
    pub fn has_focus_changed(&self, focused_tab_id: &str) -> bool {
        self.last_focused_tab.as_ref() != Some(focused_tab_id)
    }

    /// Update focused tab
    pub fn update_focused_tab(&mut self, tab_id: String) {
        self.last_focused_tab = Some(tab_id);
    }

    /// Get previous focused tab
    pub fn get_previous_focused_tab(&self) -> Option<String> {
        self.last_focused_tab.clone()
    }

    /// Check if window has new scrollback content
    pub fn has_new_scrollback(&self, window_id: &str, current_line_count: u32) -> bool {
        let previous_count = self.last_scrollback_line_counts
            .get(window_id)
            .copied()
            .unwrap_or(0);
        current_line_count > previous_count
    }

    /// Update scrollback line count for a window
    pub fn update_scrollback_count(&mut self, window_id: String, line_count: u32) {
        self.last_scrollback_line_counts.insert(window_id, line_count);
    }

    /// Get new scrollback lines range
    pub fn get_scrollback_range(&self, window_id: &str, current_line_count: u32) -> (usize, u32) {
        let previous_count = self.last_scrollback_line_counts
            .get(window_id)
            .copied()
            .unwrap_or(0);
        (previous_count as usize, previous_count)
    }
}

impl Default for KittyStateManager {
    fn default() -> Self {
        Self::new()
    }
}