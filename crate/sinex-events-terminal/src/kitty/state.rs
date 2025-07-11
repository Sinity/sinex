use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

/// Kitty process information
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct KittyProcessInfo {
    pub pid: u32,
    pub name: String,
    pub cmdline: Option<String>,
    pub parent_pid: Option<u32>,
}

/// Internal window state tracking
#[derive(Debug, Clone)]
pub struct KittyWindowState {
    pub tab_id: String,
    pub last_command: Option<String>,
    pub last_prompt_time: Option<SystemTime>,
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
