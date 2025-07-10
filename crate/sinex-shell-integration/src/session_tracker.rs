//! Shell Session Tracking
//!
//! This module provides utilities for tracking shell sessions and correlating
//! commands across session boundaries.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

use crate::shell_detector::ShellInfo;

/// Information about a shell session
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionInfo {
    pub session_id: String,
    pub shell_info: ShellInfo,
    pub start_time: chrono::DateTime<chrono::Utc>,
    pub end_time: Option<chrono::DateTime<chrono::Utc>>,
    pub working_directory: String,
    pub environment_vars: HashMap<String, String>,
    pub command_count: u64,
    pub last_activity: chrono::DateTime<chrono::Utc>,
    pub status: SessionStatus,
}

/// Status of a shell session
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionStatus {
    Active,
    Inactive,
    Ended,
}

/// Types of session events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SessionEvent {
    Started {
        session_id: String,
        shell_info: ShellInfo,
        working_directory: String,
    },
    CommandExecuted {
        session_id: String,
        command: String,
        working_directory: String,
        timestamp: chrono::DateTime<chrono::Utc>,
    },
    DirectoryChanged {
        session_id: String,
        old_directory: String,
        new_directory: String,
        timestamp: chrono::DateTime<chrono::Utc>,
    },
    Ended {
        session_id: String,
        end_time: chrono::DateTime<chrono::Utc>,
        command_count: u64,
    },
}

/// Tracks active shell sessions and their state
pub struct SessionTracker {
    sessions: Arc<RwLock<HashMap<String, SessionInfo>>>,
}

impl SessionTracker {
    /// Create a new session tracker
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Start a new shell session
    pub async fn start_session(
        &mut self,
        session_id: Option<String>,
        shell_info: &ShellInfo,
    ) -> sinex_core::Result<String> {
        let session_id = session_id.unwrap_or_else(|| {
            format!("session-{}", sinex_ulid::Ulid::new())
        });
        
        let working_directory = std::env::current_dir()
            .map(|d| d.to_string_lossy().to_string())
            .unwrap_or_else(|_| "/unknown".to_string());
        
        let environment_vars = Self::capture_relevant_env_vars();
        
        let session_info = SessionInfo {
            session_id: session_id.clone(),
            shell_info: shell_info.clone(),
            start_time: chrono::Utc::now(),
            end_time: None,
            working_directory: working_directory.clone(),
            environment_vars,
            command_count: 0,
            last_activity: chrono::Utc::now(),
            status: SessionStatus::Active,
        };
        
        let mut sessions = self.sessions.write().await;
        sessions.insert(session_id.clone(), session_info);
        
        info!(
            session_id = %session_id,
            shell_type = ?shell_info.shell_type,
            working_directory = %working_directory,
            "Started new shell session"
        );
        
        Ok(session_id)
    }
    
    /// End a shell session
    pub async fn end_session(&mut self, session_id: &str) -> sinex_core::Result<()> {
        let mut sessions = self.sessions.write().await;
        
        if let Some(session) = sessions.get_mut(session_id) {
            session.end_time = Some(chrono::Utc::now());
            session.status = SessionStatus::Ended;
            
            info!(
                session_id = %session_id,
                duration_minutes = ?(session.end_time.unwrap() - session.start_time).num_minutes(),
                command_count = session.command_count,
                "Ended shell session"
            );
        } else {
            debug!("Attempted to end unknown session: {}", session_id);
        }
        
        Ok(())
    }
    
    /// Record a command execution in a session
    pub async fn record_command(
        &mut self,
        session_id: &str,
        command: &str,
        working_directory: Option<&str>,
    ) -> sinex_core::Result<()> {
        let mut sessions = self.sessions.write().await;
        
        if let Some(session) = sessions.get_mut(session_id) {
            session.command_count += 1;
            session.last_activity = chrono::Utc::now();
            
            if let Some(new_dir) = working_directory {
                if new_dir != session.working_directory {
                    debug!(
                        session_id = %session_id,
                        old_dir = %session.working_directory,
                        new_dir = %new_dir,
                        "Directory changed in session"
                    );
                    session.working_directory = new_dir.to_string();
                }
            }
            
            debug!(
                session_id = %session_id,
                command = %command,
                command_count = session.command_count,
                "Recorded command in session"
            );
        } else {
            debug!("Attempted to record command in unknown session: {}", session_id);
        }
        
        Ok(())
    }
    
    /// Get information about a specific session
    pub async fn get_session(&self, session_id: &str) -> Option<SessionInfo> {
        let sessions = self.sessions.read().await;
        sessions.get(session_id).cloned()
    }
    
    /// Get all active sessions
    pub async fn get_active_sessions(&self) -> Vec<SessionInfo> {
        let sessions = self.sessions.read().await;
        sessions
            .values()
            .filter(|s| s.status == SessionStatus::Active)
            .cloned()
            .collect()
    }
    
    /// Get all sessions (active and ended)
    pub async fn get_all_sessions(&self) -> Vec<SessionInfo> {
        let sessions = self.sessions.read().await;
        sessions.values().cloned().collect()
    }
    
    /// Clean up ended sessions older than the specified duration
    pub async fn cleanup_old_sessions(&mut self, max_age: chrono::Duration) -> sinex_core::Result<usize> {
        let cutoff_time = chrono::Utc::now() - max_age;
        let mut sessions = self.sessions.write().await;
        
        let initial_count = sessions.len();
        
        sessions.retain(|_, session| {
            if session.status == SessionStatus::Ended {
                if let Some(end_time) = session.end_time {
                    end_time > cutoff_time
                } else {
                    // Session marked as ended but no end time - keep for now
                    true
                }
            } else {
                // Keep active sessions
                true
            }
        });
        
        let removed_count = initial_count - sessions.len();
        
        if removed_count > 0 {
            info!("Cleaned up {} old sessions", removed_count);
        }
        
        Ok(removed_count)
    }
    
    /// Mark inactive sessions based on last activity
    pub async fn mark_inactive_sessions(&mut self, inactivity_threshold: chrono::Duration) -> sinex_core::Result<usize> {
        let cutoff_time = chrono::Utc::now() - inactivity_threshold;
        let mut sessions = self.sessions.write().await;
        
        let mut marked_count = 0;
        
        for session in sessions.values_mut() {
            if session.status == SessionStatus::Active && session.last_activity < cutoff_time {
                session.status = SessionStatus::Inactive;
                marked_count += 1;
                
                debug!(
                    session_id = %session.session_id,
                    last_activity = %session.last_activity,
                    "Marked session as inactive"
                );
            }
        }
        
        if marked_count > 0 {
            info!("Marked {} sessions as inactive", marked_count);
        }
        
        Ok(marked_count)
    }
    
    /// Update session activity timestamp
    pub async fn update_activity(&mut self, session_id: &str) -> sinex_core::Result<()> {
        let mut sessions = self.sessions.write().await;
        
        if let Some(session) = sessions.get_mut(session_id) {
            session.last_activity = chrono::Utc::now();
            
            // Reactivate inactive sessions on activity
            if session.status == SessionStatus::Inactive {
                session.status = SessionStatus::Active;
                debug!("Reactivated session {}", session_id);
            }
        }
        
        Ok(())
    }
    
    fn capture_relevant_env_vars() -> HashMap<String, String> {
        let mut env_vars = HashMap::new();
        
        // Capture important environment variables
        let important_vars = [
            "USER", "HOME", "PATH", "SHELL", "TERM", "TERM_PROGRAM",
            "PWD", "OLDPWD", "SHLVL", "HISTFILE", "HISTSIZE",
            "TMUX", "STY", "SSH_CLIENT", "SSH_TTY",
        ];
        
        for var in &important_vars {
            if let Ok(value) = std::env::var(var) {
                env_vars.insert(var.to_string(), value);
            }
        }
        
        env_vars
    }
}

impl Default for SessionTracker {
    fn default() -> Self {
        Self::new()
    }
}