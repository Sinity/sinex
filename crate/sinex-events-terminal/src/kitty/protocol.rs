/// Low-level Kitty socket protocol communication
/// 
/// This module handles the framing, sending, and parsing of commands
/// over the Kitty socket protocol. It provides a clean abstraction
/// over the raw socket communication.

use sinex_core::{CoreError, ErrorContext, Result};
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tracing::{debug, warn};

/// Kitty socket protocol handler
pub struct KittyProtocol {
    socket_path: Option<String>,
}

impl KittyProtocol {
    pub fn new() -> Self {
        Self {
            socket_path: None,
        }
    }

    /// Discover and connect to a Kitty socket
    pub async fn discover_socket(&mut self) -> Result<String> {
        let socket_path = Self::find_kitty_socket().await?;
        
        // Test connection
        if Self::test_connection(&socket_path).await.is_ok() {
            self.socket_path = Some(socket_path.clone());
            Ok(socket_path)
        } else {
            Err(ErrorContext::new(CoreError::Io("Failed to connect to discovered Kitty socket".to_string()))
                .with_operation("discover_socket")
                .with_context("socket_path", socket_path)
                .build())
        }
    }

    /// Send a command to Kitty and get the response
    pub async fn send_command(&self, command: serde_json::Value) -> Result<serde_json::Value> {
        let socket_path = self.socket_path
            .as_ref()
            .ok_or_else(|| ErrorContext::new(CoreError::Configuration("No Kitty socket configured".to_string()))
                .with_operation("send_command")
                .build())?;

        let mut stream = UnixStream::connect(socket_path)
            .await
            .map_err(|e| ErrorContext::new(CoreError::Io(format!("Failed to connect to socket: {}", e)))
                .with_operation("send_command")
                .with_context("socket_path", socket_path)
                .build())?;

        let response = self.execute_command(&mut stream, command).await?;
        self.parse_response(&response).await
    }

    /// Get the current socket path
    pub fn socket_path(&self) -> Option<&str> {
        self.socket_path.as_deref()
    }

    /// Check if socket is available
    pub fn is_connected(&self) -> bool {
        self.socket_path.is_some()
    }

    /// Find available Kitty sockets in common locations
    async fn find_kitty_socket() -> Result<String> {
        let tmp_dir = std::env::var("SINEX_TMP_DIR").unwrap_or_else(|_| "/tmp".to_string());
        let current_uid = nix::unistd::getuid();
        
        let socket_candidates = vec![
            format!("{}/kitty_socket_{}", tmp_dir, std::process::id()),
            format!("{}/kitty-{}.sock", tmp_dir, whoami::username()),
            format!("/run/user/{}/kitty.sock", current_uid),
            format!("{}/kitty.sock", tmp_dir),
        ];

        for candidate in &socket_candidates {
            if Path::new(candidate).exists() {
                debug!("Found potential Kitty socket: {}", candidate);
                return Ok(candidate.clone());
            }
        }

        Err(ErrorContext::new(CoreError::Io("No accessible Kitty socket found".to_string()))
            .with_operation("find_kitty_socket")
            .with_context("attempted_paths", format!("{:?}", socket_candidates))
            .build())
    }

    /// Test if we can connect to a socket
    async fn test_connection(socket_path: &str) -> Result<()> {
        UnixStream::connect(socket_path)
            .await
            .map_err(|e| ErrorContext::new(CoreError::Io(format!("Connection test failed: {}", e)))
                .with_operation("test_connection")
                .with_context("socket_path", socket_path)
                .build())?;
        Ok(())
    }

    /// Execute a command over the socket
    async fn execute_command(&self, stream: &mut UnixStream, command: serde_json::Value) -> Result<String> {
        let cmd_str = command.to_string();
        let framed_cmd = self.frame_command(&cmd_str);

        // Send command
        stream.write_all(framed_cmd.as_bytes()).await
            .map_err(|e| ErrorContext::new(CoreError::Io(format!("Failed to write command: {}", e)))
                .with_operation("execute_command")
                .build())?;
        
        stream.flush().await
            .map_err(|e| ErrorContext::new(CoreError::Io(format!("Failed to flush stream: {}", e)))
                .with_operation("execute_command")
                .build())?;

        // Read response
        let mut response_buffer = Vec::new();
        stream.read_to_end(&mut response_buffer).await
            .map_err(|e| ErrorContext::new(CoreError::Io(format!("Failed to read response: {}", e)))
                .with_operation("execute_command")
                .build())?;

        String::from_utf8(response_buffer)
            .map_err(|e| ErrorContext::new(CoreError::Serialization(format!("Invalid UTF-8 in response: {}", e)))
                .with_operation("execute_command")
                .build())
    }

    /// Frame a command with Kitty protocol markers
    fn frame_command(&self, cmd_str: &str) -> String {
        format!("\x1bP@kitty-cmd{}\x1b\\", cmd_str)
    }

    /// Parse a framed response from Kitty
    async fn parse_response(&self, response_str: &str) -> Result<serde_json::Value> {
        // Extract JSON from framed response
        if let Some(start) = response_str.find('{') {
            if let Some(end) = response_str.rfind('}') {
                let json_str = &response_str[start..=end];
                return sinex_core::parse_json_value(json_str, "Kitty response", "parse_response");
            }
        }

        // If no JSON found, log the response for debugging
        warn!("Could not parse Kitty response as JSON. Response preview: {}", 
            response_str.chars().take(100).collect::<String>());

        Err(ErrorContext::new(CoreError::Serialization("Could not parse Kitty response as JSON".to_string()))
            .with_operation("parse_response")
            .with_context("response_preview", response_str.chars().take(100).collect::<String>())
            .build())
    }
}

impl Default for KittyProtocol {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_command() {
        let protocol = KittyProtocol::new();
        let cmd = r#"{"cmd": "ls"}"#;
        let framed = protocol.frame_command(cmd);
        assert_eq!(framed, "\x1bP@kitty-cmd{\"cmd\": \"ls\"}\x1b\\");
    }

    #[test]
    fn test_new_protocol() {
        let protocol = KittyProtocol::new();
        assert!(!protocol.is_connected());
        assert!(protocol.socket_path().is_none());
    }
}