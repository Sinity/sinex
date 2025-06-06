use serde_json::Value;
use std::collections::{HashMap, HashSet};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AssumptionError {
    #[error("Field signature mismatch: {confidence}% confidence this is wrong event type")]
    SignatureMismatch { confidence: f64 },
    
    #[error("Cross-source contamination: payload contains fields from {other_source}")]
    CrossSourceContamination { other_source: String },
    
    #[error("Missing critical fields: {missing:?}")]
    MissingCriticalFields { missing: Vec<String> },
    
    #[error("Suspicious field combination: {reason}")]
    SuspiciousFields { reason: String },
}

/// Detects assumption mismatches in event payloads
pub struct AssumptionDetector {
    /// Known field signatures for each source/event_type
    signatures: HashMap<(String, String), FieldSignature>,
    
    /// Fields that are unique to specific sources
    source_specific_fields: HashMap<String, HashSet<String>>,
}

#[derive(Debug, Clone)]
struct FieldSignature {
    required_fields: HashSet<String>,
    optional_fields: HashSet<String>,
    forbidden_fields: HashSet<String>,
}

impl AssumptionDetector {
    pub fn new() -> Self {
        let mut detector = Self {
            signatures: HashMap::new(),
            source_specific_fields: HashMap::new(),
        };
        
        detector.init_signatures();
        detector
    }
    
    /// Check if an event's payload matches its declared type
    pub fn check_assumptions(
        &self,
        source: &str,
        event_type: &str,
        payload: &Value,
    ) -> Result<(), AssumptionError> {
        // Get actual fields from payload
        let actual_fields = self.extract_fields(payload);
        
        // Check 1: Field signature matching
        if let Some(signature) = self.signatures.get(&(source.to_string(), event_type.to_string())) {
            let missing_required: Vec<_> = signature.required_fields
                .difference(&actual_fields)
                .cloned()
                .collect();
                
            if !missing_required.is_empty() {
                return Err(AssumptionError::MissingCriticalFields { 
                    missing: missing_required 
                });
            }
            
            let forbidden_present: Vec<_> = signature.forbidden_fields
                .intersection(&actual_fields)
                .cloned()
                .collect();
                
            if !forbidden_present.is_empty() {
                return Err(AssumptionError::SuspiciousFields {
                    reason: format!("Contains forbidden fields: {:?}", forbidden_present)
                });
            }
        }
        
        // Check 2: Cross-source contamination
        for (other_source, specific_fields) in &self.source_specific_fields {
            if other_source != source {
                let contamination: Vec<_> = specific_fields
                    .intersection(&actual_fields)
                    .cloned()
                    .collect();
                    
                if !contamination.is_empty() {
                    // Calculate confidence that this is wrong
                    let confidence = (contamination.len() as f64 / actual_fields.len() as f64) * 100.0;
                    
                    if confidence > 50.0 {
                        return Err(AssumptionError::CrossSourceContamination {
                            other_source: other_source.clone()
                        });
                    }
                }
            }
        }
        
        // Check 3: Field pattern analysis
        let signature_confidence = self.calculate_signature_confidence(source, event_type, &actual_fields);
        if signature_confidence < 30.0 {
            return Err(AssumptionError::SignatureMismatch { 
                confidence: 100.0 - signature_confidence 
            });
        }
        
        Ok(())
    }
    
    /// Suggest the most likely correct source/event_type based on payload
    pub fn suggest_correct_type(&self, payload: &Value) -> Option<(String, String, f64)> {
        let actual_fields = self.extract_fields(payload);
        let mut best_match = None;
        let mut best_score = 0.0;
        
        for ((source, event_type), signature) in &self.signatures {
            let score = self.calculate_field_match_score(&actual_fields, signature);
            
            if score > best_score {
                best_score = score;
                best_match = Some((source.clone(), event_type.clone(), score));
            }
        }
        
        best_match.filter(|(_, _, score)| *score > 50.0)
    }
    
    fn extract_fields(&self, payload: &Value) -> HashSet<String> {
        payload.as_object()
            .map(|obj| obj.keys().cloned().collect())
            .unwrap_or_default()
    }
    
    fn calculate_signature_confidence(
        &self,
        source: &str,
        event_type: &str,
        actual_fields: &HashSet<String>,
    ) -> f64 {
        if let Some(signature) = self.signatures.get(&(source.to_string(), event_type.to_string())) {
            let required_present = signature.required_fields
                .intersection(actual_fields)
                .count();
            let optional_present = signature.optional_fields
                .intersection(actual_fields)
                .count();
            let forbidden_present = signature.forbidden_fields
                .intersection(actual_fields)
                .count();
                
            let total_expected = signature.required_fields.len() + signature.optional_fields.len();
            let total_present = required_present + optional_present;
            
            if total_expected == 0 {
                return 0.0;
            }
            
            let base_score = (total_present as f64 / total_expected as f64) * 100.0;
            let penalty = forbidden_present as f64 * 20.0;
            
            (base_score - penalty).max(0.0)
        } else {
            0.0
        }
    }
    
    fn calculate_field_match_score(
        &self,
        actual_fields: &HashSet<String>,
        signature: &FieldSignature,
    ) -> f64 {
        let required_match = signature.required_fields
            .intersection(actual_fields)
            .count() as f64 / signature.required_fields.len().max(1) as f64;
            
        let optional_match = if !signature.optional_fields.is_empty() {
            signature.optional_fields
                .intersection(actual_fields)
                .count() as f64 / signature.optional_fields.len() as f64
        } else {
            0.0
        };
        
        let forbidden_penalty = signature.forbidden_fields
            .intersection(actual_fields)
            .count() as f64 * 0.5;
            
        ((required_match * 70.0 + optional_match * 30.0) - forbidden_penalty * 100.0).max(0.0)
    }
    
    fn init_signatures(&mut self) {
        use crate::{sources, event_type_constants};
        
        // Filesystem signatures
        self.signatures.insert(
            (sources::FILESYSTEM.to_string(), event_type_constants::filesystem::FILE_CREATED.to_string()),
            FieldSignature {
                required_fields: ["path", "size"].iter().map(|s| s.to_string()).collect(),
                optional_fields: ["permissions", "owner", "group", "created_at"].iter().map(|s| s.to_string()).collect(),
                forbidden_fields: ["window", "workspace", "pid", "command"].iter().map(|s| s.to_string()).collect(),
            }
        );
        
        self.signatures.insert(
            (sources::FILESYSTEM.to_string(), event_type_constants::filesystem::FILE_MODIFIED.to_string()),
            FieldSignature {
                required_fields: ["path"].iter().map(|s| s.to_string()).collect(),
                optional_fields: ["old_size", "new_size", "modification_type", "content_hash"].iter().map(|s| s.to_string()).collect(),
                forbidden_fields: ["window", "workspace", "exit_code"].iter().map(|s| s.to_string()).collect(),
            }
        );
        
        // Hyprland signatures
        self.signatures.insert(
            (sources::HYPRLAND.to_string(), event_type_constants::hyprland::WINDOW_FOCUSED.to_string()),
            FieldSignature {
                required_fields: ["window"].iter().map(|s| s.to_string()).collect(),
                optional_fields: ["workspace", "class", "title", "pid"].iter().map(|s| s.to_string()).collect(),
                forbidden_fields: ["path", "size", "permissions", "command"].iter().map(|s| s.to_string()).collect(),
            }
        );
        
        self.signatures.insert(
            (sources::HYPRLAND.to_string(), event_type_constants::hyprland::WORKSPACE_CHANGED.to_string()),
            FieldSignature {
                required_fields: ["workspace"].iter().map(|s| s.to_string()).collect(),
                optional_fields: ["previous_workspace", "monitor"].iter().map(|s| s.to_string()).collect(),
                forbidden_fields: ["path", "size", "command", "exit_code"].iter().map(|s| s.to_string()).collect(),
            }
        );
        
        // Terminal signatures
        self.signatures.insert(
            (sources::TERMINAL_KITTY.to_string(), event_type_constants::terminal::COMMAND_EXECUTED.to_string()),
            FieldSignature {
                required_fields: ["command"].iter().map(|s| s.to_string()).collect(),
                optional_fields: ["exit_code", "duration", "working_directory", "session_id"].iter().map(|s| s.to_string()).collect(),
                forbidden_fields: ["window", "size", "permissions"].iter().map(|s| s.to_string()).collect(),
            }
        );
        
        // Source-specific fields
        self.source_specific_fields.insert(
            sources::FILESYSTEM.to_string(),
            ["path", "size", "permissions", "inode", "blocks"].iter().map(|s| s.to_string()).collect()
        );
        
        self.source_specific_fields.insert(
            sources::HYPRLAND.to_string(),
            ["window", "workspace", "monitor", "fullscreen", "floating"].iter().map(|s| s.to_string()).collect()
        );
        
        self.source_specific_fields.insert(
            sources::TERMINAL_KITTY.to_string(),
            ["command", "exit_code", "shell", "terminal_id"].iter().map(|s| s.to_string()).collect()
        );
    }
}

impl Default for AssumptionDetector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    
    #[test]
    fn test_correct_assumptions() {
        let detector = AssumptionDetector::new();
        
        // Correct filesystem event
        let result = detector.check_assumptions(
            "filesystem",
            "file_created",
            &json!({
                "path": "/test.txt",
                "size": 1024,
                "permissions": "644"
            })
        );
        assert!(result.is_ok());
    }
    
    #[test]
    fn test_cross_source_contamination() {
        let detector = AssumptionDetector::new();
        
        // Filesystem event with hyprland fields
        let result = detector.check_assumptions(
            "filesystem",
            "file_created",
            &json!({
                "path": "/test.txt",
                "size": 1024,
                "window": "terminal",  // Wrong!
                "workspace": 2         // Wrong!
            })
        );
        
        // The forbidden fields check catches this before cross-source contamination check
        assert!(matches!(result, Err(AssumptionError::SuspiciousFields { .. })));
    }
    
    #[test]
    fn test_suggest_correct_type() {
        let detector = AssumptionDetector::new();
        
        // Payload that looks like hyprland but declared as filesystem
        let suggestion = detector.suggest_correct_type(&json!({
            "window": "firefox",
            "workspace": 1,
            "class": "Firefox"
        }));
        
        assert!(suggestion.is_some());
        let (source, event_type, confidence) = suggestion.unwrap();
        assert_eq!(source, "hyprland");
        assert_eq!(event_type, "window_focused");
        assert!(confidence > 70.0);
    }
    
    #[test]
    fn test_missing_required_fields() {
        let detector = AssumptionDetector::new();
        
        let result = detector.check_assumptions(
            "filesystem",
            "file_created",
            &json!({
                "path": "/test.txt"
                // Missing required "size" field
            })
        );
        
        assert!(matches!(result, Err(AssumptionError::MissingCriticalFields { .. })));
    }
}