use unicode_normalization::UnicodeNormalization;
use crate::{ValidationError, monitoring};

pub struct UnicodeNormalizer {
    reject_invisible: bool,
    reject_rtl_override: bool,
    reject_homoglyphs: bool,
    #[allow(dead_code)]
    max_grapheme_clusters: usize,
}

impl Default for UnicodeNormalizer {
    fn default() -> Self {
        Self {
            reject_invisible: true,
            reject_rtl_override: true,
            reject_homoglyphs: true,
            max_grapheme_clusters: 10000,
        }
    }
}

impl UnicodeNormalizer {
    pub fn normalize(&self, input: &str) -> Result<String, ValidationError> {
        // First, check for dangerous characters before normalization
        self.check_dangerous_chars(input)?;
        
        // Normalize to NFC (Canonical Decomposition, followed by Canonical Composition)
        let normalized: String = input.nfc().collect();
        
        // Check again after normalization (some attacks use normalization itself)
        self.check_dangerous_chars(&normalized)?;
        
        // Check for homoglyphs if enabled
        if self.reject_homoglyphs {
            self.check_homoglyphs(&normalized)?;
        }
        
        Ok(normalized)
    }
    
    fn check_dangerous_chars(&self, s: &str) -> Result<(), ValidationError> {
        for ch in s.chars() {
            // Zero-width characters
            if self.reject_invisible && matches!(ch,
                '\u{200B}' |  // Zero-width space
                '\u{200C}' |  // Zero-width non-joiner  
                '\u{200D}' |  // Zero-width joiner
                '\u{FEFF}' |  // Zero-width no-break space
                '\u{2060}' |  // Word joiner
                '\u{180E}'    // Mongolian vowel separator
            ) {
                monitoring::log_security_event(monitoring::SecurityEvent::UnicodeNormalizationBypass {
                    input: format!("Contains zero-width character: U+{:04X}", ch as u32),
                });
                return Err(ValidationError::UnicodeError(
                    format!("Zero-width character detected: U+{:04X}", ch as u32)
                ));
            }
            
            // Right-to-left override characters
            if self.reject_rtl_override && matches!(ch,
                '\u{202A}' |  // Left-to-right embedding
                '\u{202B}' |  // Right-to-left embedding
                '\u{202C}' |  // Pop directional formatting
                '\u{202D}' |  // Left-to-right override
                '\u{202E}' |  // Right-to-left override
                '\u{2066}' |  // Left-to-right isolate
                '\u{2067}' |  // Right-to-left isolate
                '\u{2068}' |  // First strong isolate
                '\u{2069}' |  // Pop directional isolate
                '\u{200E}' |  // Left-to-right mark
                '\u{200F}'    // Right-to-left mark
            ) {
                monitoring::log_security_event(monitoring::SecurityEvent::UnicodeNormalizationBypass {
                    input: format!("Contains RTL override: U+{:04X}", ch as u32),
                });
                return Err(ValidationError::UnicodeError(
                    format!("Direction control character detected: U+{:04X}", ch as u32)
                ));
            }
        }
        
        Ok(())
    }
    
    fn check_homoglyphs(&self, s: &str) -> Result<(), ValidationError> {
        // Common homoglyphs that can be used for attacks
        const HOMOGLYPH_PAIRS: &[(char, char, &str)] = &[
            // Latin vs Cyrillic
            ('a', 'а', "Cyrillic"),
            ('e', 'е', "Cyrillic"),
            ('o', 'о', "Cyrillic"),
            ('p', 'р', "Cyrillic"),
            ('c', 'с', "Cyrillic"),
            ('x', 'х', "Cyrillic"),
            ('y', 'у', "Cyrillic"),
            ('A', 'А', "Cyrillic"),
            ('B', 'В', "Cyrillic"),
            ('E', 'Е', "Cyrillic"),
            ('H', 'Н', "Cyrillic"),
            ('K', 'К', "Cyrillic"),
            ('M', 'М', "Cyrillic"),
            ('O', 'О', "Cyrillic"),
            ('P', 'Р', "Cyrillic"),
            ('T', 'Т', "Cyrillic"),
            ('X', 'Х', "Cyrillic"),
            
            // Latin vs Greek
            ('A', 'Α', "Greek"),
            ('B', 'Β', "Greek"),
            ('E', 'Ε', "Greek"),
            ('H', 'Η', "Greek"),
            ('I', 'Ι', "Greek"),
            ('K', 'Κ', "Greek"),
            ('M', 'Μ', "Greek"),
            ('N', 'Ν', "Greek"),
            ('O', 'Ο', "Greek"),
            ('P', 'Ρ', "Greek"),
            ('T', 'Τ', "Greek"),
            ('X', 'Χ', "Greek"),
            ('Y', 'Υ', "Greek"),
            
            // Common confusables
            ('0', 'O', "Letter/Digit"),
            ('0', 'o', "Letter/Digit"),
            ('1', 'l', "Letter/Digit"),
            ('1', 'I', "Letter/Digit"),
        ];
        
        // Check for mixed scripts (e.g., Latin + Cyrillic in same identifier)
        let mut scripts = std::collections::HashSet::new();
        for ch in s.chars() {
            if ch.is_alphabetic() {
                // Simplified script detection
                let script = if ch <= '\u{024F}' {
                    "Latin"
                } else if ch >= '\u{0400}' && ch <= '\u{04FF}' {
                    "Cyrillic"  
                } else if ch >= '\u{0370}' && ch <= '\u{03FF}' {
                    "Greek"
                } else {
                    "Other"
                };
                scripts.insert(script);
            }
        }
        
        if scripts.len() > 1 && scripts.contains(&"Latin") {
            // Mixed scripts with Latin - suspicious
            monitoring::log_security_event(monitoring::SecurityEvent::UnicodeNormalizationBypass {
                input: format!("Mixed scripts detected: {:?}", scripts),
            });
            return Err(ValidationError::UnicodeError(
                format!("Mixed scripts detected: {:?}", scripts)
            ));
        }
        
        Ok(())
    }
    
    pub fn is_visually_similar(a: &str, b: &str) -> bool {
        if a == b {
            return true;
        }
        
        // Normalize both
        let a_norm: String = a.nfc().collect();
        let b_norm: String = b.nfc().collect();
        
        if a_norm == b_norm {
            return true;
        }
        
        // Check if they differ only by homoglyphs
        if a_norm.len() != b_norm.len() {
            return false;
        }
        
        for (char_a, char_b) in a_norm.chars().zip(b_norm.chars()) {
            if char_a != char_b {
                // Check if they're known homoglyphs
                let is_homoglyph = match (char_a, char_b) {
                    ('a', 'а') | ('а', 'a') => true,
                    ('e', 'е') | ('е', 'e') => true,
                    ('o', 'о') | ('о', 'o') => true,
                    ('0', 'O') | ('O', '0') => true,
                    ('1', 'l') | ('l', '1') => true,
                    _ => false,
                };
                
                if !is_homoglyph {
                    return false;
                }
            }
        }
        
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_zero_width_rejection() {
        let normalizer = UnicodeNormalizer::default();
        
        assert!(normalizer.normalize("normal text").is_ok());
        assert!(normalizer.normalize("text\u{200B}with\u{200B}zero\u{200B}width").is_err());
        assert!(normalizer.normalize("text\u{FEFF}with\u{FEFF}bom").is_err());
    }
    
    #[test]
    fn test_rtl_override_rejection() {
        let normalizer = UnicodeNormalizer::default();
        
        assert!(normalizer.normalize("file\u{202E}txt.exe").is_err());
        assert!(normalizer.normalize("\u{202D}important.doc").is_err());
    }
    
    #[test]
    fn test_mixed_script_detection() {
        let normalizer = UnicodeNormalizer::default();
        
        assert!(normalizer.normalize("admin").is_ok()); // Pure Latin
        assert!(normalizer.normalize("админ").is_ok()); // Pure Cyrillic
        assert!(normalizer.normalize("аdmin").is_err()); // Mixed - 'а' is Cyrillic
    }
    
    #[test]
    fn test_visual_similarity() {
        assert!(UnicodeNormalizer::is_visually_similar("admin", "admin"));
        assert!(UnicodeNormalizer::is_visually_similar("аdmin", "admin")); // Cyrillic 'а'
        assert!(!UnicodeNormalizer::is_visually_similar("admin", "administrator"));
    }
}