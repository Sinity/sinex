#!/usr/bin/env python3
"""
FORGE: Configuration Error Transformation Script
Transforms HIGH priority configuration error patterns to structured ErrorContext
"""

import re
import os
import sys
from pathlib import Path
from typing import List, Tuple, Dict


class ConfigErrorTransformer:
    """Transforms configuration-related string-based errors to structured ErrorContext."""
    
    def __init__(self):
        self.patterns = [
            # Config file parsing errors
            {
                'pattern': r'CoreError::Other\(format!\("Failed to parse config file \{:\?\}: \{\}", ([^,]+), ([^)]+)\)\)',
                'replacement': r'CoreError::configuration()\n    .with_context("config_file", \1.to_string())\n    .with_operation("parse")\n    .with_source(\2)\n    .build()',
                'description': 'Config file parsing failure'
            },
            # Generic config parsing
            {
                'pattern': r'CoreError::Other\(format!\("Failed to parse config: \{\}", ([^)]+)\)\)',
                'replacement': r'CoreError::configuration()\n    .with_operation("parse")\n    .with_source(\1)\n    .build()',
                'description': 'Generic config parsing failure'
            },
            # Config validation errors
            {
                'pattern': r'CoreError::Other\(format!\("Invalid configuration: \{\}", ([^)]+)\)\)',
                'replacement': r'CoreError::configuration()\n    .with_operation("validate")\n    .with_source(\1)\n    .build()',
                'description': 'Config validation failure'
            },
            # Config validation with details
            {
                'pattern': r'CoreError::Other\(format!\("Config validation failed: \{\}", ([^)]+)\)\)',
                'replacement': r'CoreError::configuration()\n    .with_operation("validate")\n    .with_source(\1)\n    .build()',
                'description': 'Config validation failure with details'
            },
            # Config reload errors  
            {
                'pattern': r'CoreError::Other\(format!\("Failed to reload config: \{\}", ([^)]+)\)\)',
                'replacement': r'CoreError::configuration()\n    .with_operation("reload")\n    .with_source(\1)\n    .build()',
                'description': 'Config reload failure'
            },
            # Missing config file
            {
                'pattern': r'CoreError::Other\(format!\("Configuration file not found: \{:\?\}", ([^)]+)\)\)',
                'replacement': r'CoreError::configuration()\n    .with_context("config_file", \1.to_string())\n    .with_operation("locate")\n    .with_message("Configuration file not found")\n    .build()',
                'description': 'Missing config file'
            },
        ]
        
        self.target_files = [
            "crate/sinex-collector/src/config.rs",
            "crate/sinex-events/src/asciinema.rs", 
            "crate/sinex-events/src/hyprland.rs",
            "crate/sinex-events/src/terminal.rs",
        ]
        
        self.stats = {
            'files_processed': 0,
            'patterns_found': 0,
            'transformations_applied': 0,
            'errors': []
        }

    def transform_file(self, file_path: str) -> bool:
        """Transform a single file's config error patterns."""
        if not os.path.exists(file_path):
            print(f"⚠️  File not found: {file_path}")
            return False
            
        try:
            with open(file_path, 'r') as f:
                content = f.read()
                
            original_content = content
            transformations_in_file = 0
            
            # Apply each transformation pattern
            for pattern_info in self.patterns:
                pattern = pattern_info['pattern']
                replacement = pattern_info['replacement']
                description = pattern_info['description']
                
                matches = list(re.finditer(pattern, content, re.MULTILINE))
                if matches:
                    print(f"  📝 Found {len(matches)} instances of: {description}")
                    content = re.sub(pattern, replacement, content, flags=re.MULTILINE)
                    transformations_in_file += len(matches)
                    self.stats['patterns_found'] += len(matches)
            
            # Write back if changes were made
            if content != original_content:
                with open(file_path, 'w') as f:
                    f.write(content)
                    
                print(f"  ✅ Applied {transformations_in_file} transformations")
                self.stats['transformations_applied'] += transformations_in_file
                return True
            else:
                print(f"  ℹ️  No config error patterns found")
                return False
                
        except Exception as e:
            error_msg = f"Error processing {file_path}: {e}"
            print(f"  ❌ {error_msg}")
            self.stats['errors'].append(error_msg)
            return False

    def run(self) -> bool:
        """Run the transformation on all target files."""
        print("🔧 FORGE: Configuration Error Transformation")
        print("=" * 50)
        
        success = True
        files_modified = []
        
        for file_path in self.target_files:
            print(f"\n📁 Processing: {file_path}")
            self.stats['files_processed'] += 1
            
            if self.transform_file(file_path):
                files_modified.append(file_path)
        
        # Print summary
        print("\n" + "=" * 50)
        print("📊 TRANSFORMATION SUMMARY")
        print("=" * 50)
        print(f"Files processed: {self.stats['files_processed']}")
        print(f"Pattern instances found: {self.stats['patterns_found']}")
        print(f"Transformations applied: {self.stats['transformations_applied']}")
        print(f"Files modified: {len(files_modified)}")
        
        if files_modified:
            print("\n📝 Modified files:")
            for file_path in files_modified:
                print(f"  - {file_path}")
        
        if self.stats['errors']:
            print(f"\n❌ Errors encountered: {len(self.stats['errors'])}")
            for error in self.stats['errors']:
                print(f"  - {error}")
            success = False
        
        # Verification step
        print("\n🔍 VERIFICATION")
        print("=" * 20)
        print("Run these commands to verify:")
        print("  cargo check --workspace  # Check compilation")
        print("  cargo test               # Run tests") 
        print("  git diff                 # Review changes")
        
        return success and len(files_modified) > 0


def main():
    """Main entry point."""
    transformer = ConfigErrorTransformer()
    
    try:
        success = transformer.run()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Transformation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()