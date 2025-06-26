#!/usr/bin/env python3
"""
FORGE: File I/O Error Transformation Script
Transforms HIGH priority file I/O error patterns to structured ErrorContext
"""

import re
import os
import sys
from pathlib import Path
from typing import List, Tuple, Dict


class FileErrorTransformer:
    """Transforms file I/O related string-based errors to structured ErrorContext."""
    
    def __init__(self):
        self.patterns = [
            # File open errors with path
            {
                'pattern': r'CoreError::Other\(format!\("Failed to open \{:\?\}: \{\}", ([^,]+), ([^)]+)\)\)',
                'replacement': r'CoreError::io_error(&\1)\n    .with_operation("open")\n    .with_source(\2)\n    .build()',
                'description': 'File open failure with path'
            },
            # File read errors with path
            {
                'pattern': r'CoreError::Other\(format!\("Failed to read file \{:\?\}: \{\}", ([^,]+), ([^)]+)\)\)',
                'replacement': r'CoreError::io_error(&\1)\n    .with_operation("read")\n    .with_source(\2)\n    .build()',
                'description': 'File read failure with path'
            },
            # File write errors with path
            {
                'pattern': r'CoreError::Other\(format!\("Failed to write to \{:\?\}: \{\}", ([^,]+), ([^)]+)\)\)',
                'replacement': r'CoreError::io_error(&\1)\n    .with_operation("write")\n    .with_source(\2)\n    .build()',
                'description': 'File write failure with path'
            },
            # File creation errors
            {
                'pattern': r'CoreError::Other\(format!\("Failed to create file \{:\?\}: \{\}", ([^,]+), ([^)]+)\)\)',
                'replacement': r'CoreError::io_error(&\1)\n    .with_operation("create")\n    .with_source(\2)\n    .build()',
                'description': 'File creation failure'
            },
            # Directory creation errors
            {
                'pattern': r'CoreError::Other\(format!\("Failed to create directory \{:\?\}: \{\}", ([^,]+), ([^)]+)\)\)',
                'replacement': r'CoreError::io_error(&\1)\n    .with_operation("mkdir")\n    .with_source(\2)\n    .build()',
                'description': 'Directory creation failure'
            },
            # File deletion errors
            {
                'pattern': r'CoreError::Other\(format!\("Failed to delete \{:\?\}: \{\}", ([^,]+), ([^)]+)\)\)',
                'replacement': r'CoreError::io_error(&\1)\n    .with_operation("delete")\n    .with_source(\2)\n    .build()',
                'description': 'File deletion failure'
            },
            # File move/rename errors
            {
                'pattern': r'CoreError::Other\(format!\("Failed to move \{:\?\} to \{:\?\}: \{\}", ([^,]+), ([^,]+), ([^)]+)\)\)',
                'replacement': r'CoreError::io_error(&\1)\n    .with_operation("move")\n    .with_context("target", \2.to_string())\n    .with_source(\3)\n    .build()',
                'description': 'File move/rename failure'
            },
            # File copy errors
            {
                'pattern': r'CoreError::Other\(format!\("Failed to copy \{:\?\} to \{:\?\}: \{\}", ([^,]+), ([^,]+), ([^)]+)\)\)',
                'replacement': r'CoreError::io_error(&\1)\n    .with_operation("copy")\n    .with_context("target", \2.to_string())\n    .with_source(\3)\n    .build()',
                'description': 'File copy failure'
            },
            # File metadata errors
            {
                'pattern': r'CoreError::Other\(format!\("Failed to read metadata for \{:\?\}: \{\}", ([^,]+), ([^)]+)\)\)',
                'replacement': r'CoreError::io_error(&\1)\n    .with_operation("metadata")\n    .with_source(\2)\n    .build()',
                'description': 'File metadata read failure'
            },
            # File permission errors
            {
                'pattern': r'CoreError::Other\(format!\("Permission denied accessing \{:\?\}: \{\}", ([^,]+), ([^)]+)\)\)',
                'replacement': r'CoreError::io_error(&\1)\n    .with_operation("access")\n    .with_context("error_type", "permission_denied".to_string())\n    .with_source(\2)\n    .build()',
                'description': 'File permission denied'
            },
            # Generic file I/O errors
            {
                'pattern': r'CoreError::Other\(format!\("File I/O error: \{\}", ([^)]+)\)\)',
                'replacement': r'CoreError::io_error("")\n    .with_operation("generic")\n    .with_source(\1)\n    .build()',
                'description': 'Generic file I/O error'
            },
        ]
        
        self.target_files = [
            "crate/sinex-events/src/asciinema.rs",
            "crate/sinex-events/src/filesystem.rs", 
            "crate/sinex-annex/src/lib.rs",
            "crate/sinex-annex/src/blob_manager.rs",
            "crate/sinex-collector/src/config.rs",
        ]
        
        self.stats = {
            'files_processed': 0,
            'patterns_found': 0,
            'transformations_applied': 0,
            'errors': []
        }

    def transform_file(self, file_path: str) -> bool:
        """Transform a single file's file I/O error patterns."""
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
                print(f"  ℹ️  No file I/O error patterns found")
                return False
                
        except Exception as e:
            error_msg = f"Error processing {file_path}: {e}"
            print(f"  ❌ {error_msg}")
            self.stats['errors'].append(error_msg)
            return False

    def run(self) -> bool:
        """Run the transformation on all target files."""
        print("🔧 FORGE: File I/O Error Transformation")
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
        
        # Show transformation examples
        if self.stats['transformations_applied'] > 0:
            print("\n💡 TRANSFORMATION EXAMPLES")
            print("=" * 30)
            print("BEFORE:")
            print('  CoreError::Other(format!("Failed to open {:?}: {}", path, e))')
            print()
            print("AFTER:")
            print('  CoreError::io_error(&path)')
            print('      .with_operation("open")')
            print('      .with_source(e)')
            print('      .build()')
        
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
    transformer = FileErrorTransformer()
    
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