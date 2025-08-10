#!/usr/bin/env python3
"""Optimize domain.rs newtype definitions to single lines."""

import re

def optimize_define_string_type(content):
    """Convert multi-line define_string_type! to single line."""
    
    # Pattern to match define_string_type! with doc comment and struct name
    pattern = r'define_string_type!\s*\{\s*///\s*([^\n]+)\s*(\w+)\s*\}'
    
    # Replace with single-line format
    def replace_func(match):
        doc = match.group(1)
        name = match.group(2)
        return f'define_string_type!(#[doc = "{doc}"] {name});'
    
    return re.sub(pattern, replace_func, content, flags=re.MULTILINE)

def optimize_define_validated_string_type(content):
    """Convert multi-line define_validated_string_type! to single line."""
    
    # Pattern to match define_validated_string_type! with doc comment and struct name
    pattern = r'define_validated_string_type!\s*\{\s*///\s*([^\n]+)\s*(\w+)\s*\}'
    
    # Replace with single-line format  
    def replace_func(match):
        doc = match.group(1)
        name = match.group(2)
        return f'define_validated_string_type!(#[doc = "{doc}"] {name});'
    
    return re.sub(pattern, replace_func, content, flags=re.MULTILINE)

def main():
    # Read the file
    with open('/realm/project/sinex/crate/lib/sinex-core/src/types/domain.rs', 'r') as f:
        content = f.read()
    
    # Update macro syntax to accept single-line format
    macro_def_pattern = r'macro_rules! define_string_type \{.*?\n\}'
    macro_def_replace = '''macro_rules! define_string_type {
    ($(#[$meta:meta])* $name:ident) => {'''
    
    # First update the macro definition to accept the new syntax
    content = re.sub(
        r'macro_rules! define_string_type \{\s*\(\s*\$\(\#\[\$meta:meta\]\)\*\s*\$name:ident\s*\) =>',
        'macro_rules! define_string_type {\n    ($(#[$meta:meta])* $name:ident) =>',
        content
    )
    
    content = re.sub(
        r'macro_rules! define_validated_string_type \{\s*\(\s*\$\(\#\[\$meta:meta\]\)\*\s*\$name:ident\s*\) =>',
        'macro_rules! define_validated_string_type {\n    ($(#[$meta:meta])* $name:ident) =>',
        content
    )
    
    # Optimize the invocations
    content = optimize_define_string_type(content)
    content = optimize_define_validated_string_type(content)
    
    # Write the optimized content back
    with open('/realm/project/sinex/crate/lib/sinex-core/src/types/domain.rs', 'w') as f:
        f.write(content)
    
    print("Optimized domain.rs newtype definitions to single lines")

if __name__ == '__main__':
    main()