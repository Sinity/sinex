#\!/usr/bin/env python3
import os
import toml
import json

def get_sinex_deps(cargo_path):
    """Extract sinex-* dependencies from Cargo.toml"""
    try:
        with open(cargo_path, 'r') as f:
            cargo = toml.load(f)
        
        deps = {}
        if 'dependencies' in cargo:
            for dep, info in cargo['dependencies'].items():
                if dep.startswith('sinex-'):
                    optional = False
                    if isinstance(info, dict):
                        optional = info.get('optional', False)
                    deps[dep] = {'optional': optional}
        
        if 'features' in cargo:
            deps['_features'] = cargo['features']
            
        return deps
    except:
        return {}

# Find all Cargo.toml files
crates = {}
for root, dirs, files in os.walk('crate'):
    if 'Cargo.toml' in files:
        path = os.path.join(root, 'Cargo.toml')
        crate_name = os.path.basename(root)
        if crate_name.startswith('sinex'):
            crates[crate_name] = get_sinex_deps(path)

# Generate mermaid diagram
print("```mermaid")
print("graph TD")
print("    %% Crate dependency graph")
print("    ")

# Define crate types with colors
print("    %% Core crates")
print("    sinex-types[sinex-types]:::core")
print("    sinex-events[sinex-events]:::core")
print("    sinex-db[sinex-db]:::core")
print("    ")
print("    %% Facade")
print("    sinex[sinex - FACADE]:::facade")
print("    ")
print("    %% Test utils")
print("    sinex-test-utils[sinex-test-utils]:::test")
print("    ")

# Add dependencies
for crate, deps in crates.items():
    for dep, info in deps.items():
        if dep == '_features':
            continue
        if info['optional']:
            print(f"    {crate} -.->|optional| {dep}")
        else:
            print(f"    {crate} --> {dep}")

print("    ")
print("    %% Circular dependency")
print("    sinex-test-utils ==>|WANTS| sinex")
print("    ")

# Add styling
print("    classDef core fill:#f9f,stroke:#333,stroke-width:2px")
print("    classDef facade fill:#bbf,stroke:#333,stroke-width:4px")
print("    classDef test fill:#bfb,stroke:#333,stroke-width:2px")
print("```")

# Also output JSON for further processing
output = {
    'crates': crates,
    'circular_dependency': {
        'sinex': {'depends_on': 'sinex-test-utils', 'optional': True, 'via_feature': 'test'},
        'sinex-test-utils': {'depends_on': 'sinex', 'features_needed': ['standard']}
    }
}

print("\n\n## Dependency Structure (JSON)")
print(json.dumps(output, indent=2))
