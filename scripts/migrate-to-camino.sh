#!/usr/bin/env bash
# Script to migrate PathBuf and Path usage to camino's Utf8PathBuf and Utf8Path

set -euo pipefail

echo "=== Migrating to camino UTF-8 safe paths ==="

# Step 1: Add camino dependency to crates that need it
echo "1. Finding crates that use PathBuf or Path..."

CRATES_WITH_PATHS=$(find crate -name "*.rs" -type f | xargs grep -l "PathBuf\|use std::path::" | sed 's|/src/.*||' | sort -u)

for crate_dir in $CRATES_WITH_PATHS; do
    cargo_toml="$crate_dir/Cargo.toml"
    if [ -f "$cargo_toml" ]; then
        # Check if camino is already a dependency
        if ! grep -q "camino" "$cargo_toml"; then
            echo "Adding camino to $cargo_toml"
            # Find the [dependencies] section and add camino
            sed -i '/\[dependencies\]/a camino = { workspace = true }' "$cargo_toml"
        fi
    fi
done

# Step 2: Create migration patches
echo "2. Creating migration patches..."

# Common import replacements
cat > /tmp/camino_imports.patch << 'EOF'
# Replace std::path imports with camino
s/use std::path::{Path, PathBuf};/use camino::{Utf8Path, Utf8PathBuf};/g
s/use std::path::PathBuf;/use camino::Utf8PathBuf;/g
s/use std::path::Path;/use camino::Utf8Path;/g

# Replace type annotations
s/: PathBuf/: Utf8PathBuf/g
s/: &Path/: &Utf8Path/g
s/: Option<PathBuf>/: Option<Utf8PathBuf>/g
s/: Vec<PathBuf>/: Vec<Utf8PathBuf>/g
s/: &\[PathBuf\]/: &[Utf8PathBuf]/g

# Replace function signatures
s/-> PathBuf/-> Utf8PathBuf/g
s/-> &Path/-> &Utf8Path/g
s/-> Option<PathBuf>/-> Option<Utf8PathBuf>/g

# Replace struct fields
s/PathBuf,/Utf8PathBuf,/g
s/&Path,/&Utf8Path,/g

# Replace Path::new with Utf8Path::new
s/Path::new(/Utf8Path::new(/g
s/PathBuf::from(/Utf8PathBuf::from(/g
s/PathBuf::new(/Utf8PathBuf::new(/g

# Replace .to_path_buf() with .to_owned() for Utf8Path
s/\.to_path_buf()/\.to_owned()/g

# Replace dirs:: functions that return PathBuf
s/dirs::home_dir()/dirs::home_dir().map(|p| Utf8PathBuf::from_path_buf(p).expect("Home dir is not UTF-8"))/g
s/dirs::config_dir()/dirs::config_dir().map(|p| Utf8PathBuf::from_path_buf(p).expect("Config dir is not UTF-8"))/g
s/dirs::data_dir()/dirs::data_dir().map(|p| Utf8PathBuf::from_path_buf(p).expect("Data dir is not UTF-8"))/g
EOF

# Step 3: Apply patches to specific files
echo "3. Applying patches..."

# Start with sinex-types path_utils
echo "Migrating path_utils in sinex-types..."
cat > /tmp/path_utils_migration.patch << 'EOF'
--- a/crate/lib/sinex-types/src/lib.rs
+++ b/crate/lib/sinex-types/src/lib.rs
@@ -354,11 +354,11 @@
 /// Utility functions for working with paths
 pub mod path_utils {
 
-    use std::path::{Path, PathBuf};
+    use camino::{Utf8Path, Utf8PathBuf};
 
     /// Normalize a path by resolving . and .. components
-    pub fn normalize_path(path: &Path) -> PathBuf {
-        let mut components = vec![];
+    pub fn normalize_path(path: &Utf8Path) -> Utf8PathBuf {
+        let mut components: Vec<&str> = vec![];
         for component in path.components() {
             match component {
                 std::path::Component::CurDir => {}
@@ -372,7 +372,7 @@
                 _ => components.push(component.as_os_str().to_str().unwrap()),
             }
         }
-        PathBuf::from(components.join("/"))
+        Utf8PathBuf::from(components.join("/"))
     }
 }
EOF

# Apply the path_utils patch
patch -p1 < /tmp/path_utils_migration.patch || echo "Failed to apply path_utils patch"

# Step 4: Find and migrate specific patterns
echo "4. Finding specific patterns to migrate..."

# Files that need manual attention for Path conversions
echo "Files that may need manual conversion from Path to Utf8Path:"
find crate -name "*.rs" -type f | xargs grep -l "\.as_path()\|\.as_ref::<Path>()\|impl.*AsRef<Path>" || true

echo ""
echo "=== Migration Summary ==="
echo "1. Added camino dependency to crates that use paths"
echo "2. Created migration patches for common patterns"
echo "3. You should now:"
echo "   - Review and apply the patches manually"
echo "   - Handle any Path <-> Utf8Path conversions"
echo "   - Update any APIs that accept non-UTF8 paths"
echo ""
echo "Common conversions:"
echo "  - PathBuf::from(string) -> Utf8PathBuf::from(string)"
echo "  - path.to_path_buf() -> path.to_owned()"
echo "  - dirs::home_dir() -> dirs::home_dir().and_then(|p| Utf8PathBuf::from_path_buf(p).ok())"
echo ""
echo "For non-UTF8 paths, use:"
echo "  - Utf8PathBuf::from_path_buf(path_buf).expect(\"Path must be UTF-8\")"
echo "  - Utf8Path::from_path(path).expect(\"Path must be UTF-8\")"