import os
import toml

workspace_root = os.getcwd()
xtask_path = os.path.join(workspace_root, "xtask")
primitives_path = os.path.join(workspace_root, "crate/lib/sinex-primitives")

cargo_files = [
    "./crate/lib/sinex-core/Cargo.toml",
    "./crate/lib/sinex-macros/Cargo.toml",
    "./crate/lib/sinex-schema/Cargo.toml",
    "./crate/lib/sinex-services/Cargo.toml",
    "./crate/lib/sinex-processor-runtime/Cargo.toml",
    "./crate/lib/sinex-node-sdk/Cargo.toml",
    "./crate/lib/sinex-primitives/Cargo.toml",
    "./crate/core/sinex-gateway/Cargo.toml",
    "./crate/core/sinex-ingestd/Cargo.toml",
    "./crate/nodes/sinex-analytics-automaton/Cargo.toml",
    "./crate/nodes/sinex-content-automaton/Cargo.toml",
    "./crate/nodes/sinex-document-ingestor/Cargo.toml",
    "./crate/nodes/sinex-pkm-automaton/Cargo.toml",
    "./crate/nodes/sinex-search-automaton/Cargo.toml",
    "./crate/nodes/sinex-terminal-command-canonicalizer/Cargo.toml",
    "./crate/nodes/sinex-desktop-ingestor/Cargo.toml",
    "./crate/nodes/sinex-terminal-ingestor/Cargo.toml",
    "./crate/nodes/sinex-system-ingestor/Cargo.toml",
    "./crate/nodes/sinex-fs-ingestor/Cargo.toml",
    "./crate/nodes/sinex-health-automaton/Cargo.toml",
    "./cli/sinex-cli/Cargo.toml",
    "./cli/sinexctl/Cargo.toml",
    "./tests/e2e/Cargo.toml",
]


def get_relative_path(from_dir, to_path):
    return os.path.relpath(to_path, from_dir)


for cf in cargo_files:
    abs_cf = os.path.abspath(cf)
    if not os.path.exists(abs_cf):
        print(f"Skipping {cf} - not found")
        continue

    with open(abs_cf, "r") as f:
        data = toml.load(f)

    parent_dir = os.path.dirname(abs_cf)

    if "dev-dependencies" not in data:
        data["dev-dependencies"] = {}

    # Add xtask
    package_name = data.get("package", {}).get("name")
    if not package_name:
        print(f"Skipping {cf} - no package name found")
        continue

    # Add xtask
    if package_name != "xtask" and package_name != "xtask-macros":
        rel_xtask = get_relative_path(parent_dir, xtask_path)
        data["dev-dependencies"]["xtask"] = {
            "path": rel_xtask,
            "default-features": False,
            "features": ["sandbox"],
        }

    # Add sinex-primitives if not itself
    if package_name != "sinex-primitives":
        rel_prim = get_relative_path(parent_dir, primitives_path)
        data["dev-dependencies"]["sinex-primitives"] = {
            "path": rel_prim,
            "features": ["testing"],
        }

    with open(abs_cf, "w") as f:
        toml.dump(data, f)
    print(f"Updated {cf}")
