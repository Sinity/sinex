use std::collections::HashSet;
use std::fs;
use std::path::Path;

use color_eyre::eyre::{Context, Result, bail};
use serde::Deserialize;

use crate::config::workspace_root;

const REGISTRY_PATH: &str = "xtask/config/compatibility-witnesses.json";

#[derive(Debug, Deserialize)]
struct Registry {
    version: u32,
    witnesses: Vec<Witness>,
}

#[derive(Debug, Deserialize)]
struct Witness {
    id: String,
    contract: String,
    package: String,
    product_file: String,
    mutation_anchor: String,
    oracle_file: String,
    oracle_test: String,
    oracle_literal: String,
    authority: String,
}

pub(super) struct WitnessSummary {
    pub(super) registered: usize,
    pub(super) in_scope: usize,
}

pub(super) fn validate_registry(
    package_filter: Option<&str>,
    file_filter: Option<&str>,
) -> Result<WitnessSummary> {
    let root = workspace_root();
    let path = root.join(REGISTRY_PATH);
    let data = fs::read_to_string(&path).wrap_err_with(|| {
        format!(
            "failed to read compatibility witness registry {}",
            path.display()
        )
    })?;
    let registry: Registry = serde_json::from_str(&data).wrap_err_with(|| {
        format!(
            "failed to parse compatibility witness registry {}",
            path.display()
        )
    })?;
    validate_registry_at(&registry, &root)?;

    let in_scope = registry
        .witnesses
        .iter()
        .filter(|witness| {
            package_filter.is_none_or(|package| witness.package == package)
                && file_filter.is_none_or(|file| witness.product_file == file)
        })
        .count();

    Ok(WitnessSummary {
        registered: registry.witnesses.len(),
        in_scope,
    })
}

fn validate_registry_at(registry: &Registry, root: &Path) -> Result<()> {
    if registry.version != 1 {
        bail!(
            "unsupported compatibility witness registry version {}; expected 1",
            registry.version
        );
    }

    let mut ids = HashSet::new();
    for witness in &registry.witnesses {
        if !ids.insert(&witness.id) {
            bail!("duplicate compatibility witness id `{}`", witness.id);
        }
        for (field, value) in [
            ("contract", witness.contract.as_str()),
            ("package", witness.package.as_str()),
            ("oracle_test", witness.oracle_test.as_str()),
            ("authority", witness.authority.as_str()),
        ] {
            if value.trim().is_empty() {
                bail!("compatibility witness `{}` has empty {field}", witness.id);
            }
        }

        let product = read_repo_file(root, &witness.product_file, &witness.id)?;
        let occurrences = product.matches(&witness.mutation_anchor).count();
        if occurrences != 1 {
            bail!(
                "compatibility witness `{}` mutation_anchor occurs {occurrences} times in {}; expected exactly once",
                witness.id,
                witness.product_file
            );
        }

        let oracle = read_repo_file(root, &witness.oracle_file, &witness.id)?;
        if !oracle.contains(&format!("fn {}", witness.oracle_test)) {
            bail!(
                "compatibility witness `{}` oracle test `{}` is absent from {}",
                witness.id,
                witness.oracle_test,
                witness.oracle_file
            );
        }
        if !oracle.contains(&witness.oracle_literal) {
            bail!(
                "compatibility witness `{}` oracle does not independently assert literal `{}`",
                witness.id,
                witness.oracle_literal
            );
        }
    }
    Ok(())
}

fn read_repo_file(root: &Path, relative: &str, witness_id: &str) -> Result<String> {
    if relative.is_empty() || Path::new(relative).is_absolute() || relative.contains("..") {
        bail!("compatibility witness `{witness_id}` has invalid repository path `{relative}`");
    }
    let path = root.join(relative);
    fs::read_to_string(&path).wrap_err_with(|| {
        format!(
            "compatibility witness `{witness_id}` cannot read {}",
            path.display()
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn committed_registry_names_live_independent_oracles() -> Result<()> {
        let root = workspace_root();
        let registry: Registry =
            serde_json::from_str(&fs::read_to_string(root.join(REGISTRY_PATH))?)?;
        validate_registry_at(&registry, &root)
    }
}
