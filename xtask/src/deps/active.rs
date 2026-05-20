//! Cargo feature-resolved dependency views.

use color_eyre::eyre::{Result, WrapErr};
use guppy::PackageId;
use guppy::graph::cargo::{CargoOptions, CargoResolverVersion, CargoSet};
use guppy::graph::feature::StandardFeatures;
use guppy::graph::{DependencyDirection, PackageGraph};
use guppy::platform::PlatformSpec;
use std::collections::BTreeSet;

/// Direct dependency edge that Cargo would actually build.
#[derive(Debug, Clone)]
pub(crate) struct ActiveDependencyEdge {
    pub(crate) package_id: PackageId,
    pub(crate) name: String,
    pub(crate) kind: &'static str,
}

fn cargo_options(include_dev: bool) -> Result<CargoOptions<'static>> {
    let mut options = CargoOptions::new();
    options
        .set_resolver(CargoResolverVersion::V2)
        .set_include_dev(include_dev)
        .set_platform(
            PlatformSpec::build_target().context("failed to resolve current Rust build target")?,
        );
    Ok(options)
}

/// Simulate Cargo resolution for the workspace's default feature set.
pub(crate) fn workspace_cargo_set(graph: &PackageGraph, include_dev: bool) -> Result<CargoSet<'_>> {
    let options = cargo_options(include_dev)?;
    graph
        .resolve_workspace()
        .to_feature_set(StandardFeatures::Default)
        .into_cargo_set(&options)
        .context("failed to resolve workspace Cargo feature set")
}

fn package_cargo_set<'g>(
    graph: &'g PackageGraph,
    package_id: &PackageId,
    include_dev: bool,
) -> Result<CargoSet<'g>> {
    let package = graph
        .metadata(package_id)
        .context("failed to get package metadata")?;
    let options = cargo_options(include_dev)?;
    package
        .to_feature_set(StandardFeatures::Default)
        .into_cargo_set(&options)
        .with_context(|| {
            format!(
                "failed to resolve Cargo feature set for '{}'",
                package.name()
            )
        })
}

/// Package IDs active for one package's default feature set.
pub(crate) fn active_package_ids_for_package(
    graph: &PackageGraph,
    package_id: &PackageId,
    include_dev: bool,
) -> Result<BTreeSet<PackageId>> {
    Ok(cargo_set_package_ids(&package_cargo_set(
        graph,
        package_id,
        include_dev,
    )?))
}

/// Package IDs active in either the target or host build graph.
pub(crate) fn cargo_set_package_ids(cargo_set: &CargoSet<'_>) -> BTreeSet<PackageId> {
    let mut package_ids = BTreeSet::new();

    for feature_set in [cargo_set.target_features(), cargo_set.host_features()] {
        for package_id in feature_set
            .to_package_set()
            .package_ids(DependencyDirection::Forward)
        {
            package_ids.insert(package_id.clone());
        }
    }

    package_ids
}

/// Direct dependency package names active for a package's default feature set.
pub(crate) fn active_direct_dependencies(
    graph: &PackageGraph,
    package_id: &PackageId,
    include_dev: bool,
) -> Result<Vec<ActiveDependencyEdge>> {
    let cargo_set = package_cargo_set(graph, package_id, include_dev)?;
    let mut dependencies = BTreeSet::new();

    for link in cargo_set.target_links() {
        if link.from().id() == package_id {
            dependencies.insert((
                link.to().id().clone(),
                link.to().name().to_string(),
                "normal",
            ));
        }
    }

    for link in cargo_set.host_links() {
        if link.from().id() == package_id {
            dependencies.insert((link.to().id().clone(), link.to().name().to_string(), "host"));
        }
    }

    for link in cargo_set.build_dep_links() {
        if link.from().id() == package_id {
            dependencies.insert((
                link.to().id().clone(),
                link.to().name().to_string(),
                "build",
            ));
        }
    }

    for link in cargo_set.proc_macro_links() {
        if link.from().id() == package_id {
            dependencies.insert((
                link.to().id().clone(),
                link.to().name().to_string(),
                "proc-macro",
            ));
        }
    }

    Ok(dependencies
        .into_iter()
        .map(|(package_id, name, kind)| ActiveDependencyEdge {
            package_id,
            name,
            kind,
        })
        .collect())
}
