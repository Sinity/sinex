//! Write authority comes from transport configuration, independently of read grants.
use super::*;
use crate::{Error, Result};
use serde::Deserialize;
use sinex_model::Class;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WriteGrant {
    pub family: String,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub source_prefixes: Vec<String>,
    #[serde(default)]
    pub source_declarations: Vec<sinex_model::SourceDeclaration>,
    #[serde(default)]
    pub canonical_kinds: Vec<sinex_model::KindDeclaration>,
    /// The family may forward its evaluator reports to this host's deliverer.
    #[serde(default)]
    pub forward_signals: bool,
    pub kind_prefixes: Vec<String>,
    pub classes: Vec<Class>,
    #[serde(default)]
    pub canonical_prefixes: Vec<String>,
}
pub(crate) fn matches(prefixes: &[String], name: &str) -> bool {
    prefixes.iter().any(|prefix| {
        let prefix = prefix.trim_end_matches('*').trim_end_matches('.');
        name == prefix
            || name
                .strip_prefix(prefix)
                .is_some_and(|tail| tail.starts_with('.'))
    })
}
pub(crate) fn write<'a>(
    grants: &'a fetch::ProducerGrants,
    producer: &str,
) -> Result<&'a WriteGrant> {
    grants
        .get(producer)
        .and_then(|grant| grant.write.as_ref())
        .ok_or_else(|| Error::refused("producer has no configured write family"))
}
pub(crate) fn event<'a>(
    grants: &'a fetch::ProducerGrants,
    producer: &str,
    kind: &str,
    class: Class,
) -> Result<&'a WriteGrant> {
    let grant = write(grants, producer)?;
    if kind.starts_with("signal.") {
        return Err(Error::internal(
            "host signal records require the typed signal ingress capability",
        ));
    }
    if !matches(&grant.kind_prefixes, kind) || !grant.classes.contains(&class) {
        return Err(Error::refused(
            "producer write kind or class is not authorized",
        ));
    }
    if grants.iter().any(|(identity, other)| {
        identity != producer
            && other.write.as_ref().is_some_and(|other| {
                other.family == grant.family && matches(&other.canonical_prefixes, kind)
            })
    }) {
        return Err(Error::refused(
            "kind requires the configured canonical publisher",
        ));
    }
    Ok(grant)
}
pub(crate) fn validate(grants: &fetch::ProducerGrants) -> Result<()> {
    let mut publishers = std::collections::BTreeMap::new();
    let mut catalogs = std::collections::BTreeMap::new();
    for (identity, grant) in grants {
        if let Some(catalog) = &grant.indexed_catalog {
            if catalogs
                .insert((&catalog.name, catalog.version), catalog.fingerprint())
                .is_some_and(|known| known != catalog.fingerprint())
            {
                return Err(Error::internal(
                    "conflicting indexed catalog under one name and version",
                ));
            }
            let mut definitions = std::collections::BTreeMap::new();
            if catalog.name.is_empty() || catalog.version == 0 || catalog.kinds.is_empty() {
                return Err(Error::internal(format!(
                    "invalid indexed projection catalog for {identity}"
                )));
            }
            for kind in &catalog.kinds {
                if definitions
                    .insert(&kind.kind, kind.definition_hash())
                    .is_some()
                {
                    return Err(Error::internal(format!(
                        "duplicate kind version in indexed projection catalog for {identity}"
                    )));
                }
            }
        }
        let Some(write) = &grant.write else {
            continue;
        };
        if !sinex_protocol::nats::valid_producer(&write.family)
            || write.kind_prefixes.is_empty()
            || write.classes.is_empty()
        {
            return Err(Error::internal(format!(
                "invalid write family capability for {identity}"
            )));
        }
        if write.forward_signals && write.family == "sinex" {
            return Err(Error::internal(format!(
                "signal forwarding cannot use the sinex family, this host's own product, for {identity}"
            )));
        }
        if write.forward_signals && write.canonical_prefixes.is_empty() {
            return Err(Error::internal(format!(
                "signal forwarding requires the canonical family namespace for {identity}"
            )));
        }
        if write
            .host
            .as_ref()
            .is_some_and(|host| host.is_empty() || host.contains('\0'))
        {
            return Err(Error::internal(format!(
                "invalid attested host for {identity}"
            )));
        }
        for declaration in &write.source_declarations {
            if declaration.family != write.family
                || !matches(&write.source_prefixes, &declaration.source.name)
                || declaration.kinds.is_empty()
            {
                return Err(Error::internal(format!(
                    "source declaration is outside configured authority for {identity}"
                )));
            }
            for kind in &declaration.kinds {
                if !matches(&write.kind_prefixes, &kind.kind.name)
                    || !matches!(kind.declaring_producer.as_str(), "sinex")
                        && kind.declaring_producer != write.family
                {
                    return Err(Error::internal(format!(
                        "source emits an unauthorized kind for {identity}"
                    )));
                }
            }
        }
        for kind in &write.canonical_kinds {
            if kind.declaring_producer != write.family
                || !matches(&write.kind_prefixes, &kind.kind.name)
            {
                return Err(Error::internal(format!(
                    "canonical declaration is outside configured authority for {identity}"
                )));
            }
        }
        for prefix in write
            .kind_prefixes
            .iter()
            .chain(&write.canonical_prefixes)
            .chain(&write.source_prefixes)
        {
            let prefix = prefix.trim_end_matches('*').trim_end_matches('.');
            if prefix.is_empty()
                || prefix.split('.').any(|part| {
                    part.is_empty()
                        || !part.bytes().all(|byte| {
                            byte.is_ascii_lowercase()
                                || byte.is_ascii_digit()
                                || byte == b'_'
                                || byte == b'-'
                        })
                })
            {
                return Err(Error::internal(format!(
                    "invalid write kind prefix for {identity}"
                )));
            }
        }
        if write.canonical_prefixes.iter().any(|prefix| {
            !matches(
                &write.kind_prefixes,
                prefix.trim_end_matches('*').trim_end_matches('.'),
            )
        }) {
            return Err(Error::internal(format!(
                "canonical prefix is outside write capability for {identity}"
            )));
        }
        if !write.canonical_prefixes.is_empty()
            && publishers.insert(&write.family, identity).is_some()
        {
            return Err(Error::internal(format!(
                "family {} has more than one canonical publisher",
                write.family
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;

/// The same current native source authority serves admission and optional
/// retained-material header observations. Neither path registers old sources.
pub(crate) async fn native_declaration(
    conn: &mut PgConnection,
    grants: &fetch::ProducerGrants,
    producer: &str,
    source: &sinex_model::SourceRef,
) -> Result<sinex_model::SourceDeclaration, Check> {
    let write = write(grants, producer).map_err(|error| Check::Configuration(error.to_string()))?;
    if !matches(&write.source_prefixes, &source.name) {
        return Err(Check::Configuration(
            "native source is outside the configured credential capability".into(),
        ));
    }
    let declaration = sinex_store::source_declarations::load(conn, source)
        .await
        .map_err(|error| Check::Unavailable(error.to_string()))?
        .ok_or_else(|| {
            Check::Configuration(
                "native source version is not installed in the owner catalog".into(),
            )
        })?;
    if declaration.family != write.family {
        return Err(Check::Configuration(
            "native source belongs to another configured family".into(),
        ));
    }
    Ok(declaration)
}
