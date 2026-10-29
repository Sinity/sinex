//! Write authority comes from transport configuration, independently of read grants.
use super::*;
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
    #[serde(default)]
    pub forward_signals: bool,
    #[serde(default)]
    pub author_attestation: Option<sinex_protocol::signals::AuthorAttestation>,
    #[serde(default)]
    pub emergency_sinex_blocked_host: Option<String>,
    #[serde(default)]
    pub emergency_lease_seconds: Option<u32>,
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
) -> Result<&'a WriteGrant, String> {
    grants
        .get(producer)
        .and_then(|grant| grant.write.as_ref())
        .ok_or_else(|| "producer has no configured write family".into())
}
pub(crate) fn event<'a>(
    grants: &'a fetch::ProducerGrants,
    producer: &str,
    kind: &str,
    class: Class,
) -> Result<&'a WriteGrant, String> {
    let grant = write(grants, producer)?;
    if kind.starts_with("signal.") {
        return Err("host signal records require the typed signal ingress capability".into());
    }
    if !matches(&grant.kind_prefixes, kind) || !grant.classes.contains(&class) {
        return Err("producer write kind or class is not authorized".into());
    }
    if grants.iter().any(|(identity, other)| {
        identity != producer
            && other.write.as_ref().is_some_and(|other| {
                other.family == grant.family && matches(&other.canonical_prefixes, kind)
            })
    }) {
        return Err("kind requires the configured canonical publisher".into());
    }
    Ok(grant)
}
pub(crate) fn validate(grants: &fetch::ProducerGrants) -> Result<(), String> {
    let mut publishers = std::collections::BTreeMap::new();
    let mut catalogs = std::collections::BTreeMap::new();
    for (identity, grant) in grants {
        if let Some(catalog) = &grant.indexed_catalog {
            if catalogs
                .insert((&catalog.name, catalog.version), catalog.fingerprint())
                .is_some_and(|known| known != catalog.fingerprint())
            {
                return Err("conflicting indexed catalog under one name and version".into());
            }
            let mut definitions = std::collections::BTreeMap::new();
            if catalog.name.is_empty() || catalog.version == 0 || catalog.kinds.is_empty() {
                return Err(format!("invalid indexed projection catalog for {identity}"));
            }
            for kind in &catalog.kinds {
                if definitions
                    .insert(&kind.kind, kind.definition_hash())
                    .is_some()
                {
                    return Err(format!(
                        "duplicate kind version in indexed projection catalog for {identity}"
                    ));
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
            return Err(format!("invalid write family capability for {identity}"));
        }
        if write
            .emergency_sinex_blocked_host
            .as_ref()
            .is_some_and(|host| {
                host.is_empty()
                    || !write.forward_signals
                    || write.host.as_deref() != Some(host.as_str())
            })
        {
            return Err(format!(
                "emergency completion requires its configured producer host and canonical signal forwarding for {identity}"
            ));
        }
        if write
            .emergency_lease_seconds
            .is_some_and(|seconds| seconds == 0 || seconds > 3600)
            || write.emergency_lease_seconds.is_some()
                && write.emergency_sinex_blocked_host.is_none()
        {
            return Err(format!(
                "offline emergency lease requires an explicit host and a duration of 1..=3600 seconds for {identity}"
            ));
        }
        if write.forward_signals && write.canonical_prefixes.is_empty() {
            return Err(format!(
                "signal forwarding requires the canonical family namespace for {identity}"
            ));
        }
        if let Some(attestation) = &write.author_attestation {
            if !write.forward_signals || write.canonical_prefixes.is_empty() {
                return Err(format!(
                    "author attestation requires canonical signal forwarding for {identity}"
                ));
            }
            let mut seen = std::collections::BTreeSet::new();
            for rule in &attestation.shipped {
                if rule.id.strip_prefix("shipped:").is_none_or(str::is_empty)
                    || rule.revision == 0
                    || rule.fingerprint.len() != 64
                    || !rule
                        .fingerprint
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                    || !seen.insert((&rule.id, rule.revision))
                {
                    return Err(format!(
                        "invalid or conflicting shipped rule attestation for {identity}"
                    ));
                }
            }
        }
        if write
            .host
            .as_ref()
            .is_some_and(|host| host.is_empty() || host.contains('\0'))
        {
            return Err(format!("invalid attested host for {identity}"));
        }
        for declaration in &write.source_declarations {
            if declaration.family != write.family
                || !matches(&write.source_prefixes, &declaration.source.name)
                || declaration.kinds.is_empty()
            {
                return Err(format!(
                    "source declaration is outside configured authority for {identity}"
                ));
            }
            for kind in &declaration.kinds {
                if !matches(&write.kind_prefixes, &kind.kind.name)
                    || !matches!(kind.declaring_producer.as_str(), "sinex")
                        && kind.declaring_producer != write.family
                {
                    return Err(format!("source emits an unauthorized kind for {identity}"));
                }
            }
        }
        for kind in &write.canonical_kinds {
            if kind.declaring_producer != write.family
                || !matches(&write.kind_prefixes, &kind.kind.name)
            {
                return Err(format!(
                    "canonical declaration is outside configured authority for {identity}"
                ));
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
                return Err(format!("invalid write kind prefix for {identity}"));
            }
        }
        if write.canonical_prefixes.iter().any(|prefix| {
            !matches(
                &write.kind_prefixes,
                prefix.trim_end_matches('*').trim_end_matches('.'),
            )
        }) {
            return Err(format!(
                "canonical prefix is outside write capability for {identity}"
            ));
        }
        if !write.canonical_prefixes.is_empty()
            && publishers.insert(&write.family, identity).is_some()
        {
            return Err(format!(
                "family {} has more than one canonical publisher",
                write.family
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
