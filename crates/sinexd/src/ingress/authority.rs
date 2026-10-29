//! Write authority comes from transport configuration, independently of read grants.
use super::*;
use serde::Deserialize;
use sinex_model::Class;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WriteGrant {
    pub family: String,
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
    for (identity, grant) in grants {
        let Some(write) = &grant.write else {
            continue;
        };
        if !sinex_protocol::nats::valid_producer(&write.family)
            || write.kind_prefixes.is_empty()
            || write.classes.is_empty()
        {
            return Err(format!("invalid write family capability for {identity}"));
        }
        for prefix in write.kind_prefixes.iter().chain(&write.canonical_prefixes) {
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
