//! Typed, fail-closed compilation for bounded proof-obligation manifests.

use crate::command::{CommandContext, CommandResult};
use color_eyre::eyre::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

const SCHEMA_VERSION: &str = "proof-obligation-ir/v1";
const MAX_WITNESS_TIMEOUT_SECONDS: u64 = 900;

#[derive(Debug, Clone, Deserialize)]
struct Manifest {
    schema_version: String,
    claims: Vec<Claim>,
    obligations: Vec<Obligation>,
    witnesses: Vec<Witness>,
    probes: Vec<Probe>,
}

#[derive(Debug, Clone, Deserialize)]
struct Claim {
    id: String,
    statement: String,
}

#[derive(Debug, Clone, Deserialize)]
struct Obligation {
    id: String,
    claim_ids: Vec<String>,
    statement: String,
    mandatory: bool,
    required_strength: Strength,
    witness_ids: Vec<String>,
    #[serde(default)]
    probe_ids: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct Witness {
    id: String,
    mechanism: Mechanism,
    maximum_strength: Strength,
    #[serde(default)]
    command: Vec<String>,
    timeout_seconds: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
struct Probe {
    id: String,
    obligation_ids: Vec<String>,
    control_class: ControlClass,
    predicate_witness_id: String,
    expected_outcome: ProbeOutcome,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
enum Strength {
    Unproven,
    DeclaredOnly,
    CapabilityOnly,
    StructurallySupported,
    BehaviorSupported,
    BehaviorProven,
    LiveProven,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Mechanism {
    Declaration,
    CatalogPresence,
    PortableStatic,
    PortableProcess,
    NativeTest,
    DatabaseQuery,
    TransportProbe,
    LiveProductPath,
    HumanAdjudication,
}

impl Mechanism {
    const fn executes(self) -> bool {
        !matches!(
            self,
            Self::Declaration | Self::CatalogPresence | Self::PortableStatic
        )
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ControlClass {
    MeaningfulWeakening,
    KnownVacuous,
    NegativeControl,
    MissingEvidenceControl,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ProbeOutcome {
    Killed,
    Survived,
    Refused,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct CompiledObligation {
    id: String,
    mandatory: bool,
    witness_count: usize,
    meaningful_probe_count: usize,
    known_vacuous_probe_count: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct CompilationReport {
    schema_version: String,
    claim_count: usize,
    obligation_count: usize,
    witness_count: usize,
    probe_count: usize,
    obligations: Vec<CompiledObligation>,
}

pub fn execute(path: &Path, ctx: &CommandContext) -> Result<CommandResult> {
    let raw = fs::read_to_string(path).with_context(|| {
        format!(
            "failed to read proof-obligation manifest {}",
            path.display()
        )
    })?;
    let manifest: Manifest = serde_json::from_str(&raw).with_context(|| {
        format!(
            "failed to parse proof-obligation manifest {}",
            path.display()
        )
    })?;
    let report = compile_manifest(&manifest)?;

    if ctx.is_human() {
        println!(
            "Compiled {} obligations from {} ({} witnesses, {} probes)",
            report.obligation_count,
            path.display(),
            report.witness_count,
            report.probe_count
        );
    }

    Ok(CommandResult::success()
        .with_message("Proof-obligation manifest compiled")
        .with_detail(format!("manifest={}", path.display()))
        .with_detail(format!("obligations={}", report.obligation_count))
        .with_data(serde_json::to_value(report)?)
        .with_duration(ctx.elapsed()))
}

fn compile_manifest(manifest: &Manifest) -> Result<CompilationReport> {
    if manifest.schema_version != SCHEMA_VERSION {
        bail!(
            "unsupported proof-obligation schema `{}`; expected `{SCHEMA_VERSION}`",
            manifest.schema_version
        );
    }

    let claims = unique_by_id(
        &manifest.claims,
        |claim| (&claim.id, &claim.statement),
        "claim",
    )?;
    let witnesses = unique_by_id(
        &manifest.witnesses,
        |witness| (&witness.id, &witness.id),
        "witness",
    )?;
    let probes = unique_by_id(&manifest.probes, |probe| (&probe.id, &probe.id), "probe")?;
    let obligations = unique_by_id(
        &manifest.obligations,
        |obligation| (&obligation.id, &obligation.statement),
        "obligation",
    )?;

    for witness in &manifest.witnesses {
        if witness.mechanism.executes() {
            if witness.command.is_empty()
                || witness.command.iter().any(|part| part.trim().is_empty())
            {
                bail!(
                    "executable witness `{}` must define non-empty argv",
                    witness.id
                );
            }
            let timeout = witness.timeout_seconds.ok_or_else(|| {
                color_eyre::eyre::eyre!("executable witness `{}` must define a timeout", witness.id)
            })?;
            if timeout == 0 || timeout > MAX_WITNESS_TIMEOUT_SECONDS {
                bail!(
                    "executable witness `{}` timeout must be in 1..={MAX_WITNESS_TIMEOUT_SECONDS} seconds",
                    witness.id
                );
            }
        }
    }

    for probe in &manifest.probes {
        require_ref(
            &witnesses,
            &probe.predicate_witness_id,
            "witness",
            &probe.id,
        )?;
        if probe.obligation_ids.is_empty() {
            bail!("probe `{}` must target at least one obligation", probe.id);
        }
        for obligation_id in &probe.obligation_ids {
            require_ref(&obligations, obligation_id, "obligation", &probe.id)?;
        }
        match probe.control_class {
            ControlClass::MeaningfulWeakening if probe.expected_outcome != ProbeOutcome::Killed => {
                bail!("meaningful weakening `{}` must expect `killed`", probe.id);
            }
            ControlClass::KnownVacuous if probe.expected_outcome != ProbeOutcome::Survived => {
                bail!("known-vacuous probe `{}` must expect `survived`", probe.id);
            }
            _ => {}
        }
    }

    let mut compiled = Vec::with_capacity(manifest.obligations.len());
    for obligation in &manifest.obligations {
        if obligation.claim_ids.is_empty() {
            bail!(
                "obligation `{}` must reference at least one claim",
                obligation.id
            );
        }
        for claim_id in &obligation.claim_ids {
            require_ref(&claims, claim_id, "claim", &obligation.id)?;
        }
        if obligation.mandatory && obligation.witness_ids.is_empty() {
            bail!("mandatory obligation `{}` has no witness", obligation.id);
        }
        for witness_id in &obligation.witness_ids {
            require_ref(&witnesses, witness_id, "witness", &obligation.id)?;
            let witness = witnesses[witness_id];
            if witness.maximum_strength < obligation.required_strength {
                bail!(
                    "witness `{witness_id}` cannot satisfy obligation `{}` at its required strength",
                    obligation.id
                );
            }
        }

        let mut meaningful = 0;
        let mut vacuous = 0;
        for probe_id in &obligation.probe_ids {
            require_ref(&probes, probe_id, "probe", &obligation.id)?;
            let probe = probes[probe_id];
            if !probe.obligation_ids.contains(&obligation.id) {
                bail!(
                    "probe `{probe_id}` does not target obligation `{}`",
                    obligation.id
                );
            }
            meaningful += usize::from(probe.control_class == ControlClass::MeaningfulWeakening);
            vacuous += usize::from(probe.control_class == ControlClass::KnownVacuous);
        }
        if obligation.mandatory && obligation.required_strength >= Strength::BehaviorProven {
            if meaningful == 0 {
                bail!(
                    "behavior-level obligation `{}` lacks a meaningful weakening",
                    obligation.id
                );
            }
            if vacuous == 0 {
                bail!(
                    "behavior-level obligation `{}` lacks a known-vacuous control",
                    obligation.id
                );
            }
        }
        compiled.push(CompiledObligation {
            id: obligation.id.clone(),
            mandatory: obligation.mandatory,
            witness_count: obligation.witness_ids.len(),
            meaningful_probe_count: meaningful,
            known_vacuous_probe_count: vacuous,
        });
    }
    compiled.sort_by(|left, right| left.id.cmp(&right.id));

    Ok(CompilationReport {
        schema_version: manifest.schema_version.clone(),
        claim_count: manifest.claims.len(),
        obligation_count: manifest.obligations.len(),
        witness_count: manifest.witnesses.len(),
        probe_count: manifest.probes.len(),
        obligations: compiled,
    })
}

fn unique_by_id<'a, T, F>(items: &'a [T], fields: F, kind: &str) -> Result<BTreeMap<String, &'a T>>
where
    F: Fn(&T) -> (&String, &String),
{
    let mut result = BTreeMap::new();
    for item in items {
        let (id, statement) = fields(item);
        if id.trim().is_empty() || statement.trim().is_empty() {
            bail!("{kind} id and statement must not be empty");
        }
        if result.insert(id.clone(), item).is_some() {
            bail!("duplicate {kind} id `{id}`");
        }
    }
    Ok(result)
}

fn require_ref<T>(items: &BTreeMap<String, T>, id: &str, kind: &str, owner: &str) -> Result<()> {
    if !items.contains_key(id) {
        bail!("`{owner}` references unknown {kind} `{id}`");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_manifest() -> Manifest {
        Manifest {
            schema_version: SCHEMA_VERSION.into(),
            claims: vec![Claim {
                id: "claim.demo".into(),
                statement: "demo behavior".into(),
            }],
            obligations: vec![Obligation {
                id: "obligation.demo".into(),
                claim_ids: vec!["claim.demo".into()],
                statement: "demo remains sensitive".into(),
                mandatory: true,
                required_strength: Strength::BehaviorProven,
                witness_ids: vec!["witness.demo".into()],
                probe_ids: vec!["probe.break".into(), "probe.control".into()],
            }],
            witnesses: vec![Witness {
                id: "witness.demo".into(),
                mechanism: Mechanism::NativeTest,
                maximum_strength: Strength::BehaviorProven,
                command: vec![
                    "xtask".into(),
                    "test".into(),
                    "-p".into(),
                    "sinexctl".into(),
                ],
                timeout_seconds: Some(120),
            }],
            probes: vec![
                Probe {
                    id: "probe.break".into(),
                    obligation_ids: vec!["obligation.demo".into()],
                    control_class: ControlClass::MeaningfulWeakening,
                    predicate_witness_id: "witness.demo".into(),
                    expected_outcome: ProbeOutcome::Killed,
                },
                Probe {
                    id: "probe.control".into(),
                    obligation_ids: vec!["obligation.demo".into()],
                    control_class: ControlClass::KnownVacuous,
                    predicate_witness_id: "witness.demo".into(),
                    expected_outcome: ProbeOutcome::Survived,
                },
            ],
        }
    }

    #[test]
    fn compiles_bounded_non_vacuous_obligation() {
        let report = compile_manifest(&valid_manifest()).unwrap();
        assert_eq!(report.obligation_count, 1);
        assert_eq!(report.obligations[0].meaningful_probe_count, 1);
        assert_eq!(report.obligations[0].known_vacuous_probe_count, 1);
    }

    #[test]
    fn rejects_behavior_claim_without_known_vacuous_control() {
        let mut manifest = valid_manifest();
        manifest.obligations[0].probe_ids.pop();
        let error = compile_manifest(&manifest).unwrap_err().to_string();
        assert!(error.contains("lacks a known-vacuous control"));
    }

    #[test]
    fn rejects_unbounded_executable_witness() {
        let mut manifest = valid_manifest();
        manifest.witnesses[0].timeout_seconds = None;
        let error = compile_manifest(&manifest).unwrap_err().to_string();
        assert!(error.contains("must define a timeout"));
    }

    #[test]
    fn rejects_dangling_reference() {
        let mut manifest = valid_manifest();
        manifest.obligations[0].claim_ids[0] = "claim.missing".into();
        let error = compile_manifest(&manifest).unwrap_err().to_string();
        assert!(error.contains("unknown claim"));
    }
}
