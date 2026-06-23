use serde::Serialize;

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum SourceCapabilityKind {
    Coverage,
    Debt,
    Operation,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct SourceCapabilityRef<'a> {
    pub kind: SourceCapabilityKind,
    pub target: &'a str,
    pub raw: &'a str,
}

impl<'a> SourceCapabilityRef<'a> {
    #[must_use]
    pub fn parse(raw: &'a str) -> Option<Self> {
        for (prefix, kind) in [
            ("coverage:", SourceCapabilityKind::Coverage),
            ("debt:", SourceCapabilityKind::Debt),
            ("operation:", SourceCapabilityKind::Operation),
        ] {
            let Some(target) = raw.strip_prefix(prefix) else {
                continue;
            };
            if target.is_empty() {
                return None;
            }
            return Some(Self { kind, target, raw });
        }
        None
    }

    #[must_use]
    pub fn is_kind(self, kind: SourceCapabilityKind) -> bool {
        self.kind == kind
    }
}
