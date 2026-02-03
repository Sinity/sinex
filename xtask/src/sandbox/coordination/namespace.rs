use sinex_primitives::environment::environment;
use sinex_primitives::Ulid;

/// Generates unique `JetStream` subject/stream namespaces per test.
#[derive(Clone, Debug)]
pub struct PipelineNamespace {
    prefix: String,
}

impl PipelineNamespace {
    #[must_use]
    pub fn new(test_name: &str) -> Self {
        let sanitized = test_name
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() {
                    c.to_ascii_lowercase()
                } else {
                    '-'
                }
            })
            .collect::<String>();
        let trimmed = sanitized.trim_matches('-');
        let mut prefix = trimmed.chars().take(32).collect::<String>();
        if prefix.is_empty() {
            prefix.push('t');
        }
        let suffix = Ulid::new().to_string().to_lowercase();
        let full = format!("{prefix}-{suffix}");
        Self { prefix: full }
    }

    #[must_use]
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    #[must_use]
    pub fn subject(&self, base: &str) -> String {
        environment().nats_subject_with_namespace(Some(&self.prefix), base)
    }

    #[must_use]
    pub fn stream(&self, base: &str) -> String {
        environment().nats_stream_name_with_namespace(Some(&self.prefix), base)
    }

    #[must_use]
    pub fn consumer_name(&self, base: &str) -> String {
        let cleaned = base
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() {
                    c.to_ascii_uppercase()
                } else {
                    '_'
                }
            })
            .collect::<String>();
        format!("{}_{}", self.prefix.replace('-', "_"), cleaned)
    }
}
