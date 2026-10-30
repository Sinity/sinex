//! One bounded policy refresh page per producer, using the ordinary writer.
use super::*;
use sinex_store::privacy::refresh::{self, Cursor, Phase, Refresh};
impl DerivationRunner {
    pub(super) async fn refresh_policy(&mut self, epoch: i64, env: &Env) -> Result<usize, String> {
        let mut work = 0;
        for index in 0..self.keyed.len() {
            let name = self.keyed[index].name();
            if registry::selected(&mut self.read, name)
                .await
                .map_err(|error| error.to_string())?
                != Some((
                    i64::from(self.keyed[index].version_for(&self.time_settings)),
                    crate::build_identity::current()?.to_owned(),
                ))
            {
                continue;
            }
            let expected = refresh::cursor(&mut self.read, name, epoch)
                .await
                .map_err(|error| error.to_string())?;
            let mut next = expected.clone().unwrap_or_default();
            let mut keys = Vec::new();
            match next.phase {
                Phase::Complete => continue,
                Phase::Inputs => {
                    let types: Vec<_> = self.keyed[index]
                        .inputs_types()
                        .iter()
                        .map(|kind| (*kind).to_owned())
                        .collect();
                    let inputs = derivations::after_seq(
                        &mut self.read,
                        &types,
                        next.input_after,
                        EVENTS_PER_PASS,
                        sinex_store::PAGE_BYTES,
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                    if inputs.is_empty() {
                        next.phase = Phase::Outputs;
                    } else {
                        next.input_after = inputs.last().expect("nonempty").commit_seq;
                        let mut seen = HashSet::new();
                        for input in inputs {
                            let view = KeyView {
                                event_type: &input.event.event_type,
                                ts_orig: input.event.ts_orig.at,
                                payload: &input.event.payload,
                            };
                            for key in self.keyed[index].keys_for(&view, env) {
                                if seen.insert(key.clone()) {
                                    keys.push(key)
                                }
                            }
                        }
                    }
                }
                Phase::Outputs => {
                    keys = refresh::output_keys_after(
                        &mut self.read,
                        name,
                        next.output_after.as_ref(),
                        KEYS_PER_REQUEST as i64,
                        sinex_store::PAGE_BYTES,
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                    if let Some(last) = keys.last() {
                        next.output_after = Some(last.clone());
                    } else {
                        next.phase = Phase::Complete;
                    }
                }
            }
            self.submit_policy_refresh(
                name,
                keys,
                Refresh {
                    epoch,
                    expected,
                    next,
                    reset_position: false,
                },
            )
            .await?;
            work += 1;
        }
        for index in 0..self.per_event.len() {
            let name = self.per_event[index].name();
            if registry::selected(&mut self.read, name)
                .await
                .map_err(|error| error.to_string())?
                != Some((
                    i64::from(self.per_event[index].version()),
                    crate::build_identity::current()?.to_owned(),
                ))
            {
                continue;
            }
            let expected = refresh::cursor(&mut self.read, name, epoch)
                .await
                .map_err(|error| error.to_string())?;
            if expected
                .as_ref()
                .is_some_and(|cursor| cursor.phase == Phase::Complete)
            {
                continue;
            }
            self.submit_policy_refresh(
                name,
                vec![],
                Refresh {
                    epoch,
                    expected,
                    next: Cursor {
                        phase: Phase::Complete,
                        ..Cursor::default()
                    },
                    reset_position: true,
                },
            )
            .await?;
            work += 1;
        }
        Ok(work)
    }
    async fn submit_policy_refresh(
        &self,
        name: &str,
        keys: Vec<Value>,
        policy: Refresh,
    ) -> Result<(), String> {
        let batch = Batch {
            run: self.run_of(name)?,
            kind: BatchKind::Derived {
                derivation: name.to_owned(),
                scope: DerivedScope::Recompute {
                    keys,
                    policy_refresh: Some(policy),
                }
                .to_value(),
            },
            records: vec![],
            archive: vec![],
        };
        self.writer
            .submit(Priority::Derived, batch)
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    }
}
