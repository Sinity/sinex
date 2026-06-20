//! Production-path obligation tests for health export parsers.

#[cfg(test)]
mod tests {
    const SLEEP_MERGED_SUMMARY_CSV: &[u8] = b"\
sh_datauuid,start_local,end_local,sh_duration_minutes,start_delta_minutes,end_delta_minutes,sa_vs_sh_duration_minutes,trimmed_event_count,hr_avg,hr_min,hr_max,events_hr,events_light,events_deep,events_rem,sa_comment
e86b7115-e01d-45ce-98ed-b8c7248b93a3,2024-03-21T10:50:00+01:00,2024-03-21T12:40:00+01:00,110.0,0.0,-49.0,-49.4,6,,,,0,1,0,0,
";

    const SLEEP_MERGED_SUMMARY_CASE: crate::ProductionPathCase = crate::ProductionPathCase::new(
        "sleep-merged-summary",
        "sleep-merged-summary",
        crate::AdapterKind::StaticFile,
        SLEEP_MERGED_SUMMARY_CSV,
        &["sleep.session"],
    );

    crate::production_path_case_test!(sleep_merged_summary_obligations, SLEEP_MERGED_SUMMARY_CASE);
}
