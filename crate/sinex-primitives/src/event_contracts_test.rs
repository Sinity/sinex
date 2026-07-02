use super::*;
use crate::admission_policy::{STANDARD_EVENT_ADMISSION_POLICY_ID, admission_policies};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn terminal_command_contracts_are_package_and_policy_addressable() -> TestResult<()> {
    for (contract_id, source, package_id) in [
        (
            SHELL_ATUIN_COMMAND_EXECUTED_CONTRACT_ID,
            "shell.atuin",
            "terminal.atuin-history",
        ),
        (
            SHELL_KITTY_COMMAND_EXECUTED_CONTRACT_ID,
            "shell.kitty",
            "terminal.kitty-osc-live",
        ),
    ] {
        let Some(contract) = find_event_contract(contract_id) else {
            panic!("missing terminal command EventContract {contract_id}");
        };

        assert_eq!(contract.event_source, source);
        assert_eq!(contract.event_type, "command.executed");
        assert!(contract.package_refs.contains(&package_id));
        assert_eq!(
            contract.admission_policy_ref,
            Some(STANDARD_EVENT_ADMISSION_POLICY_ID)
        );

        let accepted_by_standard = admission_policies().any(|policy| {
            policy.id == STANDARD_EVENT_ADMISSION_POLICY_ID
                && policy.accepts_event_contract(contract_id)
        });
        assert!(
            accepted_by_standard,
            "{contract_id} must be admission-addressable"
        );
    }

    Ok(())
}

#[sinex_test]
async fn browser_page_visit_contract_is_package_and_policy_addressable() -> TestResult<()> {
    let Some(contract) = find_event_contract(BROWSER_PAGE_VISITED_CONTRACT_ID) else {
        panic!("missing browser page visit EventContract");
    };

    assert_eq!(contract.event_source, "webhistory");
    assert_eq!(contract.event_type, "page.visited");
    assert!(contract.package_refs.contains(&"browser.history"));
    assert_eq!(
        contract.admission_policy_ref,
        Some(STANDARD_EVENT_ADMISSION_POLICY_ID)
    );

    let accepted_by_standard = admission_policies().any(|policy| {
        policy.id == STANDARD_EVENT_ADMISSION_POLICY_ID
            && policy.accepts_event_contract(BROWSER_PAGE_VISITED_CONTRACT_ID)
    });
    assert!(accepted_by_standard);

    Ok(())
}

#[sinex_test]
async fn email_message_contracts_are_package_and_policy_addressable() -> TestResult<()> {
    for id in [
        EMAIL_MESSAGE_RECEIVED_CONTRACT_ID,
        EMAIL_MESSAGE_SENT_CONTRACT_ID,
        EMAIL_ATTACHMENT_OBSERVED_CONTRACT_ID,
        EMAIL_THREAD_OBSERVED_CONTRACT_ID,
        EMAIL_SYNC_CURSOR_OBSERVED_CONTRACT_ID,
        EMAIL_CAPTURE_RUNTIME_OBSERVED_CONTRACT_ID,
    ] {
        let Some(contract) = find_event_contract(id) else {
            panic!("missing email EventContract {id}");
        };

        assert_eq!(contract.event_source, "email");
        assert!(contract.package_refs.contains(&"email.mailbox"));
        assert_eq!(
            contract.admission_policy_ref,
            Some(STANDARD_EVENT_ADMISSION_POLICY_ID)
        );

        let accepted_by_standard = admission_policies().any(|policy| {
            policy.id == STANDARD_EVENT_ADMISSION_POLICY_ID && policy.accepts_event_contract(id)
        });
        assert!(accepted_by_standard, "{id} must be admission-addressable");
    }

    Ok(())
}

#[sinex_test]
async fn browser_live_contracts_are_package_policy_and_payload_addressable() -> TestResult<()> {
    for (contract_id, event_type) in [
        (
            BROWSER_NAVIGATION_OBSERVED_CONTRACT_ID,
            "navigation.observed",
        ),
        (BROWSER_TAB_ACTIVATED_CONTRACT_ID, "tab.activated"),
        (BROWSER_DOWNLOAD_OBSERVED_CONTRACT_ID, "download.observed"),
    ] {
        let Some(contract) = find_event_contract(contract_id) else {
            panic!("missing browser live EventContract {contract_id}");
        };

        assert_eq!(contract.event_source, "browser");
        assert_eq!(contract.event_type, event_type);
        assert!(contract.package_refs.contains(&"browser.webextension-live"));
        assert!(contract.is_canonical_event());
        assert_eq!(
            contract.admission_policy_ref,
            Some(STANDARD_EVENT_ADMISSION_POLICY_ID)
        );

        match contract.payload_schema {
            PayloadSchemaContract::PayloadInventory {
                source,
                event_type: schema_event_type,
                version,
            } => {
                assert_eq!(source, "browser");
                assert_eq!(schema_event_type, event_type);
                assert_eq!(version, "1.0.0");
            }
            PayloadSchemaContract::ExplicitSchemaId { schema_id } => {
                panic!(
                    "browser live EventContract {contract_id} should use payload inventory, got {schema_id}"
                )
            }
        }

        let accepted_by_standard = admission_policies().any(|policy| {
            policy.id == STANDARD_EVENT_ADMISSION_POLICY_ID
                && policy.accepts_event_contract(contract_id)
        });
        assert!(
            accepted_by_standard,
            "{contract_id} must be admission-addressable"
        );
    }

    Ok(())
}

#[sinex_test]
async fn media_capture_contracts_are_package_policy_and_payload_addressable() -> TestResult<()> {
    for (contract_id, package_id, source, event_type) in [
        (
            MEDIA_AUDIO_RECORDING_OBSERVED_CONTRACT_ID,
            "media.audio-transcript",
            "media.audio",
            "media.audio.recording_observed",
        ),
        (
            MEDIA_AUDIO_CAPTURE_SESSION_STARTED_CONTRACT_ID,
            "media.audio-transcript",
            "media.audio",
            "media.audio.capture_session_started",
        ),
        (
            MEDIA_AUDIO_CAPTURE_SESSION_ENDED_CONTRACT_ID,
            "media.audio-transcript",
            "media.audio",
            "media.audio.capture_session_ended",
        ),
        (
            MEDIA_AUDIO_TRANSCRIPT_SEGMENT_CONTRACT_ID,
            "media.audio-transcript",
            "media.audio",
            "media.audio.transcript_segment_observed",
        ),
        (
            MEDIA_AUDIO_TRANSCRIPTION_RUN_OBSERVED_CONTRACT_ID,
            "media.audio-transcript",
            "media.audio",
            "media.audio.transcription_run_observed",
        ),
        (
            MEDIA_SCREEN_SCREENSHOT_OBSERVED_CONTRACT_ID,
            "media.screen-ocr",
            "media.screen",
            "media.screen.screenshot_observed",
        ),
        (
            MEDIA_SCREEN_CAPTURE_SESSION_STARTED_CONTRACT_ID,
            "media.screen-ocr",
            "media.screen",
            "media.screen.capture_session_started",
        ),
        (
            MEDIA_SCREEN_CAPTURE_SESSION_ENDED_CONTRACT_ID,
            "media.screen-ocr",
            "media.screen",
            "media.screen.capture_session_ended",
        ),
        (
            MEDIA_SCREEN_VIDEO_SEGMENT_OBSERVED_CONTRACT_ID,
            "media.screen-ocr",
            "media.screen",
            "media.screen.video_segment_observed",
        ),
        (
            MEDIA_SCREEN_OCR_SEGMENT_CONTRACT_ID,
            "media.screen-ocr",
            "media.screen",
            "media.screen.ocr_segment_observed",
        ),
        (
            MEDIA_SCREEN_OCR_RUN_OBSERVED_CONTRACT_ID,
            "media.screen-ocr",
            "media.screen",
            "media.screen.ocr_run_observed",
        ),
    ] {
        let Some(contract) = find_event_contract(contract_id) else {
            panic!("missing media EventContract {contract_id}");
        };
        assert_eq!(contract.event_source, source);
        assert_eq!(contract.event_type, event_type);
        assert!(
            contract.package_refs.contains(&package_id),
            "{contract_id} should be emitted by package {package_id}"
        );
        assert!(contract.is_canonical_event());
        assert_eq!(
            contract.admission_policy_ref,
            Some(STANDARD_EVENT_ADMISSION_POLICY_ID)
        );

        match contract.payload_schema {
            PayloadSchemaContract::PayloadInventory {
                source: schema_source,
                event_type: schema_event_type,
                version,
            } => {
                assert_eq!(schema_source, source);
                assert_eq!(schema_event_type, event_type);
                assert_eq!(version, "1.0.0");
            }
            PayloadSchemaContract::ExplicitSchemaId { schema_id } => {
                panic!(
                    "media EventContract {contract_id} should use payload inventory, got {schema_id}"
                )
            }
        }

        let accepted_by_standard = admission_policies().any(|policy| {
            policy.id == STANDARD_EVENT_ADMISSION_POLICY_ID
                && policy.accepts_event_contract(contract_id)
        });
        assert!(
            accepted_by_standard,
            "{contract_id} must be admission-addressable"
        );
    }

    Ok(())
}
