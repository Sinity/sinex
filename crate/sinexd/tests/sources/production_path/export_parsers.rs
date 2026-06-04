//! Production-path obligation tests for file/export parsers.
//!
//! These cases upgrade source contracts that previously had parser-only fixture
//! coverage into the shared source host obligation harness.

#[cfg(test)]
mod tests {
    use xtask::sandbox::prelude::*;

    const RAINDROP_CSV: &[u8] = b"\
id,title,note,excerpt,url,folder,tags,created,cover,highlights,favorite
100,Page Alpha,,Short note on alpha,https://example.com/alpha,Folder A,\"rust,async\",2026-01-01T09:00:00.000Z,https://cdn.example.com/a.jpg,,false
";

    const SPOTIFY_JSON: &[u8] = br#"[
      {
        "ts": "2024-06-01T10:00:00Z",
        "ms_played": 240000,
        "platform": "Linux",
        "conn_country": "PL",
        "master_metadata_track_name": "Living In The Past",
        "master_metadata_album_artist_name": "Jethro Tull",
        "master_metadata_album_album_name": "Living In The Past",
        "spotify_track_uri": "spotify:track:AAQQ1",
        "reason_start": "trackdone",
        "reason_end": "trackdone",
        "shuffle": false,
        "skipped": false,
        "offline": false,
        "incognito_mode": false
      }
    ]"#;

    const HLEDGER_JOURNAL: &[u8] = b"\
2017-08-05 BP Buczkowice|LPG
    \tAssets:Checking:Revolut
    \tExpenses:Transport:Fuel                              52.97 PLN

";

    const MESSENGER_THREAD: &[u8] = br#"{
      "participants": ["Alice", "Bob"],
      "threadName": "Alice_Bob_thread",
      "messages": [
        {
          "isUnsent": false,
          "media": [],
          "reactions": [],
          "senderName": "Alice",
          "text": "Hey, how are you?",
          "timestamp": 1710000000000,
          "type": "text"
        }
      ]
    }"#;

    #[sinex_test]
    async fn raindrop_bookmarks_obligations() -> TestResult<()> {
        let failures = crate::_run_case(
            "raindrop-bookmarks",
            crate::AdapterKind::StaticFile,
            RAINDROP_CSV,
            &["bookmark.created"],
            crate::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "raindrop-bookmarks obligations failed: {failures:#?}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn spotify_extended_history_obligations() -> TestResult<()> {
        let failures = crate::_run_case(
            "spotify-extended-history",
            crate::AdapterKind::StaticFile,
            SPOTIFY_JSON,
            &["track.played"],
            crate::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "spotify-extended-history obligations failed: {failures:#?}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn hledger_journal_obligations() -> TestResult<()> {
        let failures = crate::_run_case(
            "hledger-journal",
            crate::AdapterKind::StaticFile,
            HLEDGER_JOURNAL,
            &["transaction.posted"],
            crate::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "hledger-journal obligations failed: {failures:#?}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn facebook_messenger_thread_obligations() -> TestResult<()> {
        let failures = crate::_run_case(
            "facebook-messenger-thread",
            crate::AdapterKind::StaticFile,
            MESSENGER_THREAD,
            &["message.sent"],
            crate::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "facebook-messenger-thread obligations failed: {failures:#?}"
        );
        Ok(())
    }
}
