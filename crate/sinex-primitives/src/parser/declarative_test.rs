    use super::*;
    use sinex_primitives::Id;
    use sinex_primitives::parser::MaterialAnchor;
    use xtask::sandbox::prelude::sinex_test;

    fn test_ctx() -> ParserContext {
        ParserContext {
            source_id: SourceId::from_static("test.unit"),
            source_material_id: Id::from_uuid(uuid::Uuid::nil()),
            record_anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
            operation_id: uuid::Uuid::nil(),
            job_id: uuid::Uuid::nil(),
            host: "test-host".into(),
            acquisition_time: Timestamp::now(),
        }
    }

    fn json_record(json: &str) -> SourceRecord {
        SourceRecord {
            material_id: Id::from_uuid(uuid::Uuid::nil()),
            anchor: MaterialAnchor::ByteRange {
                start: 0,
                len: json.len() as u64,
            },
            bytes: json.as_bytes().to_vec(),
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        }
    }

    fn minimal_spec() -> DeclarativeParserSpec {
        DeclarativeParserSpec {
            parser_id: ParserId::from_static("test-parser"),
            parser_version: "1.0.0".into(),
            source_id: SourceId::from_static("test.unit"),
            event_source: EventSource::from_static("test"),
            event_type: EventType::from_static("test.event"),
            default_privacy_context: ProcessingContext::Metadata,
            input_format: InputFormat::Json,
            fields: vec![],
            discriminator: None,
        }
    }

    #[sinex_test]
    async fn required_input_keys_follow_declared_field_sources() -> xtask::sandbox::TestResult<()> {
        let mut spec = minimal_spec();
        spec.fields = vec![
            FieldSpec {
                name: "command".into(),
                source: FieldSource::JsonPointer {
                    pointer: "/cmd".into(),
                },
                field_type: FieldType::String,
                required: true,
                default: None,
                skip_payload: false,
                privacy_context: None,
                sensitivity: Vec::new(),
                occurrence_key: false,
                timestamp: None,
                suppress_if: None,
                carry: None,
                transform: None,
                validate: None,
            },
            FieldSpec {
                name: "optional".into(),
                source: FieldSource::JsonPointer {
                    pointer: "/optional".into(),
                },
                field_type: FieldType::String,
                required: false,
                default: None,
                skip_payload: false,
                privacy_context: None,
                sensitivity: Vec::new(),
                occurrence_key: false,
                timestamp: None,
                suppress_if: None,
                carry: None,
                transform: None,
                validate: None,
            },
            FieldSpec {
                name: "line".into(),
                source: FieldSource::RawLine,
                field_type: FieldType::String,
                required: true,
                default: None,
                skip_payload: false,
                privacy_context: None,
                sensitivity: Vec::new(),
                occurrence_key: false,
                timestamp: None,
                suppress_if: None,
                carry: None,
                transform: None,
                validate: None,
            },
        ];

        assert_eq!(spec.required_input_keys(), vec!["/cmd"]);
        Ok(())
    }

    #[sinex_test]
    async fn positional_required_input_key_uses_fingerprint_column_name()
    -> xtask::sandbox::TestResult<()> {
        let mut spec = minimal_spec();
        spec.input_format = InputFormat::TabSeparated;
        spec.fields.push(FieldSpec {
            name: "command".into(),
            source: FieldSource::ColumnIndex { index: 2 },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            sensitivity: Vec::new(),
            occurrence_key: false,
            timestamp: None,
            suppress_if: None,
            carry: None,
            transform: None,
            validate: None,
        });

        assert_eq!(spec.required_input_keys(), vec!["column_2"]);
        Ok(())
    }

    #[sinex_test]
    async fn empty_spec_emits_one_event_with_empty_payload() -> xtask::sandbox::TestResult<()> {
        let intents = DeclarativeParser::evaluate(
            &minimal_spec(),
            &json_record("{}"),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].payload, serde_json::json!({}));
        Ok(())
    }

    #[sinex_test]
    async fn json_pointer_extracts_string_field() -> xtask::sandbox::TestResult<()> {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "command".into(),
            source: FieldSource::JsonPointer {
                pointer: "/cmd".into(),
            },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            sensitivity: Vec::new(),
            occurrence_key: false,
            timestamp: None,
            suppress_if: None,
            carry: None,
            transform: None,
            validate: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            &json_record(r#"{"cmd": "ls -la"}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert_eq!(intents[0].payload["command"], "ls -la");
        Ok(())
    }

    #[sinex_test]
    async fn missing_required_field_errors() -> xtask::sandbox::TestResult<()> {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "cmd".into(),
            source: FieldSource::JsonPointer {
                pointer: "/cmd".into(),
            },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            sensitivity: Vec::new(),
            occurrence_key: false,
            timestamp: None,
            suppress_if: None,
            carry: None,
            transform: None,
            validate: None,
        });
        let result = DeclarativeParser::evaluate(
            &spec,
            &json_record(r"{}"),
            &test_ctx(),
            &BindingConfig::default(),
        );
        assert!(result.is_err());
        Ok(())
    }

    #[sinex_test]
    async fn missing_optional_field_uses_default() -> xtask::sandbox::TestResult<()> {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "exit".into(),
            source: FieldSource::JsonPointer {
                pointer: "/exit".into(),
            },
            field_type: FieldType::Integer,
            required: false,
            default: Some(serde_json::json!(0)),
            skip_payload: false,
            privacy_context: None,
            sensitivity: Vec::new(),
            occurrence_key: false,
            timestamp: None,
            suppress_if: None,
            carry: None,
            transform: None,
            validate: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            &json_record(r"{}"),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert_eq!(intents[0].payload["exit"], 0);
        Ok(())
    }

    #[sinex_test]
    async fn missing_optional_no_default_omits_field() -> xtask::sandbox::TestResult<()> {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "exit".into(),
            source: FieldSource::JsonPointer {
                pointer: "/exit".into(),
            },
            field_type: FieldType::Integer,
            required: false,
            default: None,
            skip_payload: false,
            privacy_context: None,
            sensitivity: Vec::new(),
            occurrence_key: false,
            timestamp: None,
            suppress_if: None,
            carry: None,
            transform: None,
            validate: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            &json_record(r"{}"),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert!(intents[0].payload.get("exit").is_none());
        Ok(())
    }

    #[sinex_test]
    async fn skip_payload_excludes_from_output() -> xtask::sandbox::TestResult<()> {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "internal".into(),
            source: FieldSource::JsonPointer {
                pointer: "/internal".into(),
            },
            field_type: FieldType::Integer,
            required: true,
            default: None,
            skip_payload: true,
            privacy_context: None,
            sensitivity: Vec::new(),
            occurrence_key: false,
            timestamp: None,
            suppress_if: None,
            carry: None,
            transform: None,
            validate: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            &json_record(r#"{"internal": 42}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert!(intents[0].payload.get("internal").is_none());
        Ok(())
    }

    #[sinex_test]
    async fn occurrence_key_concatenates_fields_in_declared_order() -> xtask::sandbox::TestResult<()>
    {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "session".into(),
            source: FieldSource::JsonPointer {
                pointer: "/session".into(),
            },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            sensitivity: Vec::new(),
            occurrence_key: true,
            timestamp: None,
            suppress_if: None,
            carry: None,
            transform: None,
            validate: None,
        });
        spec.fields.push(FieldSpec {
            name: "id".into(),
            source: FieldSource::JsonPointer {
                pointer: "/id".into(),
            },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            sensitivity: Vec::new(),
            occurrence_key: true,
            timestamp: None,
            suppress_if: None,
            carry: None,
            transform: None,
            validate: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            &json_record(r#"{"session": "abc", "id": "evt-1"}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        let key = intents[0].occurrence_key.as_ref().unwrap();
        assert_eq!(
            key.fields,
            vec![
                ("session".into(), "abc".into()),
                ("id".into(), "evt-1".into())
            ]
        );
        Ok(())
    }

    #[sinex_test]
    async fn suppress_if_field_drops_field_only() -> xtask::sandbox::TestResult<()> {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "command".into(),
            source: FieldSource::JsonPointer {
                pointer: "/cmd".into(),
            },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: Some(ProcessingContext::Command),
            sensitivity: Vec::new(),
            occurrence_key: false,
            timestamp: None,
            suppress_if: Some(SuppressPredicate {
                binding_field: "private_mode_active".into(),
                whole_event: false,
            }),
            carry: None,
            transform: None,
            validate: None,
        });
        let binding = BindingConfig::new().with_flag("private_mode_active", true);
        let intents = DeclarativeParser::evaluate(
            &spec,
            &json_record(r#"{"cmd": "secret"}"#),
            &test_ctx(),
            &binding,
        )
        .unwrap();
        assert!(intents[0].payload.get("command").is_none());
        Ok(())
    }

    #[sinex_test]
    async fn suppress_if_whole_event_drops_event_entirely() -> xtask::sandbox::TestResult<()> {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "command".into(),
            source: FieldSource::JsonPointer {
                pointer: "/cmd".into(),
            },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: Some(ProcessingContext::Command),
            sensitivity: Vec::new(),
            occurrence_key: false,
            timestamp: None,
            suppress_if: Some(SuppressPredicate {
                binding_field: "private_mode_active".into(),
                whole_event: true,
            }),
            carry: None,
            transform: None,
            validate: None,
        });
        let binding = BindingConfig::new().with_flag("private_mode_active", true);
        let intents = DeclarativeParser::evaluate(
            &spec,
            &json_record(r#"{"cmd": "secret"}"#),
            &test_ctx(),
            &binding,
        )
        .unwrap();
        assert_eq!(intents.len(), 0);
        Ok(())
    }

    #[sinex_test]
    async fn suppress_if_inactive_passes_through() -> xtask::sandbox::TestResult<()> {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "command".into(),
            source: FieldSource::JsonPointer {
                pointer: "/cmd".into(),
            },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: Some(ProcessingContext::Command),
            sensitivity: Vec::new(),
            occurrence_key: false,
            timestamp: None,
            suppress_if: Some(SuppressPredicate {
                binding_field: "private_mode_active".into(),
                whole_event: false,
            }),
            carry: None,
            transform: None,
            validate: None,
        });
        let binding = BindingConfig::new().with_flag("private_mode_active", false);
        let intents = DeclarativeParser::evaluate(
            &spec,
            &json_record(r#"{"cmd": "ls"}"#),
            &test_ctx(),
            &binding,
        )
        .unwrap();
        assert_eq!(intents[0].payload["command"], "ls");
        Ok(())
    }

    #[sinex_test]
    async fn type_coercion_string_to_integer_works() -> xtask::sandbox::TestResult<()> {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "exit".into(),
            source: FieldSource::JsonPointer {
                pointer: "/exit".into(),
            },
            field_type: FieldType::Integer,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            sensitivity: Vec::new(),
            occurrence_key: false,
            timestamp: None,
            suppress_if: None,
            carry: None,
            transform: None,
            validate: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            &json_record(r#"{"exit": "42"}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert_eq!(intents[0].payload["exit"], 42);
        Ok(())
    }

    #[sinex_test]
    async fn type_coercion_string_to_boolean_works() -> xtask::sandbox::TestResult<()> {
        for (input, expected) in [("true", true), ("false", false), ("1", true), ("yes", true)] {
            let mut spec = minimal_spec();
            spec.fields.push(FieldSpec {
                name: "flag".into(),
                source: FieldSource::JsonPointer {
                    pointer: "/flag".into(),
                },
                field_type: FieldType::Boolean,
                required: true,
                default: None,
                skip_payload: false,
                privacy_context: None,
                sensitivity: Vec::new(),
                occurrence_key: false,
                timestamp: None,
                suppress_if: None,
                carry: None,
                transform: None,
                validate: None,
            });
            let json = format!(r#"{{"flag": {input:?}}}"#);
            let intents = DeclarativeParser::evaluate(
                &spec,
                &json_record(&json),
                &test_ctx(),
                &BindingConfig::default(),
            )
            .unwrap();
            assert_eq!(intents[0].payload["flag"], expected, "input was {input:?}");
        }
        Ok(())
    }

    #[sinex_test]
    async fn timestamp_rfc3339_parses() -> xtask::sandbox::TestResult<()> {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "ts".into(),
            source: FieldSource::JsonPointer {
                pointer: "/ts".into(),
            },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            sensitivity: Vec::new(),
            occurrence_key: false,
            timestamp: Some(TimestampSpec {
                format: TimestampFormat::Rfc3339,
                fallback: TimestampFallback::Error,
            }),
            suppress_if: None,
            carry: None,
            transform: None,
            validate: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            &json_record(r#"{"ts": "2024-01-15T12:34:56Z"}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert!(matches!(
            &intents[0].timing,
            TimingEvidence::Intrinsic { field, .. } if field == "ts"
        ));
        Ok(())
    }

    #[sinex_test]
    async fn timestamp_unix_seconds_parses() -> xtask::sandbox::TestResult<()> {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "ts".into(),
            source: FieldSource::JsonPointer {
                pointer: "/ts".into(),
            },
            field_type: FieldType::Integer,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            sensitivity: Vec::new(),
            occurrence_key: false,
            timestamp: Some(TimestampSpec {
                format: TimestampFormat::UnixSeconds,
                fallback: TimestampFallback::Error,
            }),
            suppress_if: None,
            carry: None,
            transform: None,
            validate: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            &json_record(r#"{"ts": 1705320896}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert!(matches!(
            &intents[0].timing,
            TimingEvidence::Intrinsic { .. }
        ));
        Ok(())
    }

    #[sinex_test]
    async fn timestamp_invalid_falls_back_to_material_time() -> xtask::sandbox::TestResult<()> {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "ts".into(),
            source: FieldSource::JsonPointer {
                pointer: "/ts".into(),
            },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            sensitivity: Vec::new(),
            occurrence_key: false,
            timestamp: Some(TimestampSpec {
                format: TimestampFormat::Rfc3339,
                fallback: TimestampFallback::MaterialTiming,
            }),
            suppress_if: None,
            carry: None,
            transform: None,
            validate: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            &json_record(r#"{"ts": "not a timestamp"}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert!(matches!(
            &intents[0].timing,
            TimingEvidence::Intrinsic { .. }
        ));
        Ok(())
    }

    #[sinex_test]
    async fn no_timestamp_uses_acquisition_time_with_staged_fallback_evidence()
    -> xtask::sandbox::TestResult<()> {
        let intents = DeclarativeParser::evaluate(
            &minimal_spec(),
            &json_record("{}"),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert!(matches!(
            intents[0].timing,
            TimingEvidence::StagedAtFallback
        ));
        Ok(())
    }

    #[sinex_test]
    async fn tab_separated_extracts_by_index() -> xtask::sandbox::TestResult<()> {
        let mut spec = minimal_spec();
        spec.input_format = InputFormat::TabSeparated;
        spec.fields.push(FieldSpec {
            name: "first".into(),
            source: FieldSource::ColumnIndex { index: 0 },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            sensitivity: Vec::new(),
            occurrence_key: false,
            timestamp: None,
            suppress_if: None,
            carry: None,
            transform: None,
            validate: None,
        });
        spec.fields.push(FieldSpec {
            name: "third".into(),
            source: FieldSource::ColumnIndex { index: 2 },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            sensitivity: Vec::new(),
            occurrence_key: false,
            timestamp: None,
            suppress_if: None,
            carry: None,
            transform: None,
            validate: None,
        });
        let record = SourceRecord {
            material_id: Id::from_uuid(uuid::Uuid::nil()),
            anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
            bytes: b"alpha\tbeta\tgamma".to_vec(),
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        };
        let intents =
            DeclarativeParser::evaluate(&spec, &record, &test_ctx(), &BindingConfig::default())
                .unwrap();
        assert_eq!(intents[0].payload["first"], "alpha");
        assert_eq!(intents[0].payload["third"], "gamma");
        Ok(())
    }

    #[sinex_test]
    async fn binding_config_default_is_falsy() -> xtask::sandbox::TestResult<()> {
        let b = BindingConfig::default();
        assert!(!b.is_truthy("anything"));
        Ok(())
    }

    #[sinex_test]
    async fn binding_config_with_flag_is_truthy() -> xtask::sandbox::TestResult<()> {
        let b = BindingConfig::new().with_flag("on", true);
        assert!(b.is_truthy("on"));
        assert!(!b.is_truthy("off"));
        Ok(())
    }

    #[sinex_test]
    async fn record_anchor_passes_through_to_intent() -> xtask::sandbox::TestResult<()> {
        let record = SourceRecord {
            material_id: Id::from_uuid(uuid::Uuid::nil()),
            anchor: MaterialAnchor::SqliteRow {
                table: "history".into(),
                rowid: 42,
            },
            bytes: b"{}".to_vec(),
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        };
        let intents = DeclarativeParser::evaluate(
            &minimal_spec(),
            &record,
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert!(matches!(
            &intents[0].anchor,
            MaterialAnchor::SqliteRow { table, rowid: 42 } if table == "history"
        ));
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Coverage gaps filled (#1100 substrate hardening)
    // -----------------------------------------------------------------------

    #[sinex_test]
    async fn timestamp_invalid_with_error_fallback_rejects_record() -> xtask::sandbox::TestResult<()>
    {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "ts".into(),
            source: FieldSource::JsonPointer {
                pointer: "/ts".into(),
            },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            sensitivity: Vec::new(),
            occurrence_key: false,
            timestamp: Some(TimestampSpec {
                format: TimestampFormat::Rfc3339,
                fallback: TimestampFallback::Error,
            }),
            suppress_if: None,
            carry: None,
            transform: None,
            validate: None,
        });
        let result = DeclarativeParser::evaluate(
            &spec,
            &json_record(r#"{"ts": "not-a-real-date"}"#),
            &test_ctx(),
            &BindingConfig::default(),
        );
        assert!(matches!(result, Err(ParserError::Field(_))));
        Ok(())
    }

    #[sinex_test]
    async fn timestamp_unix_millis_distinguishable_from_seconds() -> xtask::sandbox::TestResult<()>
    {
        // Same numeric input under millis vs seconds yields different timestamps.
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "ts".into(),
            source: FieldSource::JsonPointer {
                pointer: "/ts".into(),
            },
            field_type: FieldType::Integer,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            sensitivity: Vec::new(),
            occurrence_key: false,
            timestamp: Some(TimestampSpec {
                format: TimestampFormat::UnixMillis,
                fallback: TimestampFallback::Error,
            }),
            suppress_if: None,
            carry: None,
            transform: None,
            validate: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            // 1_700_000_000_000 ms = 2023-11-14T22:13:20Z
            &json_record(r#"{"ts": 1700000000000}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert!(matches!(
            &intents[0].timing,
            TimingEvidence::Intrinsic { .. }
        ));
        let expected = Timestamp::from_unix_timestamp_millis(1_700_000_000_000).unwrap();
        assert_eq!(intents[0].ts_orig, expected);
        Ok(())
    }

    #[sinex_test]
    async fn timestamp_unix_micros_parses() -> xtask::sandbox::TestResult<()> {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "ts".into(),
            source: FieldSource::JsonPointer {
                pointer: "/ts".into(),
            },
            field_type: FieldType::Integer,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            sensitivity: Vec::new(),
            occurrence_key: false,
            timestamp: Some(TimestampSpec {
                format: TimestampFormat::UnixMicros,
                fallback: TimestampFallback::Error,
            }),
            suppress_if: None,
            carry: None,
            transform: None,
            validate: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            &json_record(r#"{"ts": 1700000000000000}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert!(matches!(
            intents[0].timing,
            TimingEvidence::Intrinsic { .. }
        ));
        Ok(())
    }

    #[sinex_test]
    async fn coerce_non_integer_string_errors() -> xtask::sandbox::TestResult<()> {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "count".into(),
            source: FieldSource::JsonPointer {
                pointer: "/count".into(),
            },
            field_type: FieldType::Integer,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            sensitivity: Vec::new(),
            occurrence_key: false,
            timestamp: None,
            suppress_if: None,
            carry: None,
            transform: None,
            validate: None,
        });
        let result = DeclarativeParser::evaluate(
            &spec,
            &json_record(r#"{"count": "not-a-number"}"#),
            &test_ctx(),
            &BindingConfig::default(),
        );
        assert!(matches!(result, Err(ParserError::Field(_))));
        Ok(())
    }

    #[sinex_test]
    async fn coerce_float_with_fraction_errors_for_integer() -> xtask::sandbox::TestResult<()> {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "n".into(),
            source: FieldSource::JsonPointer {
                pointer: "/n".into(),
            },
            field_type: FieldType::Integer,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            sensitivity: Vec::new(),
            occurrence_key: false,
            timestamp: None,
            suppress_if: None,
            carry: None,
            transform: None,
            validate: None,
        });
        // 3.14 must error for FieldType::Integer.
        let err = DeclarativeParser::evaluate(
            &spec,
            &json_record(r#"{"n": 3.14}"#),
            &test_ctx(),
            &BindingConfig::default(),
        );
        assert!(matches!(err, Err(ParserError::Field(_))));
        // 3.0 must coerce to integer 3.
        let ok = DeclarativeParser::evaluate(
            &spec,
            &json_record(r#"{"n": 3.0}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert_eq!(ok[0].payload["n"], 3);
        Ok(())
    }

    #[sinex_test]
    async fn invalid_utf8_record_errors_with_decode_variant() -> xtask::sandbox::TestResult<()> {
        let spec = minimal_spec();
        let record = SourceRecord {
            material_id: Id::from_uuid(uuid::Uuid::nil()),
            anchor: MaterialAnchor::ByteRange { start: 0, len: 2 },
            bytes: vec![0xFF, 0xFE], // not valid UTF-8
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        };
        let result =
            DeclarativeParser::evaluate(&spec, &record, &test_ctx(), &BindingConfig::default());
        assert!(matches!(result, Err(ParserError::Decode(_))));
        Ok(())
    }

    #[sinex_test]
    async fn suppress_if_whole_event_without_privacy_context_drops_event()
    -> xtask::sandbox::TestResult<()> {
        // Cover the `else if suppressed_by_predicate` branch: no privacy_context
        // but whole_event = true. Must produce zero intents.
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "secret".into(),
            source: FieldSource::JsonPointer {
                pointer: "/secret".into(),
            },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            sensitivity: Vec::new(),
            occurrence_key: false,
            timestamp: None,
            suppress_if: Some(SuppressPredicate {
                binding_field: "private_mode".into(),
                whole_event: true,
            }),
            carry: None,
            transform: None,
            validate: None,
        });
        let binding = BindingConfig::new().with_flag("private_mode", true);
        let intents = DeclarativeParser::evaluate(
            &spec,
            &json_record(r#"{"secret": "x"}"#),
            &test_ctx(),
            &binding,
        )
        .unwrap();
        assert!(
            intents.is_empty(),
            "whole_event suppression must yield no intents"
        );
        Ok(())
    }

    #[sinex_test]
    async fn mismatched_source_format_returns_field_error() -> xtask::sandbox::TestResult<()> {
        // TabSeparated input with a JsonPointer source should fail with a clear
        // "incompatible" error, not silently produce an empty value.
        let mut spec = minimal_spec();
        spec.input_format = InputFormat::TabSeparated;
        spec.fields.push(FieldSpec {
            name: "f".into(),
            source: FieldSource::JsonPointer {
                pointer: "/x".into(),
            },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            sensitivity: Vec::new(),
            occurrence_key: false,
            timestamp: None,
            suppress_if: None,
            carry: None,
            transform: None,
            validate: None,
        });
        let record = SourceRecord {
            material_id: Id::from_uuid(uuid::Uuid::nil()),
            anchor: MaterialAnchor::Line {
                byte_start: 0,
                line: 1,
            },
            bytes: b"a\tb\tc".to_vec(),
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        };
        let result =
            DeclarativeParser::evaluate(&spec, &record, &test_ctx(), &BindingConfig::default());
        assert!(matches!(result, Err(ParserError::Field(_))));
        Ok(())
    }

    #[sinex_test]
    async fn occurrence_key_with_skip_payload_contributes_key_but_not_payload()
    -> xtask::sandbox::TestResult<()> {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "rowid".into(),
            source: FieldSource::JsonPointer {
                pointer: "/rowid".into(),
            },
            field_type: FieldType::Integer,
            required: true,
            default: None,
            skip_payload: true,
            privacy_context: None,
            sensitivity: Vec::new(),
            occurrence_key: true,
            timestamp: None,
            suppress_if: None,
            carry: None,
            transform: None,
            validate: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            &json_record(r#"{"rowid": 7}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert!(intents[0].payload.get("rowid").is_none());
        let key = intents[0].occurrence_key.as_ref().expect("occurrence_key");
        assert_eq!(key.fields, vec![("rowid".into(), "7".into())]);
        Ok(())
    }

    #[sinex_test]
    async fn default_value_is_type_coerced_into_payload() -> xtask::sandbox::TestResult<()> {
        // A string-typed field with a numeric default should arrive in the
        // payload as a *string*, because coerce_field runs on the default.
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: "label".into(),
            source: FieldSource::JsonPointer {
                pointer: "/label".into(),
            },
            field_type: FieldType::String,
            required: false,
            default: Some(serde_json::json!(42)),
            skip_payload: false,
            privacy_context: None,
            sensitivity: Vec::new(),
            occurrence_key: false,
            timestamp: None,
            suppress_if: None,
            carry: None,
            transform: None,
            validate: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            &json_record(r"{}"), // label missing
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert_eq!(intents[0].payload["label"], "42");
        Ok(())
    }

    #[sinex_test]
    async fn csv_row_uses_column_name_extraction() -> xtask::sandbox::TestResult<()> {
        // CsvRow decodes bytes as JSON object; ColumnName extracts by key.
        let mut spec = minimal_spec();
        spec.input_format = InputFormat::CsvRow;
        spec.fields.push(FieldSpec {
            name: "col".into(),
            source: FieldSource::ColumnName { name: "col".into() },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            sensitivity: Vec::new(),
            occurrence_key: false,
            timestamp: None,
            suppress_if: None,
            carry: None,
            transform: None,
            validate: None,
        });
        let intents = DeclarativeParser::evaluate(
            &spec,
            &json_record(r#"{"col": "val"}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert_eq!(intents[0].payload["col"], "val");
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Extension G — validation / normalization hooks (#1750)
    // -----------------------------------------------------------------------

    /// Build a single-field JSON spec with a transform/validator for hook tests.
    fn spec_with_hook(
        name: &str,
        pointer: &str,
        field_type: FieldType,
        transform: Option<FieldTransform>,
        validate: Option<FieldValidator>,
    ) -> DeclarativeParserSpec {
        let mut spec = minimal_spec();
        spec.fields.push(FieldSpec {
            name: name.into(),
            source: FieldSource::JsonPointer {
                pointer: pointer.into(),
            },
            field_type,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            sensitivity: Vec::new(),
            occurrence_key: false,
            timestamp: None,
            suppress_if: None,
            carry: None,
            transform,
            validate,
        });
        spec
    }

    #[sinex_test]
    async fn transform_split_first_keeps_segment_before_separator() -> xtask::sandbox::TestResult<()>
    {
        // Atuin host:user -> host parity.
        let spec = spec_with_hook(
            "hostname",
            "/hostname",
            FieldType::String,
            Some(FieldTransform::SplitFirst {
                separator: ":".into(),
            }),
            None,
        );
        let intents = DeclarativeParser::evaluate(
            &spec,
            &json_record(r#"{"hostname": "myhost:myuser"}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert_eq!(intents[0].payload["hostname"], "myhost");
        Ok(())
    }

    #[sinex_test]
    async fn transform_split_first_without_separator_is_noop() -> xtask::sandbox::TestResult<()> {
        let spec = spec_with_hook(
            "hostname",
            "/hostname",
            FieldType::String,
            Some(FieldTransform::SplitFirst {
                separator: ":".into(),
            }),
            None,
        );
        let intents = DeclarativeParser::evaluate(
            &spec,
            &json_record(r#"{"hostname": "barehost"}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert_eq!(intents[0].payload["hostname"], "barehost");
        Ok(())
    }

    #[sinex_test]
    async fn validator_i32_accepts_in_range_and_rejects_overflow() -> xtask::sandbox::TestResult<()>
    {
        let spec = spec_with_hook(
            "exit",
            "/exit",
            FieldType::Integer,
            None,
            Some(FieldValidator::I32),
        );
        // In range.
        let ok = DeclarativeParser::evaluate(
            &spec,
            &json_record(r#"{"exit": 127}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert_eq!(ok[0].payload["exit"], 127);
        // Out of i32 range — rejected.
        let err = DeclarativeParser::evaluate(
            &spec,
            &json_record(r#"{"exit": 9999999999}"#),
            &test_ctx(),
            &BindingConfig::default(),
        );
        assert!(matches!(err, Err(ParserError::Field(_))));
        Ok(())
    }

    #[sinex_test]
    async fn validator_int_range_rejects_out_of_bounds() -> xtask::sandbox::TestResult<()> {
        let spec = spec_with_hook(
            "n",
            "/n",
            FieldType::Integer,
            None,
            Some(FieldValidator::IntRange { min: 0, max: 10 }),
        );
        assert!(
            DeclarativeParser::evaluate(
                &spec,
                &json_record(r#"{"n": 5}"#),
                &test_ctx(),
                &BindingConfig::default(),
            )
            .is_ok()
        );
        assert!(matches!(
            DeclarativeParser::evaluate(
                &spec,
                &json_record(r#"{"n": 11}"#),
                &test_ctx(),
                &BindingConfig::default(),
            ),
            Err(ParserError::Field(_))
        ));
        Ok(())
    }

    #[sinex_test]
    async fn validator_timestamp_nanos_accepts_integer_rejects_non_integer()
    -> xtask::sandbox::TestResult<()> {
        // Integer nanoseconds in range pass.
        let int_spec = spec_with_hook(
            "ts",
            "/ts",
            FieldType::Integer,
            None,
            Some(FieldValidator::TimestampNanos),
        );
        assert!(
            DeclarativeParser::evaluate(
                &int_spec,
                &json_record(r#"{"ts": 1700000000000000000}"#),
                &test_ctx(),
                &BindingConfig::default(),
            )
            .is_ok()
        );
        // A non-integer value (Json field carrying a string) is rejected.
        let bad_spec = spec_with_hook(
            "ts",
            "/ts",
            FieldType::Json,
            None,
            Some(FieldValidator::TimestampNanos),
        );
        assert!(matches!(
            DeclarativeParser::evaluate(
                &bad_spec,
                &json_record(r#"{"ts": "not-a-number"}"#),
                &test_ctx(),
                &BindingConfig::default(),
            ),
            Err(ParserError::Field(_))
        ));
        Ok(())
    }

    #[sinex_test]
    async fn validator_non_empty_string_rejects_blank() -> xtask::sandbox::TestResult<()> {
        let spec = spec_with_hook(
            "id",
            "/id",
            FieldType::String,
            None,
            Some(FieldValidator::NonEmptyString),
        );
        assert!(
            DeclarativeParser::evaluate(
                &spec,
                &json_record(r#"{"id": "abc"}"#),
                &test_ctx(),
                &BindingConfig::default(),
            )
            .is_ok()
        );
        assert!(matches!(
            DeclarativeParser::evaluate(
                &spec,
                &json_record(r#"{"id": "   "}"#),
                &test_ctx(),
                &BindingConfig::default(),
            ),
            Err(ParserError::Field(_))
        ));
        Ok(())
    }

    #[sinex_test]
    async fn transform_then_validate_applies_in_order() -> xtask::sandbox::TestResult<()> {
        // SplitFirst runs before NonEmptyString: "h:u" -> "h" passes;
        // ":only-after" -> "" fails the non-empty validator.
        let spec = spec_with_hook(
            "hostname",
            "/hostname",
            FieldType::String,
            Some(FieldTransform::SplitFirst {
                separator: ":".into(),
            }),
            Some(FieldValidator::NonEmptyString),
        );
        let ok = DeclarativeParser::evaluate(
            &spec,
            &json_record(r#"{"hostname": "h:u"}"#),
            &test_ctx(),
            &BindingConfig::default(),
        )
        .unwrap();
        assert_eq!(ok[0].payload["hostname"], "h");
        assert!(matches!(
            DeclarativeParser::evaluate(
                &spec,
                &json_record(r#"{"hostname": ":only-after"}"#),
                &test_ctx(),
                &BindingConfig::default(),
            ),
            Err(ParserError::Field(_))
        ));
        Ok(())
    }
