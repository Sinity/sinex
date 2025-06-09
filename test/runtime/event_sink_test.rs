use sinex_shared::{EventSink, LogSink, MemorySink, FileSink, MultiSink, event_types::RawEventBuilder};
use std::sync::Arc;


#[tokio::test]
async fn test_file_sink() {
    use tempfile::NamedTempFile;
    use sinex_db::models::RawEvent;
    
    let temp_file = NamedTempFile::new().unwrap();
    let file_path = temp_file.path().to_path_buf();
    
    let sink = Arc::new(FileSink::new(file_path.clone()).await.unwrap());
    
    let event = RawEventBuilder::new(
        "test",
        "test.file",
        serde_json::json!({ "data": 42 }),
    ).build();
    
    sink.send_event(&event).await.unwrap();
    
    // Read back the file
    let content = tokio::fs::read_to_string(&file_path).await.unwrap();
    let saved_event: RawEvent = serde_json::from_str(content.trim()).unwrap();
    assert_eq!(saved_event.event_type, "test.file");
}

#[tokio::test]
async fn test_multi_sink() {
    let memory_sink = Arc::new(MemorySink::new());
    let log_sink = Arc::new(LogSink::new("MULTI"));
    
    // Need to clone the Arcs for the multi-sink
    let multi_sink = Arc::new(MultiSink::new(vec![
        Box::new(memory_sink.clone()) as Box<dyn EventSink>,
        Box::new(log_sink) as Box<dyn EventSink>,
    ]));
    
    let event = RawEventBuilder::new(
        "test",
        "test.multi",
        serde_json::json!({ "multi": true }),
    ).build();
    
    multi_sink.send_event(&event).await.unwrap();
    
    // Check memory sink got the event
    let events = memory_sink.get_events().await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_type, "test.multi");
}

#[tokio::test]
async fn test_batch_send() {
    let sink = Arc::new(MemorySink::new());
    
    let events: Vec<_> = (0..5).map(|i| {
        RawEventBuilder::new(
            "test",
            "test.batch",
            serde_json::json!({ "index": i }),
        ).build()
    }).collect();
    
    sink.send_batch(&events).await.unwrap();
    
    let stored = sink.get_events().await;
    assert_eq!(stored.len(), 5);
    for (i, event) in stored.iter().enumerate() {
        assert_eq!(event.payload["index"], i);
    }
}