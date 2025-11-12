use std::{collections::VecDeque, sync::Arc};

use async_trait::async_trait;
use color_eyre::Result;
use serde_json::json;
use sinex_gateway::{
    native_messaging::{
        run_with_transport, NativeMessage, NativeMessagingConfig, NativeMessagingTransport,
        NativeResponse,
    },
    ServiceContainer,
};
use sinex_test_utils::{sinex_test, TestContext};
use tokio::sync::Mutex;

#[derive(Clone, Default)]
struct HarnessTransport {
    state: Arc<Mutex<TransportState>>,
}

#[derive(Default)]
struct TransportState {
    inbox: VecDeque<NativeMessage>,
    outbox: Vec<NativeResponse>,
}

impl HarnessTransport {
    fn new(messages: Vec<NativeMessage>) -> Self {
        Self {
            state: Arc::new(Mutex::new(TransportState {
                inbox: VecDeque::from(messages),
                outbox: Vec::new(),
            })),
        }
    }

    async fn responses(&self) -> Vec<NativeResponse> {
        let state = self.state.lock().await;
        state.outbox.clone()
    }
}

#[async_trait]
impl NativeMessagingTransport for HarnessTransport {
    async fn read_message(&mut self) -> Result<Option<NativeMessage>> {
        let mut state = self.state.lock().await;
        Ok(state.inbox.pop_front())
    }

    async fn write_message(&mut self, response: &NativeResponse) -> Result<()> {
        let mut state = self.state.lock().await;
        state.outbox.push(response.clone());
        Ok(())
    }
}

#[sinex_test]
async fn native_messaging_rejects_untrusted_extensions(ctx: TestContext) -> Result<()> {
    let db_url = ctx.database_url().to_string();
    let services = ServiceContainer::new(Some(db_url)).await?;

    let config = NativeMessagingConfig::with_trusted_extensions([
        "chrome-extension://trusted-sinex".to_string(),
    ]);

    let malicious_request = NativeMessage::rpc(
        "analytics.event_count_by_source",
        json!({ "days_back": 1 }),
        "1",
    )
    .with_extension_id("chrome-extension://malicious");

    let transport = HarnessTransport::new(vec![malicious_request]);
    let probe = transport.clone();

    run_with_transport(services, config, transport).await?;

    let responses = probe.responses().await;
    assert!(
        !responses.is_empty(),
        "native messaging should have responded to RPC request"
    );

    let first = &responses[0];
    assert_eq!(
        first.message_type(),
        "error",
        "native messaging should reject RPC calls from extension IDs that are not in the trusted allow-list"
    );

    Ok(())
}
