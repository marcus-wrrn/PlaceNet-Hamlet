//! Cloud gateway MQTT client service.
//!
//! Replaces the WebSocket `CloudGatewayService` for the *startup* connection to
//! the cloud gateway. It logs in over HTTPS, connects to the gateway's broker
//! over MQTTS, and publishes an alive notification. The connection lifecycle
//! (login → connect → backoff → re-login) lives in [`super::tasks`].

use std::path::PathBuf;

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};

use crate::services::mqtt_client::MqttMessage;
use crate::supervisor::ManagedService;

use super::tasks::spawn_connection_task;

const OUTBOUND_CAPACITY: usize = 64;

/// Configuration for the cloud gateway MQTT connection.
#[derive(Debug, Clone)]
pub struct GatewayMqttConfig {
    /// Base URL of the gateway HTTPS API (e.g. `https://gateway.example.com:8443`).
    pub login_url: String,
    pub username: String,
    pub password: String,
    /// Pinned CA for the gateway broker / API. `None` → webpki roots.
    pub broker_cafile: Option<PathBuf>,
    /// Connect to the broker over TLS (false only for local plaintext testing).
    pub broker_tls: bool,
}

/// Cloneable handle for publishing to the gateway (e.g. on the `cmds` topic).
#[derive(Clone)]
pub struct GatewayMqttHandle {
    tx: mpsc::Sender<MqttMessage>,
}

impl GatewayMqttHandle {
    /// Enqueue an outbound publish. Returns an error if the service has stopped.
    pub async fn publish(&self, topic: impl Into<String>, payload: impl Into<Vec<u8>>) -> Result<(), String> {
        self.tx
            .send(MqttMessage { topic: topic.into(), payload: payload.into().into() })
            .await
            .map_err(|_| "gateway mqtt channel closed".to_string())
    }
}

pub struct GatewayMqttService {
    config: GatewayMqttConfig,
    outbound_tx: mpsc::Sender<MqttMessage>,
    outbound_rx: Option<mpsc::Receiver<MqttMessage>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl GatewayMqttService {
    pub fn new(config: GatewayMqttConfig) -> Self {
        let (outbound_tx, outbound_rx) = mpsc::channel(OUTBOUND_CAPACITY);
        Self {
            config,
            outbound_tx,
            outbound_rx: Some(outbound_rx),
            shutdown_tx: None,
        }
    }

    pub fn handle(&self) -> GatewayMqttHandle {
        GatewayMqttHandle { tx: self.outbound_tx.clone() }
    }
}

#[async_trait]
impl ManagedService for GatewayMqttService {
    async fn start(&mut self) -> Result<u32, String> {
        let outbound_rx = self
            .outbound_rx
            .take()
            .ok_or("GatewayMqttService already started")?;

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        self.shutdown_tx = Some(shutdown_tx);

        spawn_connection_task(self.config.clone(), outbound_rx, shutdown_rx);
        Ok(0)
    }

    async fn stop(&mut self) -> Result<(), String> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        Ok(())
    }

    async fn is_running(&mut self) -> bool {
        self.shutdown_tx.is_some()
    }
}
