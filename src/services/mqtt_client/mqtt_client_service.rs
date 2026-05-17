use async_trait::async_trait;
use rumqttc::{AsyncClient, MqttOptions, QoS, Transport};
use tokio::sync::{mpsc, oneshot};

use crate::config::MqttClientConfig;
use crate::supervisor::ManagedService;
use super::internals::{MqttCommand, MqttMessageSender, MqttOutboundReceiver, TopicSubscription};
use super::tasks;

#[derive(Clone)]
pub struct MqttClientHandle {
    tx: mpsc::Sender<MqttCommand>,
}

impl MqttClientHandle {
    /// Sends a subscribe request to the MQTT client for the given topic and QoS level.
    /// Waits for acknowledgement from the client task before returning.
    pub async fn subscribe(&self, topic: impl Into<String>, qos: QoS) -> Result<(), String> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(MqttCommand::Subscribe { topic: topic.into(), qos, reply })
            .await
            .map_err(|_| "mqtt client channel closed".to_string())?;
        rx.await.map_err(|_| "mqtt client dropped reply".to_string())?
    }

    /// Subscribes to every topic in `subscriptions` in order, returning the first error encountered.
    pub async fn subscribe_all(&self, subscriptions: &[TopicSubscription]) -> Result<(), String> {
        for sub in subscriptions {
            self.subscribe(sub.topic, sub.qos).await?;
        }
        Ok(())
    }

    /// Sends an unsubscribe request to the MQTT client for the given topic.
    /// Waits for acknowledgement from the client task before returning.
    pub async fn unsubscribe(&self, topic: impl Into<String>) -> Result<(), String> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(MqttCommand::Unsubscribe { topic: topic.into(), reply })
            .await
            .map_err(|_| "mqtt client channel closed".to_string())?;
        rx.await.map_err(|_| "mqtt client dropped reply".to_string())?
    }

    /// Sends a publish request to the MQTT client for the given topic, QoS level, and payload.
    /// Waits for acknowledgement from the client task before returning.
    pub async fn publish(
        &self,
        topic: impl Into<String>,
        qos: QoS,
        payload: impl Into<Vec<u8>>,
    ) -> Result<(), String> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(MqttCommand::Publish {
                topic: topic.into(),
                qos,
                payload: payload.into(),
                reply,
            })
            .await
            .map_err(|_| "mqtt client channel closed".to_string())?;
        rx.await.map_err(|_| "mqtt client dropped reply".to_string())?
    }
}

pub struct MqttClientService {
    config: MqttClientConfig,
    cmd_tx: mpsc::Sender<MqttCommand>,
    cmd_rx: Option<mpsc::Receiver<MqttCommand>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    cmd_shutdown_tx: Option<oneshot::Sender<()>>,
    msg_tx: MqttMessageSender,
    out_rx: Option<MqttOutboundReceiver>,
    connected_tx: Option<oneshot::Sender<()>>,
}

impl MqttClientService {
    /// Creates a new `MqttClientService` with the provided configuration and pre-constructed
    /// channel endpoints. The service is not started until [`ManagedService::start`] is called.
    pub fn new(
        config: MqttClientConfig,
        cmd_tx: mpsc::Sender<MqttCommand>,
        cmd_rx: mpsc::Receiver<MqttCommand>,
        msg_tx: MqttMessageSender,
        out_rx: MqttOutboundReceiver,
        connected_tx: oneshot::Sender<()>,
    ) -> Self {
        Self {
            config,
            cmd_tx,
            cmd_rx: Some(cmd_rx),
            shutdown_tx: None,
            cmd_shutdown_tx: None,
            msg_tx,
            out_rx: Some(out_rx),
            connected_tx: Some(connected_tx),
        }
    }

    /// Returns a cloneable [`MqttClientHandle`] that can be used to send commands to this service.
    pub fn handle(&self) -> MqttClientHandle {
        MqttClientHandle { tx: self.cmd_tx.clone() }
    }

    fn take_channels(
        &mut self,
    ) -> Result<(mpsc::Receiver<MqttCommand>, MqttOutboundReceiver, oneshot::Sender<()>), String> {
        let cmd_rx = self
            .cmd_rx
            .take()
            .ok_or("MqttClientService command receiver already consumed")?;
        let out_rx = self
            .out_rx
            .take()
            .ok_or("MqttClientService outbound receiver already consumed")?;
        let connected_tx = self
            .connected_tx
            .take()
            .ok_or("MqttClientService connected sender already consumed")?;
        Ok((cmd_rx, out_rx, connected_tx))
    }

    async fn build_mqtt_opts(&self) -> Result<MqttOptions, String> {
        let mut opts =
            MqttOptions::new(&self.config.client_id, &self.config.host, self.config.port);
        opts.set_keep_alive(std::time::Duration::from_secs(30));

        if self.config.tls_enabled {
            let ca = tokio::fs::read(&self.config.cafile).await.map_err(|e| {
                format!("Failed to read CA cert '{}': {}", self.config.cafile.display(), e)
            })?;
            let cert = tokio::fs::read(&self.config.certfile).await.map_err(|e| {
                format!("Failed to read client cert '{}': {}", self.config.certfile.display(), e)
            })?;
            let key = tokio::fs::read(&self.config.keyfile).await.map_err(|e| {
                format!("Failed to read client key '{}': {}", self.config.keyfile.display(), e)
            })?;
            opts.set_transport(Transport::tls(ca, Some((cert, key)), None));
        } else {
            opts.set_credentials(&self.config.username, &self.config.password);
        }

        Ok(opts)
    }

}

#[async_trait]
impl ManagedService for MqttClientService {
    async fn start(&mut self) -> Result<u32, String> {
        if self.shutdown_tx.is_some() {
            return Err("MqttClientService is already running".to_string());
        }

        let (cmd_rx, out_rx, connected_tx) = self.take_channels()?;
        let msg_tx = self.msg_tx.clone();

        let opts = self.build_mqtt_opts().await?;
        let (client, eventloop) = AsyncClient::new(opts, 10);

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let (cmd_shutdown_tx, cmd_shutdown_rx) = oneshot::channel::<()>();
        self.shutdown_tx = Some(shutdown_tx);
        self.cmd_shutdown_tx = Some(cmd_shutdown_tx);

        tasks::spawn_eventloop_task(client.clone(), eventloop, out_rx, msg_tx, shutdown_rx, Some(connected_tx));
        tasks::spawn_command_task(client, cmd_rx, cmd_shutdown_rx);

        Ok(0)
    }

    async fn stop(&mut self) -> Result<(), String> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
            if let Some(cmd_tx) = self.cmd_shutdown_tx.take() {
                let _ = cmd_tx.send(());
            }
            Ok(())
        } else {
            Err("MqttClientService is not running".to_string())
        }
    }

    async fn is_running(&mut self) -> bool {
        self.shutdown_tx.is_some()
    }
}
