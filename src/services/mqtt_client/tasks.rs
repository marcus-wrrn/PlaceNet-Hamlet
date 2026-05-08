use rumqttc::{AsyncClient, Event, EventLoop, Packet, QoS};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

use super::mqtt_client_service::{MqttCommand, MqttMessage, MqttMessageSender, MqttOutboundReceiver};

/// Spawns the MQTT event loop task, which drives three concurrent concerns:
/// - **Shutdown**: stops the client and exits cleanly when `shutdown_rx` fires.
/// - **Outbound publish**: drains `out_rx` and publishes each message to the broker.
/// - **Inbound events**: polls the rumqttc `EventLoop`; forwards `Publish` packets to
///   `msg_tx`, signals connection readiness on the first `ConnAck`, and logs errors
///   with a 5-second back-off before retrying.
pub fn spawn_eventloop_task(
    client: AsyncClient,
    mut eventloop: EventLoop,
    mut out_rx: MqttOutboundReceiver,
    msg_tx: MqttMessageSender,
    mut shutdown_rx: oneshot::Receiver<()>,
    mut connected_tx: Option<oneshot::Sender<()>>,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    info!("MQTT client eventloop shutting down");
                    let _ = client.disconnect().await;
                    break;
                }
                Some(msg) = out_rx.recv() => {
                    let payload_str = std::str::from_utf8(&msg.payload).unwrap_or("<binary>");
                    debug!(topic = %msg.topic, payload = %payload_str, "MQTT outbound publish");
                    if let Err(e) = client
                        .publish(&msg.topic, QoS::AtLeastOnce, false, msg.payload)
                        .await
                    {
                        error!(topic = %msg.topic, error = %e, "MQTT outbound publish failed");
                    }
                }
                poll = eventloop.poll() => {
                    match poll {
                        Ok(Event::Incoming(Packet::Publish(publish))) => {
                            let payload_str = std::str::from_utf8(&publish.payload).unwrap_or("<binary>");
                            info!(topic = %publish.topic, "MQTT inbound message received");
                            debug!(topic = %publish.topic, payload = %payload_str, "MQTT inbound message payload");
                            let msg = MqttMessage {
                                topic: publish.topic.clone(),
                                payload: publish.payload.clone(),
                            };
                            if let Err(e) = msg_tx.send(msg).await {
                                error!(
                                    topic = %publish.topic,
                                    "MQTT inbound queue full or closed: {}",
                                    e
                                );
                            }
                        }
                        Ok(Event::Incoming(Packet::ConnAck(_))) => {
                            info!("MQTT client connected to broker");
                            if let Some(tx) = connected_tx.take() {
                                let _ = tx.send(());
                            }
                        }
                        Ok(Event::Incoming(Packet::Disconnect)) => {
                            warn!("MQTT broker disconnected");
                        }
                        Ok(_) => {}
                        Err(e) => {
                            error!("MQTT eventloop error: {}", e);
                            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        }
                    }
                }
            }
        }
    });
}

/// Spawns the MQTT command task, which serializes control operations against the broker.
/// Receives [`MqttCommand`] variants over `cmd_rx` and dispatches them to the rumqttc
/// `AsyncClient`: `Subscribe`, `Unsubscribe`, and `Publish` each return their result via
/// a oneshot reply channel. A `Shutdown` command terminates the loop.
pub fn spawn_command_task(client: AsyncClient, mut cmd_rx: mpsc::Receiver<MqttCommand>) {
    tokio::spawn(async move {
        while let Some(cmd) = cmd_rx.recv().await {
            match cmd {
                MqttCommand::Subscribe { topic, qos, reply } => {
                    let result = client
                        .subscribe(&topic, qos)
                        .await
                        .map_err(|e| format!("subscribe failed: {}", e));
                    if result.is_ok() {
                        info!(topic = %topic, "MQTT subscribed");
                    }
                    let _ = reply.send(result);
                }
                MqttCommand::Unsubscribe { topic, reply } => {
                    let result = client
                        .unsubscribe(&topic)
                        .await
                        .map_err(|e| format!("unsubscribe failed: {}", e));
                    if result.is_ok() {
                        info!(topic = %topic, "MQTT unsubscribed");
                    }
                    let _ = reply.send(result);
                }
                MqttCommand::Publish { topic, qos, payload, reply } => {
                    let result = client
                        .publish(&topic, qos, false, payload)
                        .await
                        .map_err(|e| format!("publish failed: {}", e));
                    if let Err(ref e) = result {
                        error!(topic = %topic, error = %e, "MQTT publish error");
                    }
                    let _ = reply.send(result);
                }
                MqttCommand::Shutdown => {
                    break;
                }
            }
        }
    });
}
