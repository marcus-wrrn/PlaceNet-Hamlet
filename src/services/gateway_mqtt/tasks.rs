//! The cloud-gateway connection task.
//!
//! Owns the full connect cycle: log in over HTTPS, build an MQTTS connection
//! from the returned credentials, run the inbound/outbound loop, and on failure
//! back off (2 s → 60 s) before reconnecting. Credentials are cached and reused
//! across transport reconnects; a re-login only happens after an auth rejection.

use std::time::Duration;

use rumqttc::{AsyncClient, ConnectionError, Event, Packet, QoS};
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};

use crate::config::{MqttClientConfig, MqttTlsMode};
use crate::services::mqtt_client::{build_mqtt_opts, MqttMessage};

use super::login::{login, LoginError};
use super::protocol::{Envelope, LoginRequest, LoginResponse};
use super::GatewayMqttConfig;

const INITIAL_RECONNECT_DELAY: Duration = Duration::from_secs(2);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(60);

pub fn spawn_connection_task(
    config: GatewayMqttConfig,
    mut outbound_rx: mpsc::Receiver<MqttMessage>,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    tokio::spawn(async move {
        let mut delay = INITIAL_RECONNECT_DELAY;
        // Cached login result, reused across reconnects until an auth failure.
        let mut cached: Option<LoginResponse> = None;

        'reconnect: loop {
            // ── 1. Ensure we have credentials (login if needed) ──────────
            let creds = match &cached {
                Some(c) => c.clone(),
                None => {
                    let req = LoginRequest {
                        username: config.username.clone(),
                        password: config.password.clone(),
                    };
                    let attempt = tokio::select! {
                        _ = &mut shutdown_rx => break 'reconnect,
                        r = login(&config.login_url, &req, config.broker_cafile.as_deref()) => r,
                    };
                    match attempt {
                        Ok(resp) => {
                            info!(device_id = %resp.device_id, "Logged in to cloud gateway");
                            cached = Some(resp.clone());
                            resp
                        }
                        Err(LoginError::Unauthorized) => {
                            error!("Gateway rejected credentials (401) — retrying after backoff");
                            if backoff(&mut delay, &mut shutdown_rx).await { break 'reconnect; }
                            continue 'reconnect;
                        }
                        Err(LoginError::Transient(e)) => {
                            warn!(error = %e, delay_secs = delay.as_secs(), "Gateway login failed, retrying");
                            if backoff(&mut delay, &mut shutdown_rx).await { break 'reconnect; }
                            continue 'reconnect;
                        }
                    }
                }
            };

            // ── 2. Build the MQTTS connection from the returned creds ─────
            let client_cfg = MqttClientConfig {
                client_id: creds.device_id.clone(),
                host: creds.broker.host.clone(),
                port: creds.broker.port,
                tls_mode: if config.broker_tls { MqttTlsMode::ServerOnly } else { MqttTlsMode::None },
                cafile: config.broker_cafile.clone(),
                certfile: None,
                keyfile: None,
                username: creds.mqtt_username.clone(),
                password: creds.mqtt_password.clone(),
            };

            let opts = match build_mqtt_opts(&client_cfg).await {
                Ok(o) => o,
                Err(e) => {
                    error!(error = %e, "Failed to build gateway MQTT options");
                    if backoff(&mut delay, &mut shutdown_rx).await { break 'reconnect; }
                    continue 'reconnect;
                }
            };

            info!(host = %creds.broker.host, port = creds.broker.port, "Connecting to gateway broker");
            let (client, mut eventloop) = AsyncClient::new(opts, 10);

            // ── 3. Inbound / outbound loop ────────────────────────────────
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => {
                        let _ = client.disconnect().await;
                        break 'reconnect;
                    }

                    Some(msg) = outbound_rx.recv() => {
                        if let Err(e) = client
                            .publish(&msg.topic, QoS::AtLeastOnce, false, msg.payload.to_vec())
                            .await
                        {
                            warn!(topic = %msg.topic, error = %e, "Gateway publish failed");
                        }
                    }

                    poll = eventloop.poll() => {
                        match poll {
                            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                                delay = INITIAL_RECONNECT_DELAY;
                                info!(device_id = %creds.device_id, "Connected to cloud gateway broker");
                                on_connected(&client, &creds).await;
                            }
                            Ok(Event::Incoming(Packet::Publish(p))) => {
                                handle_inbound(&p.topic, &p.payload);
                            }
                            Ok(_) => {}
                            Err(ConnectionError::ConnectionRefused(code)) => {
                                // Broker refused the credentials — force a fresh login.
                                warn!(?code, "Gateway broker refused connection, will re-login");
                                cached = None;
                                break;
                            }
                            Err(e) => {
                                warn!(error = %e, "Gateway MQTT connection error, reconnecting");
                                break;
                            }
                        }
                    }
                }
            }

            // ── 4. Backoff before the next reconnect ──────────────────────
            if backoff(&mut delay, &mut shutdown_rx).await { break 'reconnect; }
        }

        info!("Cloud gateway MQTT task stopped");
    });
}

/// On ConnAck: subscribe to the device's `cmds`/`connect` topics and publish the
/// `alive` notification.
async fn on_connected(client: &AsyncClient, creds: &LoginResponse) {
    for topic in [&creds.topics.cmds, &creds.topics.connect] {
        if let Err(e) = client.subscribe(topic, QoS::AtLeastOnce).await {
            warn!(topic = %topic, error = %e, "Failed to subscribe to gateway topic");
        }
    }

    let alive = Envelope::Alive { device_id: creds.device_id.clone() };
    match serde_json::to_vec(&alive) {
        Ok(payload) => {
            if let Err(e) = client
                .publish(&creds.topics.notify, QoS::AtLeastOnce, false, payload)
                .await
            {
                warn!(error = %e, "Failed to publish alive notification");
            } else {
                info!(topic = %creds.topics.notify, "Published alive notification");
            }
        }
        Err(e) => error!(error = %e, "Failed to serialise alive envelope"),
    }
}

/// Inbound frames are logged for now; the deferred server-side router will act
/// on `cmds`/`connect` envelopes (Connect/Relay/Ack).
fn handle_inbound(topic: &str, payload: &[u8]) {
    match serde_json::from_slice::<Envelope>(payload) {
        Ok(env) => info!(topic = %topic, ?env, "Inbound gateway envelope"),
        Err(_) => info!(topic = %topic, "Inbound gateway message (undecoded)"),
    }
}

/// Sleep for the current backoff delay (interruptible by shutdown), then double
/// it up to the cap. Returns `true` if shutdown fired during the wait.
async fn backoff(delay: &mut Duration, shutdown_rx: &mut oneshot::Receiver<()>) -> bool {
    let shutting_down = tokio::select! {
        _ = shutdown_rx => true,
        _ = tokio::time::sleep(*delay) => false,
    };
    *delay = (*delay * 2).min(MAX_RECONNECT_DELAY);
    shutting_down
}
