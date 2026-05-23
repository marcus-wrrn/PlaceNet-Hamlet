use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::info;
use crate::config::Config;
use crate::services::mqtt_brokerage::manager::{register_onto as register_mqtt_broker, start_mosquitto_brokerage};
use crate::services::mqtt_brokerage::provision_mqtt_broker_cert;
use crate::services::mqtt_client::manager::{register_onto as register_mqtt_client, start_mqtt_client};
use crate::services::mqtt_client::{self, MqttClientHandle, MqttMessageReceiver};
use rumqttc::QoS;
use crate::services::mqtt_client::provision_node_identity;
use crate::infra::ca::CaService;
use crate::services::local_gateway::manager::{register_onto as register_gateway, start_gateway, build_brokerage_info, TopicChannel};
use crate::services::local_gateway::BeaconRegistry;
use crate::services::cloud_gateway::manager::{register_onto as register_cloud_gateway, start_cloud_gateway};
use crate::services::cloud_gateway::{connect_to_gateway, messages::GatewayMessage};
use crate::services;
use crate::supervisor::{Supervisor, SupervisorHandle};

/// Cloneable subset of App state used by the broadcast loop.
pub struct BroadcastState {
    server_url: String,
    mqtt_handle: MqttClientHandle,
    broadcast_topics: Arc<RwLock<Vec<String>>>,
}

impl BroadcastState {
    /// Periodically publishes this node's `server_url` to all registered beacon topics.
    ///
    /// Fires every 30 seconds. Iterates over `broadcast_topics` and publishes a JSON payload
    /// `{"server_url": "..."}` to each topic at QoS 1. Runs until the task is cancelled.
    /// TODO: Add rotating encryption keys + dynamic broadcast intervals
    pub async fn run_broadcast_loop(self) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
        loop {
            interval.tick().await;
            // TODO: Expand upon payload content
            let payload = serde_json::json!({ "server_url": self.server_url });
            let payload_str = payload.to_string();

            let topics = self.broadcast_topics.read().await;
            for topic in topics.iter() {
                if let Err(e) = self
                    .mqtt_handle.publish(topic, QoS::AtLeastOnce, payload_str.clone())
                    .await
                {
                    tracing::error!(topic, "Failed to publish beacon broadcast: {}", e);
                } else {
                    info!(topic, "Broadcast published to beacon topic");
                }
            }
        }
    }
}

pub struct App {
    /// Kept alive to drive the supervisor task until shutdown.
    _supervisor_handle: SupervisorHandle,
    /// Sends beacon messages from brokerage to service 
    beacon_inbound_rx: MqttMessageReceiver,
    mqtt_handle: MqttClientHandle,
    server_url: String,
    own_gateway_url: Option<String>,
    /// Used to subscribe to new topics during beacon handshake
    beacon_topic_rx: mpsc::Receiver<TopicChannel>,
    broadcast_topics: Arc<RwLock<Vec<String>>>,
}

impl App {
    pub async fn initialize(config: Config) -> Self {
        // ── Service detection ────────────────────────────────────────────
        let required_binaries = ["mosquitto", "mosquitto_passwd"];
        let capabilities = services::detect_capabilities(&required_binaries).await;
        let mut supervisor = Supervisor::new();

        // ── Initialise Certificate Authority ─────────────────────────────
        let ca_db_url = std::env::var("CA_DATABASE_URL")
            .unwrap_or_else(|_| "sqlite://placenet_ca.db".to_string());
        let ca_service = match CaService::register(&ca_db_url).await {
            Ok(ca) => ca,
            Err(e) => {
                tracing::error!("Failed to initialise CA: {}", e);
                std::process::exit(1);
            }
        };

        // ── Build gateway brokerage info ─────────────────────────────────
        let ca_cert_pem = match ca_service.ca_cert_pem().await {
            Ok(pem) => pem,
            Err(e) => {
                tracing::error!("Failed to retrieve CA cert PEM: {}", e);
                std::process::exit(1);
            }
        };
        let brokerage_info = build_brokerage_info(&config.mqtt_brokerage, ca_cert_pem);

        // ── Provision broker TLS cert ────────────────────────────────────
        if config.mqtt_brokerage.tls_enabled {
            if let Err(e) = provision_mqtt_broker_cert(&ca_service, &config.mqtt_brokerage).await {
                tracing::error!("{}", e);
                std::process::exit(1);
            }
        }

        // ── Register Mosquitto broker ────────────────────────────────────
        let mqtt_broker_config = Arc::new(RwLock::new(config.mqtt_brokerage));
        let (broker_available, brokerage_handle) =
            register_mqtt_broker(&mut supervisor, &capabilities, Arc::clone(&mqtt_broker_config)).await;

        // ── Register gateway service ─────────────────────────────────────
        let (beacon_topic_tx, beacon_topic_rx) = mpsc::channel::<TopicChannel>(64);
        register_gateway(&mut supervisor, config.http, brokerage_info, brokerage_handle, ca_service.clone(), Some(beacon_topic_tx));

        // ── Provision node identity for MQTT mutual TLS ──────────────────
        if config.mqtt_client.tls_enabled {
            if let Err(e) = provision_node_identity(&ca_service, &config.mqtt_client).await {
                tracing::error!("{}", e);
                std::process::exit(1);
            }
        }

        // ── Register MQTT client ─────────────────────────────────────────
        let mqtt_handles = register_mqtt_client(&mut supervisor, config.mqtt_client, broker_available);
        let mqtt_handle = mqtt_handles.handle;
        let inbound_rx = mqtt_handles.inbound_rx;
        let _outbound_tx = mqtt_handles.outbound_tx;
        let mqtt_connected_rx = mqtt_handles.connected_rx;

        // ── Register cloud gateway client ────────────────────────────────
        let cloud_gateway_handle = register_cloud_gateway(
            &mut supervisor,
            config.gateway_registration.server_url.clone(),
            config.gateway_registration.gateway_url.clone(),
        );
        let cloud_gateway_available = cloud_gateway_handle.is_some();

        let supervisor_handle = supervisor.spawn();

        // ── Start services ───────────────────────────────────────────────
        start_mosquitto_brokerage(broker_available, &supervisor_handle).await;
        start_mqtt_client(broker_available, &supervisor_handle).await;

        if broker_available {
            if mqtt_connected_rx.await.is_ok() {
                if let Err(e) = mqtt_handle.subscribe_all(&mqtt_client::required_subscriptions()).await {
                    tracing::error!("Failed to subscribe to required topics: {}", e);
                }
            } else {
                tracing::error!("MQTT client disconnected before connection was established");
            }
        }

        start_gateway(&supervisor_handle).await;
        if cloud_gateway_available {
            start_cloud_gateway(&supervisor_handle).await;
        }

        let broadcast_topics: Arc<RwLock<Vec<String>>> = Arc::new(RwLock::new(Vec::new()));
        Self {
            _supervisor_handle: supervisor_handle,
            beacon_inbound_rx: inbound_rx,
            mqtt_handle,
            server_url: config.gateway_registration.server_url,
            own_gateway_url: config.gateway_registration.gateway_url,
            beacon_topic_rx,
            broadcast_topics,
        }
    }

    pub fn broadcast_state(&self) -> BroadcastState {
        BroadcastState {
            server_url: self.server_url.clone(),
            mqtt_handle: self.mqtt_handle.clone(),
            broadcast_topics: Arc::clone(&self.broadcast_topics),
        }
    }

    /// Processes incoming beacon requests.
    pub async fn run_beacon_message_loop(mut self) {
        let mut registry = BeaconRegistry::new(
            self.own_gateway_url,
            Arc::clone(&self.broadcast_topics),
            self.mqtt_handle.clone(),
        );

        loop {
            let msg = tokio::select! {
                // ── New topic channel from a completed device handshake ──────
                Some(channel) = self.beacon_topic_rx.recv() => {
                    registry.register_channel(channel).await;
                    continue;
                }

                // ── Periodic stale-beacon sweep ──────────────────────────────
                _ = registry.check_timeouts() => {
                    continue;
                }

                // ── Inbound MQTT message from a beacon ───────────────────────
                msg = self.beacon_inbound_rx.recv() => match msg {
                    Some(m) => m,
                    None => break,
                }
            };

            info!(topic = %msg.topic, "Received MQTT message");

            // Refresh last_seen for the beacon that sent this message.
            // Topics follow the pattern "{beacon_id}/rec".
            if let Some(beacon_id) = msg.topic.strip_suffix("/rec") {
                registry.update_last_seen(beacon_id);
            }

            // Parse the raw beacon payload, falling back to a string value if it
            // isn't valid JSON.
            let beacon_payload: serde_json::Value =
                serde_json::from_slice(&msg.payload)
                    .unwrap_or_else(|_| {
                        serde_json::Value::String(
                            String::from_utf8_lossy(&msg.payload).into_owned(),
                        )
                    });

            // Extract the gateway_url advertised by the beacon, if present.
            let beacon_gateway_url = beacon_payload
                .as_object()
                .and_then(|obj| obj.get("gateway_url"))
                .and_then(|v| v.as_str())
                .map(str::to_owned);

            if let Some(gw_url) = beacon_gateway_url {
                if registry.is_gateway_known(&gw_url) {
                    info!(gateway = %gw_url, "Beacon's gateway already connected, skipping");
                } else {
                    info!(gateway = %gw_url, "Beacon advertises new gateway — registering");
                    registry.insert_gateway(gw_url.clone());
                    let (handle, shutdown_tx) = connect_to_gateway(self.server_url.clone(), gw_url.clone());
                    // Queue the beacon payload immediately; the connection task will
                    // deliver it after the Register frame is sent.
                    let registration = GatewayMessage::BeaconRegistration {
                        server_url: self.server_url.clone(),
                        payload: beacon_payload,
                    };
                    if let Err(e) = handle.send(registration).await {
                        tracing::error!(gateway = %gw_url, "Failed to queue beacon payload: {}", e);
                    }

                    // Associate the connection with the beacon that triggered it
                    // so timeout cleanup can tear it down.
                    if let Some(bid) = msg.topic.strip_suffix("/rec").map(str::to_owned) {
                        registry.associate_gateway(&bid, gw_url, shutdown_tx);
                    }
                }
            }
        }
    }
}
