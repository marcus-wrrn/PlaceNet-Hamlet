use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rumqttc::QoS;
use tokio::sync::{oneshot, RwLock};

use crate::services::beacon_management::manager::{TopicChannel, TopicType};
use crate::services::mqtt_client::MqttClientHandle;

/// A beacon is considered stale and deregistered after this period of silence.
const BEACON_TIMEOUT: Duration = Duration::from_secs(120);

/// How often to scan for timed-out beacons.
const BEACON_TIMEOUT_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// Runtime state tracked per registered beacon.
struct BeaconRecord {
    listen_topic: String,
    broadcast_topic: String,
    last_seen: Instant,
    gateway_url: Option<String>,
    /// Dropping this signals the WebSocket task to close.
    gateway_shutdown_tx: Option<oneshot::Sender<()>>,
}

/// Manages the lifecycle of registered beacons.
///
/// Tracks per-beacon MQTT topics and gateway associations, refreshes
/// `last_seen` timestamps on inbound messages, and periodically sweeps
/// stale entries via [`BeaconRegistry::check_timeouts`].
pub struct BeaconRegistry {
    records: HashMap<String, BeaconRecord>,
    /// Gateway URLs we are already connected to (or own). Prevents duplicate connections.
    known_gateways: HashSet<String>,
    broadcast_topics: Arc<RwLock<Vec<String>>>,
    mqtt_handle: MqttClientHandle,
    timeout_tick: tokio::time::Interval,
}

impl BeaconRegistry {
    /// Create a new registry.
    ///
    /// `own_gateway_url` is pre-seeded into `known_gateways` so that beacons
    /// advertising the same URL do not trigger a redundant connection.
    pub fn new(
        own_gateway_url: Option<String>,
        broadcast_topics: Arc<RwLock<Vec<String>>>,
        mqtt_handle: MqttClientHandle,
    ) -> Self {
        let mut known_gateways = HashSet::new();
        if let Some(url) = own_gateway_url {
            known_gateways.insert(url);
        }

        let mut timeout_tick = tokio::time::interval(BEACON_TIMEOUT_CHECK_INTERVAL);
        timeout_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        Self {
            records: HashMap::new(),
            known_gateways,
            broadcast_topics,
            mqtt_handle,
            timeout_tick,
        }
    }

    /// Handle a new [`TopicChannel`] produced by a completed device handshake.
    ///
    /// Subscribes to listen/hybrid topics via the MQTT client and registers
    /// broadcast topics in the shared list. Creates the [`BeaconRecord`] entry
    /// if it does not yet exist.
    pub async fn register_channel(&mut self, channel: TopicChannel) {
        let topic = channel.topic.topic.clone();
        let beacon_id = channel.beacon_id.clone();

        // Ensure the record exists before the async calls below.
        self.records.entry(beacon_id.clone()).or_insert_with(|| BeaconRecord {
            listen_topic: String::new(),
            broadcast_topic: String::new(),
            last_seen: Instant::now(),
            gateway_url: None,
            gateway_shutdown_tx: None,
        });

        match channel.topic_type {
            TopicType::Listen => {
                let qos = qos_from_u8(channel.topic.qos);
                if let Err(e) = self.mqtt_handle.subscribe(&topic, qos).await {
                    tracing::error!(topic, "Failed to subscribe to beacon listen topic: {}", e);
                } else {
                    tracing::info!(beacon_id = %beacon_id, topic, "Subscribed to beacon listen topic");
                    if let Some(entry) = self.records.get_mut(&beacon_id) {
                        entry.listen_topic = topic;
                    }
                }
            }
            TopicType::Broadcast => {
                self.broadcast_topics.write().await.push(topic.clone());
                tracing::info!(beacon_id = %beacon_id, topic, "Registered beacon broadcast topic");
                if let Some(entry) = self.records.get_mut(&beacon_id) {
                    entry.broadcast_topic = topic;
                }
            }
            TopicType::Hybrid => {
                let qos = qos_from_u8(channel.topic.qos);
                if let Err(e) = self.mqtt_handle.subscribe(&topic, qos).await {
                    tracing::error!(topic, "Failed to subscribe to beacon hybrid topic: {}", e);
                } else {
                    tracing::info!(beacon_id = %beacon_id, topic, "Subscribed to beacon hybrid topic");
                    self.broadcast_topics.write().await.push(topic.clone());
                    if let Some(entry) = self.records.get_mut(&beacon_id) {
                        entry.listen_topic = topic.clone();
                        entry.broadcast_topic = topic;
                    }
                }
            }
        }
    }

    /// Wait for the next timeout tick, then sweep and deregister stale beacons.
    ///
    /// For each stale entry this method:
    /// - Unsubscribes from its listen topic.
    /// - Removes its broadcast topic from the shared list.
    /// - Evicts its gateway URL from `known_gateways`.
    /// - Drops `gateway_shutdown_tx`, which signals the WebSocket task to close.
    pub async fn check_timeouts(&mut self) {
        self.timeout_tick.tick().await;

        let now = Instant::now();
        let stale: Vec<String> = self.records
            .iter()
            .filter(|(_, r)| now.duration_since(r.last_seen) > BEACON_TIMEOUT)
            .map(|(id, _)| id.clone())
            .collect();

        for beacon_id in stale {
            if let Some(record) = self.records.remove(&beacon_id) {
                tracing::warn!(beacon_id, "Beacon timed out — deregistering");

                // Unsubscribe from the listen topic.
                if !record.listen_topic.is_empty() {
                    if let Err(e) = self.mqtt_handle.unsubscribe(&record.listen_topic).await {
                        tracing::error!(
                            topic = %record.listen_topic,
                            "Failed to unsubscribe timed-out beacon listen topic: {}",
                            e
                        );
                    }
                }

                // Remove from broadcast list.
                if !record.broadcast_topic.is_empty() {
                    self.broadcast_topics.write().await
                        .retain(|t| t != &record.broadcast_topic);
                }

                // Evict from known_gateways so a future beacon can
                // reconnect via the same gateway URL.
                if let Some(ref gw_url) = record.gateway_url {
                    self.known_gateways.remove(gw_url);
                }

                // Dropping gateway_shutdown_tx signals the WebSocket task to close.
            }
        }
    }

    /// Refresh the `last_seen` timestamp for the beacon with the given ID.
    ///
    /// No-op if the beacon is not currently registered.
    pub fn update_last_seen(&mut self, beacon_id: &str) {
        if let Some(record) = self.records.get_mut(beacon_id) {
            record.last_seen = Instant::now();
        }
    }

    /// Returns `true` if `gw_url` is already in the known-gateways set.
    pub fn is_gateway_known(&self, gw_url: &str) -> bool {
        self.known_gateways.contains(gw_url)
    }

    /// Add `gw_url` to the known-gateways set.
    pub fn insert_gateway(&mut self, gw_url: String) {
        self.known_gateways.insert(gw_url);
    }

    /// Associate an active gateway connection with the beacon that triggered it.
    ///
    /// Stores `gw_url` and `shutdown_tx` on the beacon record so that timeout
    /// cleanup can tear down the WebSocket connection when the beacon goes stale.
    /// No-op if `beacon_id` is not currently registered.
    pub fn associate_gateway(
        &mut self,
        beacon_id: &str,
        gw_url: String,
        shutdown_tx: oneshot::Sender<()>,
    ) {
        if let Some(record) = self.records.get_mut(beacon_id) {
            record.gateway_url = Some(gw_url);
            record.gateway_shutdown_tx = Some(shutdown_tx);
        }
    }
}

/// Map a raw QoS byte (as sent in the protocol) to a [`rumqttc::QoS`] variant.
fn qos_from_u8(qos: u8) -> QoS {
    match qos {
        0 => QoS::AtMostOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtLeastOnce,
    }
}
