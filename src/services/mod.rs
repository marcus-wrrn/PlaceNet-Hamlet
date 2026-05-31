pub mod capabilities;
pub mod cloud_gateway;
pub mod gateway_mqtt;
pub mod beacon_management;
pub mod mqtt_brokerage;
pub mod mqtt_client;
pub mod peer;

pub use capabilities::{detect_capabilities, ServiceCapabilities, BinaryInfo};

use serde::Serialize;

/// Identifiers for all managed services.
///
/// Used as keys when referencing services throughout the process manager
/// and any associated state maps.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceId {
    Gateway,
    Mosquitto,
    MqttClient,
    CloudGateway,
    GatewayMqttClient,
}
