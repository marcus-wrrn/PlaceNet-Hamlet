use serde::{Deserialize, Serialize};
use crate::config::MqttBrokerageConfig;

/// mDNS settings advertised by the device.
#[derive(Debug, Deserialize)]
pub struct MdnsConfig {
    pub hostname: String,
    pub port: u16,
}

/// Registration payload sent by a device on the `POST /` route.
#[derive(Debug, Deserialize)]
pub struct DeviceInfo {
    pub address: String,
    pub mdns: MdnsConfig,
    /// PEM-encoded Certificate Signing Request for the device.
    pub csr_pem: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct MqttTopicConfig {
    pub topic: String,
    pub qos: u8,
}

/// Broker connection details returned to a device after a successful handshake.
#[derive(Debug, Clone, Serialize)]
pub struct MqttBrokerageInfo {
    pub address: String,
    pub port: u16,
    pub topics: Vec<MqttTopicConfig>,
    /// PEM-encoded CA certificate. The device must trust this to verify the broker's
    /// TLS certificate and to present its own CA-signed client certificate.
    pub ca_cert_pem: String,
}

/// Wraps a raw beacon MQTT registration payload with this server's identity
/// information so that receiving peers know which hamlet forwarded the
/// message and how to reach the cloud gateway.
#[derive(Debug, Serialize, Deserialize)]
pub struct EnrichedRegistrationMessage {
    /// Raw payload published by the beacon on the "registration" MQTT topic.
    pub beacon_payload: serde_json::Value,
    /// URL of the hamlet server that received and forwarded this message.
    /// Used as an opaque identity/ID by the cloud gateway.
    pub server_url: String,
    /// URL of the cloud gateway WebSocket endpoint, if configured.
    pub gateway_url: Option<String>,
}

pub fn build_brokerage_info(config: &MqttBrokerageConfig, ca_cert_pem: String) -> MqttBrokerageInfo {
    let port = if config.tls_enabled { config.mqtts_port } else { config.port };
    MqttBrokerageInfo {
        address: "localhost".to_string(),
        port,
        topics: vec![],
        ca_cert_pem,
    }
}
