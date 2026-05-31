//! The cloud-gateway registration wire contract (Hamlet side).
//!
//! Mirrors `placenet-cloud-gateway/src/protocol.rs`. The two crates don't share
//! a dependency, so the types are duplicated; keep them in sync.

use serde::{Deserialize, Serialize};

/// `POST /api/login` request body.
#[derive(Debug, Clone, Serialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

/// Broker coordinates returned by the gateway.
#[derive(Debug, Clone, Deserialize)]
pub struct BrokerInfo {
    pub host: String,
    pub port: u16,
}

/// The three per-device topics, fully qualified.
#[derive(Debug, Clone, Deserialize)]
pub struct DeviceTopics {
    pub cmds: String,
    pub connect: String,
    pub notify: String,
}

/// `POST /api/login` success response body.
#[derive(Debug, Clone, Deserialize)]
pub struct LoginResponse {
    pub protocol_version: String,
    pub device_id: String,
    pub mqtt_username: String,
    pub mqtt_password: String,
    pub broker: BrokerInfo,
    pub topics: DeviceTopics,
}

/// JSON envelope carried on the per-device topics. Only `Alive` is used now.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Envelope {
    Alive { device_id: String },
}
