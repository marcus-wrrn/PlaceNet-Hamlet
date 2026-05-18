use http_body_util::combinators::BoxBody;
use hyper::body::Bytes;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};

use crate::infra::ca::CaService;
use crate::services::local_gateway::manager::TopicChannel;
use crate::services::mqtt_brokerage::MqttBrokerageHandle;

pub const SUPPORTED_VERSION: &str = "0.0.1";

/// Device handshake: sign CSR, return cert + brokerage info.
pub(super) const HEADER_INIT: &str = "x-placenet-init";

/// Client cert registration: sign CSR, return cert + CA cert.
pub(super) const HEADER_REGISTER: &str = "x-placenet-register";

/// Health check: returns 200 OK.
pub(super) const HEADER_HEALTH: &str = "x-placenet-health";

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
    /// PEM-encoded CA certificate. The device must trust this to verify the broker's
    /// TLS certificate and to present its own CA-signed client certificate.
    pub ca_cert_pem: String,
}
pub const BODY_LIMIT: usize = 64 * 1024;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub type ProxyBody = BoxBody<Bytes, hyper::Error>;

#[derive(Clone)]
pub struct AppState {
    pub ca: CaService,
    pub brokerage_info: MqttBrokerageInfo,
    pub brokerage: Option<MqttBrokerageHandle>,
    pub upstream_port: u16,
    /// Sends newly assigned beacon broadcast topics to the app for subscription and broadcasting.
    pub beacon_topic_tx: Option<mpsc::Sender<TopicChannel>>,
}

pub struct BoundGateway {
    pub state: AppState,
    pub listener: TcpListener,
    pub local_addr: SocketAddr,
    pub shutdown_rx: oneshot::Receiver<()>,
}
