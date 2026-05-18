use http_body_util::combinators::BoxBody;
use hyper::body::Bytes;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};

use crate::infra::ca::CaService;
use crate::services::local_gateway::internals::handshake::MqttBrokerageInfo;
use crate::services::local_gateway::manager::TopicChannel;
use crate::services::mqtt_brokerage::MqttBrokerageHandle;

pub const SUPPORTED_VERSION: &str = "0.0.1";
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
