use tokio::sync::mpsc;
use tracing::{error, info};

use crate::config::{HttpConfig, MqttBrokerageConfig};
use crate::infra::ca::CaService;
use crate::services::mqtt_brokerage::MqttBrokerageHandle;
use crate::supervisor::{Supervisor, SupervisorHandle};
use crate::services::ServiceId;
use super::gateway_service::GatewayService;
use super::internals::{MqttBrokerageInfo, MqttTopicConfig};

/// Build and register the gateway service onto the supervisor.
pub fn register_onto(
    supervisor: &mut Supervisor,
    config: HttpConfig,
    brokerage_info: MqttBrokerageInfo,
    brokerage: Option<MqttBrokerageHandle>,
    ca: CaService,
    beacon_topic_tx: Option<mpsc::Sender<TopicChannel>>,
) {
    let service = GatewayService::new(config, ca, brokerage_info, brokerage, beacon_topic_tx);
    supervisor.register(ServiceId::Gateway, Box::new(service), true);
}

/// Start the gateway service via the supervisor.
pub async fn start_gateway(supervisor_handle: &SupervisorHandle) {
    match supervisor_handle.start_service(ServiceId::Gateway).await {
        Ok(()) => info!("Gateway service started"),
        Err(e) => error!("Failed to start gateway service: {}", e),
    }
}

#[derive(Debug, Clone)]
pub enum TopicType {
    Listen,
    Broadcast,
    Hybrid,
}

#[derive(Debug, Clone)]
pub struct TopicChannel {
    pub beacon_id: String,
    pub topic: MqttTopicConfig,
    pub topic_type: TopicType,
}

pub fn build_brokerage_info(config: &MqttBrokerageConfig, ca_cert_pem: String) -> MqttBrokerageInfo {
    let port = if config.tls_enabled { config.mqtts_port } else { config.port };
    MqttBrokerageInfo {
        address: "localhost".to_string(),
        port,
        //topics: vec![],
        ca_cert_pem,
    }
}
