use tokio::sync::mpsc;
use tracing::{error, info};

use crate::config::HttpConfig;
use crate::infra::ca::CaService;
use crate::services::mqtt_brokerage::MqttBrokerageHandle;
use crate::supervisor::{Supervisor, SupervisorHandle};
use crate::services::ServiceId;
use super::GatewayService;
use super::handshake::{MqttBrokerageInfo, TopicChannel};

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
