use tracing::{error, info, warn};

use crate::config::GatewayRegistrationConfig;
use crate::services::ServiceId;
use crate::supervisor::{Supervisor, SupervisorHandle};

use super::{GatewayMqttConfig, GatewayMqttHandle, GatewayMqttService};

/// Build and register the cloud gateway MQTT client.
///
/// Returns `None` (and registers nothing) when `PLACENET_GATEWAY_URL` is unset —
/// the cloud gateway connection is disabled in that case.
pub fn register_onto(
    supervisor: &mut Supervisor,
    cfg: &GatewayRegistrationConfig,
) -> Option<GatewayMqttHandle> {
    let login_url = match &cfg.gateway_url {
        None => {
            warn!("PLACENET_GATEWAY_URL not set — cloud gateway client disabled");
            return None;
        }
        Some(url) => url.clone(),
    };

    if cfg.username.is_empty() || cfg.password.is_empty() {
        warn!("PLACENET_GATEWAY_USERNAME/PASSWORD not set — cloud gateway client disabled");
        return None;
    }

    let config = GatewayMqttConfig {
        login_url,
        username: cfg.username.clone(),
        password: cfg.password.clone(),
        broker_cafile: cfg.broker_cafile.clone(),
        broker_tls: cfg.broker_tls,
    };

    let service = GatewayMqttService::new(config);
    let handle = service.handle();
    supervisor.register(ServiceId::GatewayMqttClient, Box::new(service), true);
    Some(handle)
}

/// Start the cloud gateway MQTT client via the supervisor.
pub async fn start_gateway_mqtt(supervisor_handle: &SupervisorHandle) {
    match supervisor_handle.start_service(ServiceId::GatewayMqttClient).await {
        Ok(()) => info!("Cloud gateway MQTT client started"),
        Err(e) => error!("Failed to start cloud gateway MQTT client: {}", e),
    }
}
