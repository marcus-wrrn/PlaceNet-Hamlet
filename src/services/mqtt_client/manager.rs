use rumqttc::QoS;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn, error};

use crate::config::MqttClientConfig;
use crate::supervisor::{Supervisor, SupervisorHandle};
use crate::services::ServiceId;
use super::internals::{MqttCommand, MqttMessage, MqttMessageReceiver, MqttOutboundSender, TopicSubscription};
use super::mqtt_client_service::{MqttClientHandle, MqttClientService};
use crate::infra::ca::CaService;

const CMD_CAPACITY: usize = 64;
const MSG_CAPACITY: usize = 64;

/// Handles returned to the caller after registering the MQTT client service.
pub struct MqttManagerHandles {
    pub handle: MqttClientHandle,
    pub inbound_rx: MqttMessageReceiver,
    pub outbound_tx: MqttOutboundSender,
    /// Resolves once the MQTT client has received a ConnAck from the broker.
    pub connected_rx: oneshot::Receiver<()>,
}

/// Build and register the MQTT client service onto the supervisor.
///
/// `available` should match the availability of the broker this client
/// connects to (e.g. `mosquitto_available`). Returns the channel handles
/// the caller needs to interact with the MQTT client.
pub fn register_onto(
    supervisor: &mut Supervisor,
    config: MqttClientConfig,
    available: bool,
) -> MqttManagerHandles {
    let (cmd_tx, cmd_rx) = mpsc::channel::<MqttCommand>(CMD_CAPACITY);
    let (msg_tx, msg_rx) = mpsc::channel::<MqttMessage>(MSG_CAPACITY);
    let (out_tx, out_rx) = mpsc::channel::<MqttMessage>(MSG_CAPACITY);
    let (connected_tx, connected_rx) = oneshot::channel::<()>();

    let service = MqttClientService::new(config, cmd_tx.clone(), cmd_rx, msg_tx, out_rx, connected_tx);
    let handle = service.handle();

    supervisor.register(ServiceId::MqttClient, Box::new(service), available);

    MqttManagerHandles {
        handle,
        inbound_rx: msg_rx,
        outbound_tx: out_tx,
        connected_rx,
    }
}

/// Load required subscriptions for beacon communications
pub fn required_subscriptions() -> Vec<TopicSubscription> {
    vec![
        TopicSubscription { topic: "registration", qos: QoS::AtLeastOnce },
        TopicSubscription { topic: "key-bridge", qos: QoS::AtLeastOnce },
    ]
}

/// Start the MQTT client service via the supervisor.
pub async fn start_mqtt_client(
    mosquitto_available: bool,
    supervisor_handle: &SupervisorHandle,
) {
    if mosquitto_available {
        match supervisor_handle.start_service(ServiceId::MqttClient).await {
            Ok(()) => info!("MQTT client started"),
            Err(e) => error!("Failed to start MQTT client service: {}", e),
        }
    } else {
        warn!("Mosquitto not available — skipping MQTT client start");
    }
}

pub async fn provision_node_identity(ca: &CaService, cfg: &MqttClientConfig) -> Result<(), String> {
    let (cert_pem, key_pem) = ca.ensure_node_identity().await?;
    if let Some(parent) = cfg.certfile.parent() {
        tokio::fs::create_dir_all(parent).await
            .map_err(|e| format!("Failed to create cert dir: {e}"))?;
    }
    tokio::fs::write(&cfg.certfile, &cert_pem).await
        .map_err(|e| format!("Failed to write node cert: {e}"))?;
    tokio::fs::write(&cfg.keyfile, &key_pem).await
        .map_err(|e| format!("Failed to write node key: {e}"))?;
    Ok(())
}