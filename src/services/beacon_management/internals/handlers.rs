use hyper::body::Incoming;
use hyper::{Request, Response};
use tracing::{debug, error, info};

use super::types::MqttTopicConfig;
use super::super::manager::{TopicChannel, TopicType};
use super::requests::{DeviceInitRequest, DeviceRegisterRequest};
use super::response::{json_response, text_response};
use super::types::{AppState, ProxyBody};

/// `X-PlaceNet-Health` — health check: returns 200 OK.
pub(in crate::services::beacon_management) async fn handle_health(_req: Request<Incoming>) -> Response<ProxyBody> {
    text_response(200, "OK")
}

/// `X-PlaceNet-Init: <version>` — device handshake: sign CSR, return cert + brokerage info.
pub(in crate::services::beacon_management) async fn handle_device_init(state: &AppState, req: Request<Incoming>) -> Response<ProxyBody> {
    let request = match DeviceInitRequest::process_request(req).await {
        Ok(p) => p,
        Err(r) => return r,
    };

    let device = &request.device;

    info!(
        mdns_hostname = %device.mdns.hostname,
        mdns_port = device.mdns.port,
        address = %device.address,
        "Device handshake — signing CSR",
    );
    debug!(
        mdns_hostname = %device.mdns.hostname,
        address = %device.address,
        csr_pem = %device.csr_pem,
        "Device init request payload",
    );

    let cert_pem = match state.ca.sign_csr(&device.mdns.hostname, &device.csr_pem).await {
        Ok(pem) => pem,
        Err(e) => {
            error!("Failed to sign device CSR: {}", e);
            return text_response(500, "Failed to sign device certificate");
        }
    };
    // add PEM to the brokerage for authentication
    if let Some(brokerage_handle) = &state.brokerage {
        if let Err(e) = brokerage_handle.register_cert_identity(&device.mdns.hostname, &cert_pem).await {
            error!("Failed to register device MQTT identity: {}", e);
            return text_response(500, "Failed to register device MQTT identity");
        }
    }

    let beacon_id = uuid::Uuid::new_v4().to_string();
    let broadcast_topic = format!("{}/cast", beacon_id);
    let receive_topic = format!("{}/rec", beacon_id);

    if let Some(tx) = &state.beacon_topic_tx {
        let listen_channel = TopicChannel {
            beacon_id: beacon_id.clone(),
            topic: MqttTopicConfig { topic: receive_topic.clone(), qos: 1 },
            topic_type: TopicType::Listen,
        };
        let broadcast_channel = TopicChannel {
            beacon_id: beacon_id.clone(),
            topic: MqttTopicConfig { topic: broadcast_topic.clone(), qos: 1 },
            topic_type: TopicType::Broadcast,
        };
        if let Err(e) = tx.try_send(listen_channel) {
            tracing::warn!(beacon_id, "Failed to register beacon listen topic: {}", e);
        }
        if let Err(e) = tx.try_send(broadcast_channel) {
            tracing::warn!(beacon_id, "Failed to register beacon broadcast topic: {}", e);
        }
    }

    let mut brokerage = state.brokerage_info.clone();
    brokerage.address = request.broker_host;

    let resp = serde_json::json!({
        "cert_pem": cert_pem,
        "brokerage": brokerage,
        "beacon_id": beacon_id,
    });
    debug!(
        mdns_hostname = %device.mdns.hostname,
        beacon_id,
        broadcast_topic,
        receive_topic,
        broker_address = %brokerage.address,
        broker_port = brokerage.port,
        cert_pem = %cert_pem,
        brokerage_info = %serde_json::to_string(&brokerage).unwrap_or_default(),
        "Device init response payload",
    );
    json_response(200, serde_json::to_vec(&resp).unwrap())
}

/// `X-PlaceNet-Register: <version>` — client cert registration: sign CSR, return cert + CA cert.
pub(in crate::services::beacon_management) async fn handle_client_register(state: &AppState, req: Request<Incoming>) -> Response<ProxyBody> {
    let request = match DeviceRegisterRequest::process_request(req).await {
        Ok(p) => p,
        Err(r) => return r,
    };

    let client_id = uuid::Uuid::new_v4().to_string();

    info!(client_id, "Client register — signing CSR");
    debug!(client_id, csr_pem = %request.csr_pem, "Client register request payload");

    let cert_pem = match state.ca.sign_csr(&client_id, &request.csr_pem).await {
        Ok(pem) => pem,
        Err(e) => {
            error!(client_id, "Failed to sign client CSR: {}", e);
            return text_response(500, "Failed to sign client certificate");
        }
    };

    let ca_cert_pem = match state.ca.ca_cert_pem().await {
        Ok(pem) => pem,
        Err(e) => {
            error!("Failed to retrieve CA cert: {}", e);
            return text_response(500, "Failed to retrieve CA certificate");
        }
    };

    info!(client_id, "Client registered successfully");
    debug!(
        client_id,
        cert_pem = %cert_pem,
        ca_cert_pem = %ca_cert_pem,
        "Client register response payload",
    );

    let resp = serde_json::json!({ "client_id": client_id, "cert_pem": cert_pem, "ca_cert_pem": ca_cert_pem });
    json_response(201, serde_json::to_vec(&resp).unwrap())
}
