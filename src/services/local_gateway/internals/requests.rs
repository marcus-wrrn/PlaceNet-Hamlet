use http_body_util::{BodyExt, Limited};
use hyper::body::Incoming;
use hyper::{Request, Response};
use tracing::warn;

use super::handshake::DeviceInfo;
use super::headers::HEADER_INIT;
use super::response::text_response;
use super::{ProxyBody, BODY_LIMIT, SUPPORTED_VERSION};

pub(super) struct DeviceInitRequest {
    // TODO: use version checking in later iterations
    pub(super) _version: String,
    pub(super) broker_host: String,
    pub(super) device: DeviceInfo,
}

impl DeviceInitRequest {
    pub(super) async fn process_request(req: Request<Incoming>) -> Result<Self, Response<ProxyBody>> {
        let version = match req.headers().get(HEADER_INIT).and_then(|v| v.to_str().ok()) {
            Some(v) => v.to_owned(),
            None => return Err(text_response(400, "Invalid X-PlaceNet-Init header")),
        };

        if version != SUPPORTED_VERSION {
            warn!("Unsupported PlaceNet init version: {}", version);
            return Err(text_response(400, "Unsupported version"));
        }

        let broker_host = req
            .headers()
            .get(hyper::header::HOST)
            .and_then(|v| v.to_str().ok())
            .map(|h| {
                // Strip port: "192.168.2.39:8080" → "192.168.2.39"
                match h.rfind(':') {
                    Some(i) if !h.starts_with('[') => h[..i].to_string(),
                    _ => h.to_string(),
                }
            })
            .unwrap_or_else(|| "localhost".to_string());

        let body_bytes = match Limited::new(req.into_body(), BODY_LIMIT).collect().await {
            Ok(b) => b.to_bytes(),
            Err(e) => {
                warn!("Failed to read device init body: {}", e);
                return Err(text_response(400, "Failed to read request body"));
            }
        };

        let device: DeviceInfo = match serde_json::from_slice(&body_bytes) {
            Ok(d) => d,
            Err(e) => {
                warn!("Invalid device init payload: {}", e);
                return Err(text_response(422, "Invalid JSON body"));
            }
        };

        Ok(Self { _version: version, broker_host, device })
    }
}

pub(super) struct DeviceRegisterRequest {
    pub(super) csr_pem: String,
}

impl DeviceRegisterRequest {
    pub(super) async fn process_request(req: Request<Incoming>) -> Result<Self, Response<ProxyBody>> {
        let body_bytes = match Limited::new(req.into_body(), BODY_LIMIT).collect().await {
            Ok(b) => b.to_bytes(),
            Err(e) => {
                warn!("Failed to read client register body: {}", e);
                return Err(text_response(400, "Failed to read request body"));
            }
        };

        #[derive(serde::Deserialize)]
        struct Payload { csr_pem: String }

        let payload: Payload = match serde_json::from_slice(&body_bytes) {
            Ok(p) => p,
            Err(e) => {
                warn!("Invalid client register payload: {}", e);
                return Err(text_response(422, "Invalid JSON body"));
            }
        };

        Ok(Self { csr_pem: payload.csr_pem })
    }
}
