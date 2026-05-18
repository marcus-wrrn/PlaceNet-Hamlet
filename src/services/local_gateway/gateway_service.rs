use async_trait::async_trait;
use http_body_util::combinators::BoxBody;
use hyper::body::Bytes;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio_rustls::TlsAcceptor;
use tracing::{info, warn};

use crate::config::HttpConfig;
use crate::infra::ca::CaService;
use crate::services::mqtt_brokerage::MqttBrokerageHandle;
use crate::supervisor::ManagedService;
use super::handshake::{MqttBrokerageInfo, TopicChannel};

pub(super) const SUPPORTED_VERSION: &str = "0.0.1";
pub(super) const BODY_LIMIT: usize = 64 * 1024;

pub(super) type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub(super) type ProxyBody = BoxBody<Bytes, hyper::Error>;

#[derive(Clone)]
pub(super) struct AppState {
    pub(super) ca: CaService,
    pub(super) brokerage_info: MqttBrokerageInfo,
    pub(super) brokerage: Option<MqttBrokerageHandle>,
    pub(super) upstream_port: u16,
    /// Sends newly assigned beacon broadcast topics to the app for subscription and broadcasting.
    pub(super) beacon_topic_tx: Option<mpsc::Sender<TopicChannel>>,
}

pub struct GatewayService {
    config: HttpConfig,
    ca: CaService,
    brokerage_info: MqttBrokerageInfo,
    brokerage: Option<MqttBrokerageHandle>,
    beacon_topic_tx: Option<mpsc::Sender<TopicChannel>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

pub(super) struct BoundGateway {
    pub(super) state: AppState,
    pub(super) listener: TcpListener,
    pub(super) local_addr: SocketAddr,
    pub(super) shutdown_rx: oneshot::Receiver<()>,
}

impl GatewayService {
    pub fn new(
        config: HttpConfig,
        ca: CaService,
        brokerage_info: MqttBrokerageInfo,
        brokerage: Option<MqttBrokerageHandle>,
        beacon_topic_tx: Option<mpsc::Sender<TopicChannel>>,
    ) -> Self {
        Self { config, ca, brokerage_info, brokerage, beacon_topic_tx, shutdown_tx: None }
    }

    async fn initialize(&self) -> Result<(BoundGateway, oneshot::Sender<()>), String> {
        let addr = format!("{}:{}", self.config.host, self.config.port);
        let state = AppState {
            ca: self.ca.clone(),
            brokerage_info: self.brokerage_info.clone(),
            brokerage: self.brokerage.clone(),
            upstream_port: self.config.upstream_port,
            beacon_topic_tx: self.beacon_topic_tx.clone(),
        };

        let listener = TcpListener::bind(&addr).await
            .map_err(|e| format!("Failed to bind listener on {}: {}", addr, e))?;

        let local_addr = listener.local_addr()
            .map_err(|e| format!("Failed to get local address: {}", e))?;

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        Ok((BoundGateway { state, listener, local_addr, shutdown_rx }, shutdown_tx))
    }
}

#[async_trait]
impl ManagedService for GatewayService {
    async fn start(&mut self) -> Result<u32, String> {
        if self.shutdown_tx.is_some() {
            return Err("GatewayService is already running".to_string());
        }
        
        let (mut gateway, shutdown_tx) = self.initialize().await?;
        self.shutdown_tx = Some(shutdown_tx);

        if self.config.tls_enabled {
            let tls_config = super::tls::build_tls_config(&self.ca).await?;
            let tls_acceptor = TlsAcceptor::from(tls_config);

            tokio::spawn(async move {
                info!("HTTPS service listening on {} → upstream :{}", gateway.local_addr, gateway.state.upstream_port);

                loop {
                    let (tcp_stream, peer_addr) = tokio::select! {
                        result = gateway.listener.accept() => match result {
                            Ok(pair) => pair,
                            Err(e) => { warn!("TCP accept error: {}", e); continue; }
                        },
                        _ = &mut gateway.shutdown_rx => { info!("HTTPS service shutting down"); break; }
                    };

                    tokio::spawn(super::proxy::serve_tls_connection(tcp_stream, tls_acceptor.clone(), gateway.state.clone(), peer_addr));
                }
            });
        } else {
            tokio::spawn(async move {
                info!("HTTP service listening on {} → upstream :{} (TLS disabled)", gateway.local_addr, gateway.state.upstream_port);

                loop {
                    let (tcp_stream, peer_addr) = tokio::select! {
                        result = gateway.listener.accept() => match result {
                            Ok(pair) => pair,
                            Err(e) => { warn!("TCP accept error: {}", e); continue; }
                        },
                        _ = &mut gateway.shutdown_rx => { info!("HTTP service shutting down"); break; }
                    };

                    tokio::spawn(super::proxy::serve_connection(tcp_stream, gateway.state.clone(), peer_addr));
                }
            });
        }

        Ok(0)
    }

    async fn stop(&mut self) -> Result<(), String> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
            Ok(())
        } else {
            Err("GatewayService is not running".to_string())
        }
    }

    async fn is_running(&mut self) -> bool {
        self.shutdown_tx.is_some()
    }
}
