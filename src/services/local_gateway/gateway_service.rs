use async_trait::async_trait;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio_rustls::TlsAcceptor;
use tracing::info;

use crate::config::HttpConfig;
use crate::infra::ca::CaService;
use crate::services::mqtt_brokerage::MqttBrokerageHandle;
use crate::supervisor::ManagedService;
use super::manager::TopicChannel;
use super::internals::{AppState, BoundGateway, MqttBrokerageInfo};

pub struct GatewayService {
    config: HttpConfig,
    ca: CaService,
    brokerage_info: MqttBrokerageInfo,
    brokerage: Option<MqttBrokerageHandle>,
    beacon_topic_tx: Option<mpsc::Sender<TopicChannel>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
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

        let (gateway, shutdown_tx) = self.initialize().await?;
        self.shutdown_tx = Some(shutdown_tx);

        if self.config.tls_enabled {
            let tls_config = super::internals::tls::build_tls_config(&self.ca).await?;
            let tls_acceptor = TlsAcceptor::from(tls_config);
            info!(
                "HTTPS gateway bound on {} → upstream :{}",
                gateway.local_addr, gateway.state.upstream_port
            );
            super::tasks::spawn_tls_accept_loop(gateway, tls_acceptor);
        } else {
            info!(
                "HTTP gateway bound on {} → upstream :{} (TLS disabled)",
                gateway.local_addr, gateway.state.upstream_port
            );
            super::tasks::spawn_plain_accept_loop(gateway);
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
