use tokio_rustls::TlsAcceptor;
use tracing::{info, warn};

use super::internals::BoundGateway;

pub fn spawn_tls_accept_loop(mut gateway: BoundGateway, tls_acceptor: TlsAcceptor) {
    tokio::spawn(async move {
        info!(
            "HTTPS service listening on {} → upstream :{}",
            gateway.local_addr, gateway.state.upstream_port
        );
        loop {
            let (tcp_stream, peer_addr) = tokio::select! {
                result = gateway.listener.accept() => match result {
                    Ok(pair) => pair,
                    Err(e) => { warn!("TCP accept error: {}", e); continue; }
                },
                _ = &mut gateway.shutdown_rx => { info!("HTTPS service shutting down"); break; }
            };
            tokio::spawn(super::proxy::serve_tls_connection(
                tcp_stream,
                tls_acceptor.clone(),
                gateway.state.clone(),
                peer_addr,
            ));
        }
    });
}

pub fn spawn_plain_accept_loop(mut gateway: BoundGateway) {
    tokio::spawn(async move {
        info!(
            "HTTP service listening on {} → upstream :{} (TLS disabled)",
            gateway.local_addr, gateway.state.upstream_port
        );
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
