use std::convert::Infallible;
use std::net::SocketAddr;

use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;
use tracing::{error, info, warn};

use super::handlers::{handle_client_register, handle_device_init, handle_health};
use super::headers::{HEADER_HEALTH, HEADER_INIT, HEADER_REGISTER};
use super::response::text_response;
use super::internals::{AppState, BoxError, ProxyBody};

/// Dispatch a request: handle PlaceNet protocol requests locally, proxy everything else upstream.
pub(super) async fn dispatch(state: AppState, req: Request<Incoming>) -> Result<Response<ProxyBody>, Infallible> {
    if req.headers().contains_key(HEADER_HEALTH) {
        return Ok(handle_health(req).await);
    }
    if req.headers().contains_key(HEADER_INIT) {
        return Ok(handle_device_init(&state, req).await);
    }
    if req.headers().contains_key(HEADER_REGISTER) {
        return Ok(handle_client_register(&state, req).await);
    }

    match try_forward(state.upstream_port, req).await {
        Ok(resp) => Ok(resp),
        Err(e) => {
            error!("Proxy error: {}", e);
            Ok(text_response(502, "Bad Gateway"))
        }
    }
}

pub(super) async fn try_forward(upstream_port: u16, req: Request<Incoming>) -> Result<Response<ProxyBody>, BoxError> {
    let upstream = TcpStream::connect(("127.0.0.1", upstream_port)).await?;
    let io = TokioIo::new(upstream);

    let (mut sender, conn) = hyper::client::conn::http1::Builder::new()
        .handshake(io)
        .await?;

    tokio::spawn(async move {
        if let Err(e) = conn.await {
            error!("Upstream connection error: {}", e);
        }
    });

    let resp = sender.send_request(req).await?;
    Ok(resp.map(|b| b.boxed()))
}

pub(super) async fn serve_connection(stream: TcpStream, state: AppState, peer_addr: SocketAddr) {
    let io = TokioIo::new(stream);
    let svc = hyper::service::service_fn(move |req| {
        let state = state.clone();
        async move { dispatch(state, req).await }
    });

    if let Err(e) = http1::Builder::new().serve_connection(io, svc).await {
        let msg = e.to_string();
        if !msg.contains("connection reset") && !msg.contains("broken pipe") {
            error!(%peer_addr, "HTTP connection error: {}", e);
        }
    }
}

pub(super) async fn serve_tls_connection(
    stream: TcpStream,
    tls_acceptor: TlsAcceptor,
    state: AppState,
    peer_addr: SocketAddr,
) {
    let tls_stream = match tls_acceptor.accept(stream).await {
        Ok(s) => s,
        Err(e) => { warn!(%peer_addr, "TLS handshake failed: {}", e); return; }
    };

    info!(%peer_addr, "TLS handshake complete");

    let io = TokioIo::new(tls_stream);
    let svc = hyper::service::service_fn(move |req| {
        let state = state.clone();
        async move { dispatch(state, req).await }
    });

    if let Err(e) = http1::Builder::new().serve_connection(io, svc).await {
        let msg = e.to_string();
        if !msg.contains("connection reset") && !msg.contains("broken pipe") {
            error!(%peer_addr, "HTTPS connection error: {}", e);
        }
    }
}
