//! Minimal HTTPS client for the gateway `/api/login` call.
//!
//! Uses the existing hyper 1.x + tokio-rustls stack (ring provider) rather than
//! pulling in a heavier HTTP client. Supports both `http://` (dev) and
//! `https://` gateways, verifying the server against a pinned CA or the webpki
//! root store.

use std::path::Path;
use std::sync::Arc;

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::{Method, Request};
use hyper_util::rt::TokioIo;
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, RootCertStore};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tracing::warn;

use super::protocol::{LoginRequest, LoginResponse};

/// Outcome of a login attempt, distinguishing auth rejection (don't retry the
/// same creds blindly) from transient/connection errors (retry as-is).
pub enum LoginError {
    /// The gateway rejected the credentials (HTTP 401).
    Unauthorized,
    /// Any other failure (network, 5xx, parse, …) — safe to retry.
    Transient(String),
}

/// POST credentials to `{base_url}/api/login` and parse the response.
pub async fn login(
    base_url: &str,
    req: &LoginRequest,
    cafile: Option<&Path>,
) -> Result<LoginResponse, LoginError> {
    let url = Url::parse(base_url).map_err(LoginError::Transient)?;
    let path = format!("{}/api/login", url.path.trim_end_matches('/'));

    let body = serde_json::to_vec(req)
        .map_err(|e| LoginError::Transient(format!("serialise login: {e}")))?;

    let stream = TcpStream::connect((url.host.as_str(), url.port))
        .await
        .map_err(|e| LoginError::Transient(format!("connect {}:{}: {e}", url.host, url.port)))?;

    if url.tls {
        let tls = tls_connector(cafile).map_err(LoginError::Transient)?;
        let server_name = ServerName::try_from(url.host.clone())
            .map_err(|e| LoginError::Transient(format!("invalid server name: {e}")))?;
        let tls_stream = tls
            .connect(server_name, stream)
            .await
            .map_err(|e| LoginError::Transient(format!("tls handshake: {e}")))?;
        send_request(TokioIo::new(tls_stream), &url.host, &path, body).await
    } else {
        warn!("gateway login over plaintext HTTP (dev only)");
        send_request(TokioIo::new(stream), &url.host, &path, body).await
    }
}

async fn send_request<I>(io: I, host: &str, path: &str, body: Vec<u8>) -> Result<LoginResponse, LoginError>
where
    I: hyper::rt::Read + hyper::rt::Write + Unpin + Send + 'static,
{
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
        .await
        .map_err(|e| LoginError::Transient(format!("http handshake: {e}")))?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            warn!("gateway login connection error: {e}");
        }
    });

    let request = Request::builder()
        .method(Method::POST)
        .uri(path)
        .header(hyper::header::HOST, host)
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(body)))
        .map_err(|e| LoginError::Transient(format!("build request: {e}")))?;

    let resp = sender
        .send_request(request)
        .await
        .map_err(|e| LoginError::Transient(format!("send request: {e}")))?;

    let status = resp.status();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .map_err(|e| LoginError::Transient(format!("read body: {e}")))?
        .to_bytes();

    if status == hyper::StatusCode::UNAUTHORIZED {
        return Err(LoginError::Unauthorized);
    }
    if !status.is_success() {
        return Err(LoginError::Transient(format!(
            "login returned {}: {}",
            status,
            String::from_utf8_lossy(&bytes)
        )));
    }

    serde_json::from_slice::<LoginResponse>(&bytes)
        .map_err(|e| LoginError::Transient(format!("parse login response: {e}")))
}

fn tls_connector(cafile: Option<&Path>) -> Result<TlsConnector, String> {
    let mut roots = RootCertStore::empty();
    if let Some(cafile) = cafile {
        let pem = std::fs::read(cafile)
            .map_err(|e| format!("read gateway CA '{}': {e}", cafile.display()))?;
        let mut reader = std::io::BufReader::new(pem.as_slice());
        for cert in rustls_pemfile::certs(&mut reader) {
            let cert = cert.map_err(|e| format!("parse gateway CA: {e}"))?;
            roots.add(cert).map_err(|e| format!("add gateway CA: {e}"))?;
        }
    } else {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(TlsConnector::from(Arc::new(config)))
}

/// A tiny URL split — enough for `scheme://host[:port][/path]`.
struct Url {
    tls: bool,
    host: String,
    port: u16,
    path: String,
}

impl Url {
    fn parse(input: &str) -> Result<Self, String> {
        let (scheme, rest) = input
            .split_once("://")
            .ok_or_else(|| format!("invalid gateway url '{input}'"))?;
        let tls = match scheme {
            "https" => true,
            "http" => false,
            other => return Err(format!("unsupported gateway scheme '{other}'")),
        };
        let (authority, path) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i..]),
            None => (rest, "/"),
        };
        let (host, port) = match authority.rsplit_once(':') {
            Some((h, p)) if !h.is_empty() => {
                let port = p.parse::<u16>().map_err(|_| format!("invalid port in '{input}'"))?;
                (h.to_string(), port)
            }
            _ => (authority.to_string(), if tls { 443 } else { 80 }),
        };
        Ok(Self { tls, host, port, path: path.to_string() })
    }
}
