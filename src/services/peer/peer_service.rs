use hyper::{Method, Request, Uri};
use hyper::body::Bytes;
use hyper_util::rt::TokioIo;
use http_body_util::{BodyExt, Full};
use tokio::net::TcpStream;
use tracing::{error, info};

/// Send a plain-text message to a peer hamlet node.
///
/// `base_url` should be like `"http://127.0.0.1:9090"` (no trailing slash).
pub async fn send_message(base_url: &str, message: &str) -> Result<(), String> {
    let url: Uri = format!("{}/peer/message", base_url)
        .parse()
        .map_err(|e| format!("Invalid peer URL: {}", e))?;

    let host = url.host().ok_or("Peer URL has no host")?;
    let port = url.port_u16().unwrap_or(80);
    let addr = format!("{}:{}", host, port);

    let stream = TcpStream::connect(&addr)
        .await
        .map_err(|e| format!("Failed to connect to peer {}: {}", addr, e))?;

    let io = TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
        .await
        .map_err(|e| format!("HTTP handshake failed: {}", e))?;

    tokio::spawn(async move {
        if let Err(e) = conn.await {
            error!("Peer connection error: {}", e);
        }
    });

    let body = Full::new(Bytes::from(message.to_owned()));
    let request = Request::builder()
        .method(Method::POST)
        .uri(url.path_and_query().map(|pq| pq.as_str()).unwrap_or("/peer/message"))
        .header("host", format!("{}:{}", host, port))
        .header("content-type", "text/plain")
        .body(body)
        .map_err(|e| format!("Failed to build request: {}", e))?;

    let response = sender
        .send_request(request)
        .await
        .map_err(|e| format!("Failed to send request to peer: {}", e))?;

    let status = response.status();
    let body_bytes = response
        .into_body()
        .collect()
        .await
        .map_err(|e| format!("Failed to read peer response body: {}", e))?
        .to_bytes();

    if status.is_success() {
        info!(peer = %addr, "Peer acknowledged: {}", String::from_utf8_lossy(&body_bytes));
        Ok(())
    } else {
        Err(format!(
            "Peer returned {} — {}",
            status,
            String::from_utf8_lossy(&body_bytes)
        ))
    }
}
