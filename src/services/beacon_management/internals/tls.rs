use std::sync::Arc;

use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls_pemfile::{certs, pkcs8_private_keys};

use crate::infra::ca::CaService;

/// Build a `rustls::ServerConfig` from the live CA.
///
/// The server presents the CA cert itself as its identity (self-signed).
/// Client certificates are not required at this layer — device authentication
/// is handled at the HTTP layer via the `X-PlaceNet-Init` handshake.
pub async fn build_tls_config(ca: &CaService) -> Result<Arc<ServerConfig>, String> {
    let ca_guard = ca.state.read().await;
    let ca_state = ca_guard.as_ref().ok_or("CA not initialised")?;

    // --- Certificate chain -------------------------------------------------
    let cert_pem = ca_state.cert.pem();
    let cert_chain: Vec<CertificateDer<'static>> = {
        let mut reader = std::io::BufReader::new(cert_pem.as_bytes());
        certs(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Failed to parse CA cert PEM: {}", e))?
    };

    // --- Private key -------------------------------------------------------
    let key_pem = ca_state.key_pair.serialize_pem();
    let private_key: PrivateKeyDer<'static> = {
        let mut reader = std::io::BufReader::new(key_pem.as_bytes());
        let keys: Vec<_> = pkcs8_private_keys(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Failed to parse CA private key PEM: {}", e))?;
        let raw = keys.into_iter().next().ok_or("No PKCS8 private key found in CA PEM")?;
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(raw.secret_pkcs8_der().to_vec()))
    };

    // --- Build ServerConfig ------------------------------------------------
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, private_key)
        .map_err(|e| format!("Failed to build TLS server config: {}", e))?;

    Ok(Arc::new(config))
}
