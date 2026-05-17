use rcgen::{
    Certificate, CertificateParams, CertificateSigningRequestParams,
    DistinguishedName, DnType, ExtendedKeyUsagePurpose, Ia5String, IsCa, KeyPair,
    KeyUsagePurpose, SanType,
};
use sqlx::SqlitePool;
use tracing::info;

const CA_CN: &str = "PlaceNet Root CA";

/// Live CA state held in memory after initialisation.
pub struct CaState {
    pub subject_cn: String,
    pub cert: Certificate,
    pub key_pair: KeyPair,
}

/// Load an existing CA from the database, or generate a new one and persist it.
pub async fn load_or_generate_ca(pool: &SqlitePool) -> Result<CaState, String> {
    if let Some(state) = load_ca_from_db(pool).await? {
        info!("Loading existing CA from database");
        Ok(state)
    } else {
        info!("No CA found — generating new root CA");
        generate_and_persist_ca(pool).await
    }
}

async fn load_ca_from_db(pool: &SqlitePool) -> Result<Option<CaState>, String> {
    let row: Option<(String, String)> = sqlx::query_as(
        "SELECT cert_pem, key_pem FROM ca_keys WHERE id = 1",
    )
    .fetch_optional(pool)
    .await
    .map_err(|e| format!("Failed to query CA from database: {}", e))?;

    let Some((cert_pem, key_pem)) = row else {
        return Ok(None);
    };

    let key_pair = KeyPair::from_pem(&key_pem)
        .map_err(|e| format!("Failed to parse CA private key: {}", e))?;
    let params = CertificateParams::from_ca_cert_pem(&cert_pem)
        .map_err(|e| format!("Failed to parse CA certificate: {}", e))?;
    let cert = params
        .self_signed(&key_pair)
        .map_err(|e| format!("Failed to reconstruct CA cert: {}", e))?;

    Ok(Some(CaState { subject_cn: CA_CN.to_string(), cert, key_pair }))
}

async fn generate_and_persist_ca(pool: &SqlitePool) -> Result<CaState, String> {
    let key_pair = KeyPair::generate()
        .map_err(|e| format!("Failed to generate CA key pair: {}", e))?;

    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];

    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, CA_CN);
    dn.push(DnType::OrganizationName, "PlaceNet");
    params.distinguished_name = dn;

    let cert = params
        .self_signed(&key_pair)
        .map_err(|e| format!("Failed to self-sign CA certificate: {}", e))?;

    let cert_pem = cert.pem();
    let key_pem = key_pair.serialize_pem();

    sqlx::query("INSERT INTO ca_keys (id, cert_pem, key_pem) VALUES (1, ?, ?)")
        .bind(&cert_pem)
        .bind(&key_pem)
        .execute(pool)
        .await
        .map_err(|e| format!("Failed to persist CA to database: {}", e))?;

    info!("Root CA stored in database");

    Ok(CaState { subject_cn: CA_CN.to_string(), cert, key_pair })
}

/// Generate a new key pair and a CA-signed client certificate for this node's own MQTT identity.
///
/// Returns `(cert_pem, key_pem, issued_at, expires_at)`.
/// The cert has `ExtendedKeyUsagePurpose::ClientAuth` so Mosquitto accepts it for mutual TLS.
pub fn generate_node_identity(ca: &CaState) -> Result<(String, String, i64, i64), String> {
    let key_pair = KeyPair::generate()
        .map_err(|e| format!("Failed to generate node identity key pair: {}", e))?;

    let mut params = CertificateParams::default();
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, "hamlet");
    dn.push(DnType::OrganizationName, "PlaceNet");
    params.distinguished_name = dn;
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];

    let cert = params
        .signed_by(&key_pair, &ca.cert, &ca.key_pair)
        .map_err(|e| format!("Failed to sign node identity certificate: {}", e))?;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let expires_at = now + 365 * 24 * 60 * 60;

    Ok((cert.pem(), key_pair.serialize_pem(), now, expires_at))
}

/// Generate a new key pair and CA-signed TLS server certificate for the MQTT broker.
///
/// The cert carries `ServerAuth` EKU and the provided SANs so that TLS clients
/// (e.g. the beacon) can verify the broker by IP address or hostname.
pub fn generate_broker_identity(
    ca: &CaState,
    san_ips: &[std::net::IpAddr],
    san_hostnames: &[&str],
) -> Result<(String, String), String> {
    let key_pair = KeyPair::generate()
        .map_err(|e| format!("Failed to generate broker key pair: {}", e))?;

    let mut params = CertificateParams::default();
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, "placenet-broker");
    dn.push(DnType::OrganizationName, "PlaceNet");
    params.distinguished_name = dn;
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];

    let mut sans: Vec<SanType> = san_ips.iter().map(|ip| SanType::IpAddress(*ip)).collect();
    for name in san_hostnames {
        if let Ok(s) = Ia5String::try_from(*name) {
            sans.push(SanType::DnsName(s));
        }
    }
    params.subject_alt_names = sans;

    let cert = params
        .signed_by(&key_pair, &ca.cert, &ca.key_pair)
        .map_err(|e| format!("Failed to sign broker certificate: {}", e))?;

    Ok((cert.pem(), key_pair.serialize_pem()))
}

/// Sign a PEM-encoded CSR with the loaded root CA.
///
/// Returns `(cert_pem, issued_at, expires_at)` where the timestamps are Unix seconds.
pub fn sign_csr(ca: &CaState, csr_pem: &str) -> Result<(String, i64, i64), String> {
    let csr = CertificateSigningRequestParams::from_pem(csr_pem)
        .map_err(|e| format!("Invalid CSR: {}", e))?;

    let signed = csr
        .signed_by(&ca.cert, &ca.key_pair)
        .map_err(|e| format!("Failed to sign CSR: {}", e))?;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    // TODO: Add an expiration system
    let expires_at = now + 365 * 24 * 60 * 60;

    Ok((signed.pem(), now, expires_at))
}
