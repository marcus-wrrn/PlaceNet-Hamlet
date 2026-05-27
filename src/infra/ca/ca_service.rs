use std::sync::Arc;
use sqlx::SqlitePool;
use tokio::sync::RwLock;
use tracing::info;

use super::operations::{self, CaState};

#[derive(Debug, Clone)]
pub struct DeviceCertRecord {
    pub device_id: String,
    pub cert_pem: String,
    pub issued_at: i64,
    pub expires_at: i64,
    pub revoked: bool,
}

/// Holds the loaded/generated CA state and the DB pool, shared across the application.
#[derive(Clone)]
pub struct CaService {
    pub state: Arc<RwLock<Option<CaState>>>,
    pool: SqlitePool,
}

impl CaService {
    /// Open (or create) the SQLite database at `db_url`, run migrations, and load or generate
    /// the root CA.
    pub async fn register(db_url: &str) -> Result<Self, String> {
        use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
        use std::str::FromStr;

        let options = SqliteConnectOptions::from_str(db_url)
            .map_err(|e| format!("Invalid database URL: {}", e))?
            .create_if_missing(true);

        let pool = SqlitePoolOptions::new()
            .connect_with(options)
            .await
            .map_err(|e| format!("Failed to open CA database: {}", e))?;

        let service = Self::new(pool);
        service.init().await?;
        Ok(service)
    }

    pub fn new(pool: SqlitePool) -> Self {
        Self {
            state: Arc::new(RwLock::new(None)),
            pool,
        }
    }

    /// Run migrations, load from database if a CA exists, otherwise generate a new root CA.
    pub async fn init(&self) -> Result<(), String> {
        sqlx::migrate!("./migrations")
            .run(&self.pool)
            .await
            .map_err(|e| format!("Failed to run CA migrations: {}", e))?;

        let ca_state = operations::load_or_generate_ca(&self.pool).await?;
        info!("CA ready (CN={})", ca_state.subject_cn);
        *self.state.write().await = Some(ca_state);
        Ok(())
    }

    /// Sign a PEM-encoded CSR, persist the issued cert under `device_id`, and return the cert PEM.
    /// If the device already has a cert it is replaced (upsert).
    pub async fn sign_csr(&self, device_id: &str, csr_pem: &str) -> Result<String, String> {
        let guard = self.state.read().await;
        let ca = guard.as_ref().ok_or("CA not initialised")?;
        let (cert_pem, issued_at, expires_at) = operations::sign_csr(ca, csr_pem)?;

        sqlx::query(
            "INSERT INTO device_certs (device_id, cert_pem, issued_at, expires_at, revoked)
             VALUES (?, ?, ?, ?, 0)
             ON CONFLICT(device_id) DO UPDATE SET
                 cert_pem   = excluded.cert_pem,
                 issued_at  = excluded.issued_at,
                 expires_at = excluded.expires_at,
                 revoked    = 0",
        )
        .bind(device_id)
        .bind(&cert_pem)
        .bind(issued_at)
        .bind(expires_at)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("Failed to persist device cert: {}", e))?;

        info!(device_id, "Issued and stored certificate");
        Ok(cert_pem)
    }

    /// Return the stored cert PEM for a device, or `None` if unknown.
    pub async fn get_cert(&self, device_id: &str) -> Result<Option<String>, String> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT cert_pem FROM device_certs WHERE device_id = ?")
                .bind(device_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| format!("Failed to query device cert: {}", e))?;

        Ok(row.map(|(pem,)| pem))
    }

    /// Return all known device cert records.
    pub async fn list_devices(&self) -> Result<Vec<DeviceCertRecord>, String> {
        let rows: Vec<(String, String, i64, i64, i64)> = sqlx::query_as(
            "SELECT device_id, cert_pem, issued_at, expires_at, revoked FROM device_certs",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| format!("Failed to list device certs: {}", e))?;

        Ok(rows
            .into_iter()
            .map(|(device_id, cert_pem, issued_at, expires_at, revoked)| DeviceCertRecord {
                device_id,
                cert_pem,
                issued_at,
                expires_at,
                revoked: revoked != 0,
            })
            .collect())
    }

    /// Mark a device cert as revoked. Returns an error if the device is unknown.
    pub async fn revoke(&self, device_id: &str) -> Result<(), String> {
        let rows_affected = sqlx::query(
            "UPDATE device_certs SET revoked = 1 WHERE device_id = ?",
        )
        .bind(device_id)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("Failed to revoke device cert: {}", e))?
        .rows_affected();

        if rows_affected == 0 {
            return Err(format!("Unknown device: {}", device_id));
        }

        info!(device_id, "Certificate revoked");
        Ok(())
    }

    /// Return the root CA certificate as PEM, or an error if not yet initialised.
    pub async fn ca_cert_pem(&self) -> Result<String, String> {
        let guard = self.state.read().await;
        let ca = guard.as_ref().ok_or("CA not initialised")?;
        Ok(ca.cert.pem())
    }

    /// Generate a CA-signed TLS server certificate for the MQTT broker.
    ///
    /// The certificate carries `ServerAuth` EKU and Subject Alternative Names for
    /// the supplied IPs and hostnames so that connecting TLS clients can verify it.
    /// A fresh cert is generated on every call (no persistence).
    pub async fn generate_broker_cert(
        &self,
        san_ips: &[std::net::IpAddr],
        san_hostnames: &[&str],
    ) -> Result<(String, String), String> {
        let guard = self.state.read().await;
        let ca = guard.as_ref().ok_or("CA not initialised")?;
        operations::generate_broker_identity(ca, san_ips, san_hostnames)
    }

    /// Return the node's own client cert and key PEM, generating and persisting them if absent.
    ///
    /// On first call a new key pair and CA-signed client certificate are created and stored in
    /// the `node_identity` table. Subsequent calls return the stored values unchanged, so the
    /// identity is stable across restarts.
    pub async fn ensure_node_identity(&self) -> Result<(String, String), String> {
        let row: Option<(String, String)> = sqlx::query_as(
            "SELECT cert_pem, key_pem FROM node_identity WHERE id = 1",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| format!("Failed to query node identity: {}", e))?;

        if let Some((cert_pem, key_pem)) = row {
            info!("Loaded existing node identity from database");
            return Ok((cert_pem, key_pem));
        }

        let guard = self.state.read().await;
        let ca = guard.as_ref().ok_or("CA not initialised")?;
        let (cert_pem, key_pem, issued_at, expires_at) =
            operations::generate_node_identity(ca)?;

        sqlx::query(
            "INSERT INTO node_identity (id, cert_pem, key_pem, issued_at, expires_at)
             VALUES (1, ?, ?, ?, ?)",
        )
        .bind(&cert_pem)
        .bind(&key_pem)
        .bind(issued_at)
        .bind(expires_at)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("Failed to persist node identity: {}", e))?;

        info!("Generated and stored new node identity certificate");
        Ok((cert_pem, key_pem))
    }

    /// Return a clone of the underlying SQLite pool.
    ///
    /// Intended for use by other services (e.g. `BroadcastKeyManagement`) that
    /// share the same database and benefit from running migrations in a single pass.
    pub fn pool(&self) -> SqlitePool {
        self.pool.clone()
    }

    /// Returns `true` if the device has a revoked certificate.
    pub async fn is_revoked(&self, device_id: &str) -> Result<bool, String> {
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT revoked FROM device_certs WHERE device_id = ?")
                .bind(device_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| format!("Failed to query revocation status: {}", e))?;

        Ok(row.map(|(r,)| r != 0).unwrap_or(false))
    }
}
