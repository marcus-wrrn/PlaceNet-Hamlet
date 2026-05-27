use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use sha2::Sha256;
use sqlx::SqlitePool;

type HmacSha256 = Hmac<Sha256>;

// ── Public types ──────────────────────────────────────────────────────────────

/// A single broadcast key record loaded from the database.
///
/// The `key_id` (4 bytes) is sent in every broadcast as `kid`.
/// The `key_material` (16 bytes, AES-128 sized) is used locally to derive
/// a per-slot HMAC token; it is **never** transmitted over LoRa.
#[derive(Debug, Clone)]
pub struct BroadcastKey {
    pub key_id: [u8; 4],
    pub key_material: [u8; 16],
    /// Unix seconds: when this key stops being the active outbound key.
    pub rotates_at: i64,
    /// Unix seconds: when this key is permanently deleted from the DB.
    pub expires_at: i64,
}

// ── BroadcastKeyManagement ────────────────────────────────────────────────────

/// Manages the lifecycle of broadcast authentication keys.
///
/// ## Protocol
/// Each broadcast payload includes two compact fields:
/// - `kid` — 8-char lowercase hex (4-byte key ID): tells the beacon which key to use.
/// - `tok` — 8-char lowercase hex (4-byte truncated HMAC-SHA256): authenticates the sender.
///
/// The token is derived as:
/// ```text
/// slot   = unix_timestamp / rotation_interval_secs
/// tok[0..4] = HMAC-SHA256(key_material, slot.to_be_bytes())[0..4]
/// ```
///
/// Beacons verify `tok` by independently computing the same value. They should accept
/// tokens for `slot`, `slot - 1`, and `slot + 1` to tolerate clock drift.
///
/// ## Key lifecycle
/// ```text
/// Active  (is_active=1, rotates_at > now)  →  current outbound key
/// Retired (is_active=0, expires_at > now)  →  kept so late-joining beacons can verify
/// (deleted via sweep_expired)              →  past expires_at
/// ```
///
/// ## Security note
/// Key material is stored as plaintext BLOB in SQLite. The security boundary is
/// OS-level file permissions on the database file. SQLCipher can be layered on
/// top if encryption at rest is required.
pub struct BroadcastKeyManagement {
    pool: SqlitePool,
    rotation_interval: Duration,
    key_expiry: Duration,
}

impl BroadcastKeyManagement {
    /// Create a new manager.
    ///
    /// - `rotation_interval` — how long each key is the active outbound key.
    /// - `key_expiry` — how long a retired key is kept before deletion.
    ///   Must be greater than `rotation_interval` to give beacons time to receive
    ///   the new key before the old one is purged.
    pub fn new(pool: SqlitePool, rotation_interval: Duration, key_expiry: Duration) -> Self {
        Self { pool, rotation_interval, key_expiry }
    }

    /// Return the current active key, rotating if the active key has passed its
    /// `rotates_at` deadline or no key exists yet.
    ///
    /// This is the primary entry point called at the start of each broadcast cycle.
    pub async fn get_or_rotate_key(&self) -> Result<BroadcastKey, String> {
        let now = unix_now();

        let row: Option<(Vec<u8>, Vec<u8>, i64, i64)> = sqlx::query_as(
            "SELECT key_id, key_material, rotates_at, expires_at
             FROM broadcast_keys
             WHERE is_active = 1 AND rotates_at > ?
             ORDER BY rotates_at DESC
             LIMIT 1",
        )
        .bind(now)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| format!("Failed to query broadcast keys: {}", e))?;

        if let Some((key_id_bytes, key_material_bytes, rotates_at, expires_at)) = row {
            return build_key(&key_id_bytes, &key_material_bytes, rotates_at, expires_at);
        }

        // No valid active key — generate a new one.
        tracing::info!("No active broadcast key found - rotating to a new key");
        self.rotate_key().await
    }

    /// Derive a 4-byte authentication token for the current time slot.
    ///
    /// The slot advances every `rotation_interval` seconds so the token changes
    /// at the same cadence as key rotation. The derivation is:
    /// ```text
    /// HMAC-SHA256(key_material, (unix_now / rotation_secs).to_be_bytes())[0..4]
    /// ```
    ///
    /// Beacons that hold `key_material` can re-derive this value independently.
    pub fn generate_token(&self, key: &BroadcastKey) -> [u8; 4] {
        let rotation_secs = self.rotation_interval.as_secs().max(1);
        let slot = unix_now() as u64 / rotation_secs;

        let mut mac = HmacSha256::new_from_slice(&key.key_material)
            .expect("HMAC-SHA256 accepts any key length");
        mac.update(&slot.to_be_bytes());
        let result = mac.finalize().into_bytes();

        let mut token = [0u8; 4];
        token.copy_from_slice(&result[..4]);
        token
    }

    /// Return the over-the-air fields ready to embed in the broadcast JSON payload.
    ///
    /// Returns `(kid, tok)` — both 8-character lowercase hex strings (4 bytes each).
    /// Total overhead per broadcast: 16 ASCII characters of key authentication data.
    pub fn ota_fields(&self, key: &BroadcastKey) -> (String, String) {
        let kid = hex_encode(&key.key_id);
        let tok = hex_encode(&self.generate_token(key));
        (kid, tok)
    }

    /// Delete all keys whose `expires_at` timestamp is in the past.
    ///
    /// Should be called once per broadcast cycle. Returns the number of rows deleted.
    pub async fn sweep_expired(&self) -> Result<u64, String> {
        let now = unix_now();
        let rows = sqlx::query("DELETE FROM broadcast_keys WHERE expires_at <= ?")
            .bind(now)
            .execute(&self.pool)
            .await
            .map_err(|e| format!("Failed to sweep expired broadcast keys: {}", e))?
            .rows_affected();

        if rows > 0 {
            tracing::info!(deleted = rows, "Swept expired broadcast keys");
        }
        Ok(rows)
    }

    // ── Private ───────────────────────────────────────────────────────────────

    /// Generate a fresh random key, mark previous active keys as retired, and persist.
    async fn rotate_key(&self) -> Result<BroadcastKey, String> {
        let now = unix_now();
        let rotates_at = now + self.rotation_interval.as_secs() as i64;
        let expires_at = now + self.key_expiry.as_secs() as i64;

        let key_material: [u8; 16] = rand::random();
        let key_id: [u8; 4] = rand::random();

        // Retire all currently active keys.
        sqlx::query("UPDATE broadcast_keys SET is_active = 0 WHERE is_active = 1")
            .execute(&self.pool)
            .await
            .map_err(|e| format!("Failed to retire active broadcast keys: {}", e))?;

        // Persist the new key.
        sqlx::query(
            "INSERT INTO broadcast_keys
                 (key_id, key_material, created_at, rotates_at, expires_at, is_active)
             VALUES (?, ?, ?, ?, ?, 1)",
        )
        .bind(key_id.as_slice())
        .bind(key_material.as_slice())
        .bind(now)
        .bind(rotates_at)
        .bind(expires_at)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("Failed to insert new broadcast key: {}", e))?;

        tracing::info!(
            kid = %hex_encode(&key_id),
            rotates_at,
            expires_at,
            "Generated and persisted new broadcast key"
        );

        Ok(BroadcastKey { key_id, key_material, rotates_at, expires_at })
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

fn build_key(
    key_id_bytes: &[u8],
    key_material_bytes: &[u8],
    rotates_at: i64,
    expires_at: i64,
) -> Result<BroadcastKey, String> {
    let key_id: [u8; 4] = key_id_bytes.try_into().map_err(|_| {
        format!(
            "Corrupt broadcast_keys row: key_id is {} bytes, expected 4",
            key_id_bytes.len()
        )
    })?;
    let key_material: [u8; 16] = key_material_bytes.try_into().map_err(|_| {
        format!(
            "Corrupt broadcast_keys row: key_material is {} bytes, expected 16",
            key_material_bytes.len()
        )
    })?;
    Ok(BroadcastKey { key_id, key_material, rotates_at, expires_at })
}
