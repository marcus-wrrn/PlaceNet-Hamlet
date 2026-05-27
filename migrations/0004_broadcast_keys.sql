-- Broadcast authentication keys for beacon discovery messages.
--
-- Each key carries a 4-byte ID (sent over LoRa as `kid`) and 16 bytes of
-- AES-128 key material used to derive a 4-byte HMAC-SHA256 token (`tok`).
-- Key material is never sent over the air; it is distributed to beacons
-- via the TLS-protected MQTT channel during or after device handshake.
--
-- Lifecycle:
--   is_active=1  →  currently used for outbound broadcasts
--   is_active=0  →  rotated out but kept until expires_at for beacon catch-up
--   (deleted)    →  past expires_at, swept by BroadcastKeyManagement::sweep_expired

CREATE TABLE IF NOT EXISTS broadcast_keys (
    key_id       BLOB    NOT NULL PRIMARY KEY,   -- 4 bytes: compact OTA identifier
    key_material BLOB    NOT NULL,               -- 16 bytes: AES-128 key for HMAC-SHA256
    created_at   INTEGER NOT NULL,               -- Unix seconds: key generation time
    rotates_at   INTEGER NOT NULL,               -- Unix seconds: when this key stops being active
    expires_at   INTEGER NOT NULL,               -- Unix seconds: when this key is deleted
    is_active    INTEGER NOT NULL DEFAULT 1      -- 1 = current outbound key, 0 = retired
);

-- Fast lookup for the current active key.
CREATE INDEX IF NOT EXISTS idx_broadcast_keys_active
    ON broadcast_keys (is_active, rotates_at DESC);
