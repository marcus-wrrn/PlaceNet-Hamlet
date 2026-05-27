use std::path::PathBuf;
use tracing::info;

pub struct MqttClientConfig {
    pub client_id: String,
    pub host: String,
    pub port: u16,
    pub tls_enabled: bool,
    /// Path to the CA certificate used to verify the broker's TLS certificate.
    pub cafile: PathBuf,
    /// Path to this node's client certificate (used for TLS mutual auth).
    pub certfile: PathBuf,
    /// Path to this node's client private key (used for TLS mutual auth).
    pub keyfile: PathBuf,
    /// Username for plain (non-TLS) MQTT auth.
    pub username: String,
    /// Password for plain (non-TLS) MQTT auth.
    pub password: String,
}

impl MqttClientConfig {
    fn from_env(config_dir: &PathBuf) -> Self {
        let client_id = std::env::var("MQTT_CLIENT_ID")
            .unwrap_or_else(|_| "hamlet".to_string());
        let host = "localhost".to_string();
        let tls_enabled = std::env::var("MQTT_TLS_ENABLED")
            .unwrap_or_else(|_| "false".to_string())
            .trim()
            .eq_ignore_ascii_case("true");

        let port: u16 = if tls_enabled {
            std::env::var("MQTTS_PORT")
                .unwrap_or_else(|_| "8883".to_string())
                .parse()
                .unwrap_or(8883)
        } else {
            std::env::var("MQTT_PORT")
                .unwrap_or_else(|_| "1883".to_string())
                .parse()
                .unwrap_or(1883)
        };
        let cafile = config_dir.join(
            std::env::var("MQTT_CAFILE").unwrap_or_else(|_| "certs/ca.crt".to_string()),
        );
        let certfile = config_dir.join(
            std::env::var("MQTT_CLIENT_CERTFILE").unwrap_or_else(|_| "certs/client.crt".to_string()),
        );
        let keyfile = config_dir.join(
            std::env::var("MQTT_CLIENT_KEYFILE").unwrap_or_else(|_| "certs/client.key".to_string()),
        );
        let username = std::env::var("MQTT_USERNAME")
            .unwrap_or_else(|_| "placenet".to_string());
        let password = std::env::var("MQTT_PASSWORD")
            .unwrap_or_else(|_| "changeme".to_string());

        Self { client_id, host, port, tls_enabled, cafile, certfile, keyfile, username, password }
    }
}

pub struct MqttBrokerageConfig {
    pub port: u16,
    pub client_id: String,
    pub config_file: PathBuf,
    pub password_file: PathBuf,
    pub username: String,
    pub password: String,
    pub tls_enabled: bool,
    pub mqtts_port: u16,
    /// Path to the CA certificate (used by Mosquitto for client verification if needed).
    pub cafile: PathBuf,
    /// Path to the broker's TLS certificate.
    pub certfile: PathBuf,
    /// Path to the broker's TLS private key.
    pub keyfile: PathBuf,
    /// Explicit IP SANs for the broker TLS cert. If empty, all non-loopback
    /// local IPs are auto-detected at startup.
    pub san_ips: Vec<std::net::IpAddr>,
    /// Additional DNS hostname SANs for the broker TLS cert. "localhost" is
    /// always included regardless of this list.
    pub san_hostnames: Vec<String>,
}

impl MqttBrokerageConfig {
    fn from_env(config_dir: &PathBuf) -> Self {
        let port: u16 = std::env::var("MQTT_PORT")
            .unwrap_or_else(|_| "1883".to_string())
            .parse()
            .unwrap_or(1883);
        let mqtts_port: u16 = std::env::var("MQTTS_PORT")
            .unwrap_or_else(|_| "8883".to_string())
            .parse()
            .unwrap_or(8883);
        let client_id = std::env::var("MQTT_CLIENT_ID")
            .unwrap_or_else(|_| "hamlet".to_string());
        let username = std::env::var("MQTT_USERNAME")
            .unwrap_or_else(|_| "placenet".to_string());
        let password = std::env::var("MQTT_PASSWORD")
            .unwrap_or_else(|_| "changeme".to_string());
        let tls_enabled = std::env::var("MQTT_TLS_ENABLED")
            .unwrap_or_else(|_| "true".to_string())
            .trim()
            .eq_ignore_ascii_case("true");
        let cafile = config_dir.join(
            std::env::var("MQTT_CAFILE").unwrap_or_else(|_| "certs/ca.crt".to_string()),
        );
        let certfile = config_dir.join(
            std::env::var("MQTT_CERTFILE").unwrap_or_else(|_| "certs/broker.crt".to_string()),
        );
        let keyfile = config_dir.join(
            std::env::var("MQTT_KEYFILE").unwrap_or_else(|_| "certs/broker.key".to_string()),
        );
        let san_ips: Vec<std::net::IpAddr> = std::env::var("BROKER_SAN_IPS")
            .unwrap_or_default()
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        let san_hostnames: Vec<String> = std::env::var("BROKER_SAN_HOSTNAMES")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty() && s != "localhost")
            .collect();
        let config_file = config_dir.join("mosquitto.conf");
        let password_file = config_dir.join("passwd");
        Self {
            port,
            mqtts_port,
            client_id,
            config_file,
            password_file,
            username,
            password,
            tls_enabled,
            cafile,
            certfile,
            keyfile,
            san_ips,
            san_hostnames,
        }
    }

    /// Generate the mosquitto.conf file at `self.config_file`.
    ///
    /// Authentication is always required. When `tls_enabled` is true a TLS
    /// listener is emitted on `mqtts_port` and the plain listener is omitted
    /// to force all traffic through the encrypted channel.
    pub async fn write_config(&self) -> Result<(), String> {
        let config_dir = self.config_file.parent()
            .ok_or_else(|| "config_file has no parent directory".to_string())?;

        tokio::fs::create_dir_all(config_dir)
            .await
            .map_err(|e| format!("Failed to create config dir: {}", e))?;

        let mut config = String::from("# Auto-generated by hamlet\n");
        config.push_str("persistence false\n");
        config.push_str("log_dest stderr\n\n");

        if self.tls_enabled {
            // Plain listener is omitted when TLS is enabled to force all
            // traffic through the encrypted channel.
            // require_certificate true means the client cert IS the credential —
            // no password_file needed. use_identity_as_username maps the cert CN
            // to the MQTT username so ACL rules can reference it.
            config.push_str(&format!(
                "listener {}\n\
                 protocol mqtt\n\
                 cafile {}\n\
                 certfile {}\n\
                 keyfile {}\n\
                 tls_version tlsv1.2\n\
                 require_certificate true\n\
                 use_identity_as_username true\n",
                self.mqtts_port,
                self.cafile.display(),
                self.certfile.display(),
                self.keyfile.display(),
            ));
        } else {
            config.push_str(&format!(
                "listener {}\n\
                 allow_anonymous false\n\
                 password_file {}\n",
                self.port,
                self.password_file.display(),
            ));
        }

        tokio::fs::write(&self.config_file, &config)
            .await
            .map_err(|e| format!("Failed to write mosquitto.conf: {}", e))?;

        info!("Wrote mosquitto config to {}", self.config_file.display());
        Ok(())
    }
}

pub struct HttpConfig {
    pub host: String,
    pub port: u16,
    pub tls_enabled: bool,
    /// Port of the local upstream process to proxy traffic to.
    pub upstream_port: u16,
}

impl HttpConfig {
    fn from_env() -> Self {
        let host = std::env::var("HTTP_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
        let port: u16 = std::env::var("HTTP_PORT")
            .unwrap_or_else(|_| "8080".to_string())
            .parse()
            .unwrap_or(8080);
        let tls_enabled = std::env::var("HTTP_TLS_ENABLED")
            .unwrap_or_else(|_| "true".to_string())
            .trim()
            .eq_ignore_ascii_case("true");
        let upstream_port: u16 = std::env::var("HTTP_UPSTREAM_PORT")
            .unwrap_or_else(|_| "3000".to_string())
            .parse()
            .unwrap_or(3000);
        Self { host, port, tls_enabled, upstream_port }
    }
}

// /// Configuration for connecting to a peer hamlet server.
// pub struct PeerConfig {
//     /// Base URL of the peer server, e.g. "http://192.168.1.42:8080"
//     pub peer_url: Option<String>,
// }

// impl PeerConfig {
//     fn from_env() -> Self {
//         let peer_url = std::env::var("PEER_URL").ok();
//         Self { peer_url }
//     }
// }

/// Configuration for the beacon management service.
pub struct BeaconManagementConfig {
    /// When true, a random jitter delay is inserted between beacon broadcasts.
    /// Intended for testing only — prevents packet collisions when multiple
    /// beacons are both transmitting and receiving simultaneously.
    pub use_jitter: bool,
    /// How often (in seconds) the home node publishes its URL to beacon topics.
    pub broadcast_interval: u16,
    /// How long (in seconds) each broadcast key remains the active outbound key
    /// before being rotated. A new key is generated and distributed to beacons
    /// at the start of the next broadcast cycle after this interval elapses.
    /// Default: 3600 (1 hour).
    pub key_rotation_interval: u32,
    /// How long (in seconds) a retired broadcast key is kept in the database
    /// after rotation. Must be greater than `key_rotation_interval` so that
    /// beacons that miss a rotation broadcast can still verify incoming messages
    /// with the old key. Default: 7200 (2 hours).
    pub key_expiry: u32,
}

impl BeaconManagementConfig {
    fn from_env() -> Self {
        let use_jitter = std::env::var("USE_NETWORK_JITTER")
            .unwrap_or_else(|_| "false".to_string())
            .trim()
            .eq_ignore_ascii_case("true");
        let broadcast_interval = std::env::var("BROADCAST_INTERVAL")
            .unwrap_or_else(|_| "60".to_string())
            .trim()
            .parse::<u16>()
            .unwrap_or(60);
        let key_rotation_interval = std::env::var("KEY_ROTATION_INTERVAL")
            .unwrap_or_else(|_| "3600".to_string())
            .trim()
            .parse::<u32>()
            .unwrap_or(3600);
        let key_expiry = std::env::var("KEY_EXPIRY")
            .unwrap_or_else(|_| "7200".to_string())
            .trim()
            .parse::<u32>()
            .unwrap_or(7200);

        Self { use_jitter, broadcast_interval, key_rotation_interval, key_expiry }
    }
}

/// Configuration for cloud gateway registration.
pub struct GatewayRegistrationConfig {
    /// This server's URL, used as its identity when registering with the cloud
    /// gateway and when enriching forwarded registration messages.
    /// e.g. "https://home-a.example.local:8080"
    /// Does not need to be internet-routable; it is used as an opaque ID.
    pub server_url: String,

    /// URL of the cloud gateway WebSocket endpoint.
    /// e.g. "ws://gateway.example.com:9000"
    /// Must be reachable from the open internet.
    pub gateway_url: Option<String>,
}

impl GatewayRegistrationConfig {
    fn from_env() -> Self {
        let server_url = std::env::var("PLACENET_SERVER_URL")
            .unwrap_or_else(|_| "http://localhost:8080".to_string());
        let gateway_url = std::env::var("PLACENET_GATEWAY_URL").ok();
        Self { server_url, gateway_url }
    }
}

pub struct Config {
    pub mqtt_brokerage: MqttBrokerageConfig,
    pub mqtt_client: MqttClientConfig,
    pub http: HttpConfig,
    // pub peer: PeerConfig,
    pub gateway_registration: GatewayRegistrationConfig,
    pub beacon_management: BeaconManagementConfig,
    pub config_dir: PathBuf,
}

impl Config {
    /// Load configuration from environment variables, falling back to defaults.
    pub fn from_env() -> Self {
        let config_dir_raw = PathBuf::from(
            std::env::var("PLACENET_CONFIG_DIR").unwrap_or_else(|_| "config".to_string()),
        );
        let config_dir = if config_dir_raw.is_absolute() {
            config_dir_raw
        } else {
            std::env::current_dir()
                .expect("cannot determine current directory")
                .join(config_dir_raw)
        };
        let mqtt_brokerage = MqttBrokerageConfig::from_env(&config_dir);
        let mqtt_client = MqttClientConfig::from_env(&config_dir);
        let http = HttpConfig::from_env();
        // let peer = PeerConfig::from_env();
        let gateway_registration = GatewayRegistrationConfig::from_env();
        let beacon_management = BeaconManagementConfig::from_env();
        Self { mqtt_brokerage, mqtt_client, http, gateway_registration, beacon_management, config_dir }
    }
}
