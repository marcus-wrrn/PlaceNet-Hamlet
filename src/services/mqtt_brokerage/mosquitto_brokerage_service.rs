use std::sync::Arc;
use async_trait::async_trait;
use tokio::process::{Child, Command};
use tokio::io::AsyncBufReadExt;
use tokio::sync::RwLock;
use tracing::{debug, info, error};

use crate::config::MqttBrokerageConfig;
use crate::infra::ca::CaService;
use crate::supervisor::ManagedService;

fn get_local_ips() -> Vec<std::net::IpAddr> {
    let mut ips = Vec::new();

    unsafe {
        let mut ifaddrs: *mut libc::ifaddrs = std::ptr::null_mut();
        if libc::getifaddrs(&mut ifaddrs) == 0 {
            let mut ifa = ifaddrs;
            while !ifa.is_null() {
                let addr = (*ifa).ifa_addr;
                if !addr.is_null() {
                    match (*addr).sa_family as libc::c_int {
                        libc::AF_INET => {
                            let sin = addr as *const libc::sockaddr_in;
                            let ip = std::net::Ipv4Addr::from(u32::from_be((*sin).sin_addr.s_addr));
                            if !ip.is_loopback() {
                                ips.push(std::net::IpAddr::V4(ip));
                            }
                        }
                        libc::AF_INET6 => {
                            let sin6 = addr as *const libc::sockaddr_in6;
                            let ip = std::net::Ipv6Addr::from((*sin6).sin6_addr.s6_addr);
                            if !ip.is_loopback() {
                                ips.push(std::net::IpAddr::V6(ip));
                            }
                        }
                        _ => {}
                    }
                }
                ifa = (*ifa).ifa_next;
            }
            libc::freeifaddrs(ifaddrs);
        }
    }

    ips.push(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
    ips
}

/// Write the CA cert and a fresh broker TLS cert/key to the paths in `cfg`.
pub async fn provision_mqtt_broker_cert(ca: &CaService, cfg: &MqttBrokerageConfig) -> Result<(), String> {
    if let Some(parent) = cfg.certfile.parent() {
        tokio::fs::create_dir_all(parent).await
            .map_err(|e| format!("Failed to create broker cert dir: {e}"))?;
    }

    let ca_cert_pem = ca.ca_cert_pem().await?;
    tokio::fs::write(&cfg.cafile, &ca_cert_pem).await
        .map_err(|e| format!("Failed to write broker CA cert: {e}"))?;

    let san_ips: Vec<std::net::IpAddr> = if cfg.san_ips.is_empty() {
        get_local_ips()
    } else {
        cfg.san_ips.clone()
    };
    let mut san_hostnames: Vec<&str> = vec!["localhost"];
    san_hostnames.extend(cfg.san_hostnames.iter().map(|s| s.as_str()));
    info!(
        san_ips = ?san_ips,
        san_dns = ?san_hostnames,
        cafile = %cfg.cafile.display(),
        certfile = %cfg.certfile.display(),
        "Provisioning broker TLS cert",
    );
    let (cert_pem, key_pem) = ca.generate_broker_cert(&san_ips, &san_hostnames).await?;
    debug!(broker_cert_pem = %cert_pem, "Broker cert PEM");
    tokio::fs::write(&cfg.certfile, &cert_pem).await
        .map_err(|e| format!("Failed to write broker cert: {e}"))?;
    tokio::fs::write(&cfg.keyfile, &key_pem).await
        .map_err(|e| format!("Failed to write broker key: {e}"))?;
    info!("Broker TLS cert written successfully");

    Ok(())
}

/// A cloneable handle for interacting with the MQTT brokerage at runtime.
#[derive(Clone)]
pub struct MqttBrokerageHandle {
    config: Arc<RwLock<MqttBrokerageConfig>>,
}

impl MqttBrokerageHandle {
    /// Write the signed device certificate to `{certs_dir}/{hostname}.crt` to record the
    /// device's MQTT identity. Mosquitto with `require_certificate true` accepts any CA-signed
    /// cert; this file is a local record used for future dynamic-security provisioning.
    pub async fn register_cert_identity(&self, hostname: &str, cert_pem: &str) -> Result<(), String> {
        let certs_dir = {
            let config = self.config.read().await;
            config.cafile
                .parent()
                .ok_or_else(|| "cafile has no parent directory".to_string())?
                .to_path_buf()
        };
        let cert_path = certs_dir.join(format!("{}.crt", hostname));
        tokio::fs::write(&cert_path, cert_pem).await
            .map_err(|e| format!("Failed to write device cert '{}': {}", cert_path.display(), e))?;
        info!(hostname, cert_path = %cert_path.display(), "Device MQTT cert identity registered");
        Ok(())
    }
}

/// Manages a Mosquitto MQTT broker child process
pub struct MosquittoBrokerageService {
    binary_path: String,
    password_binary: Option<String>,
    config: Arc<RwLock<MqttBrokerageConfig>>,
    child: Option<Child>,
}

impl MosquittoBrokerageService {
    pub fn new(
        binary_path: String,
        password_binary: Option<String>,
        config: Arc<RwLock<MqttBrokerageConfig>>,
    ) -> Self {
        Self {
            binary_path,
            password_binary,
            config,
            child: None,
        }
    }

    pub fn handle(&self) -> MqttBrokerageHandle {
        MqttBrokerageHandle { config: Arc::clone(&self.config) }
    }

    /// Add or update a user's password using mosquitto_passwd
    pub async fn set_password(&self, username: &str, password: &str) -> Result<(), String> {
        let passwd_bin = self.password_binary.as_deref()
            .ok_or_else(|| "mosquitto_passwd is not installed".to_string())?;

        let password_file = self.config.read().await.password_file.clone();
        if !password_file.exists() {
            let output = Command::new(passwd_bin)
                .args(["-c", "-b"])
                .arg(&password_file)
                .arg(username)
                .arg(password)
                .output()
                .await
                .map_err(|e| format!("Failed to run mosquitto_passwd: {}", e))?;

            if !output.status.success() {
                return Err(format!(
                    "mosquitto_passwd failed: {}",
                    String::from_utf8_lossy(&output.stderr)
                ));
            }
        } else {
            let output = Command::new(passwd_bin)
                .arg("-b")
                .arg(&password_file)
                .arg(username)
                .arg(password)
                .output()
                .await
                .map_err(|e| format!("Failed to run mosquitto_passwd: {}", e))?;

            if !output.status.success() {
                return Err(format!(
                    "mosquitto_passwd failed: {}",
                    String::from_utf8_lossy(&output.stderr)
                ));
            }
        }

        info!("Set password for MQTT user: {}", username);
        Ok(())
    }

    /// Delete a user from the password file
    pub async fn delete_user(&self, username: &str) -> Result<(), String> {
        let passwd_bin = self.password_binary.as_deref()
            .ok_or_else(|| "mosquitto_passwd is not installed".to_string())?;

        let password_file = self.config.read().await.password_file.clone();

        if !password_file.exists() {
            return Err("Password file does not exist".to_string());
        }

        let output = Command::new(passwd_bin)
            .arg("-D")
            .arg(&password_file)
            .arg(username)
            .output()
            .await
            .map_err(|e| format!("Failed to run mosquitto_passwd: {}", e))?;

        if !output.status.success() {
            return Err(format!(
                "mosquitto_passwd failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        info!("Deleted MQTT user: {}", username);
        Ok(())
    }
}

#[async_trait]
impl ManagedService for MosquittoBrokerageService {
    async fn start(&mut self) -> Result<u32, String> {
        if self.child.is_some() {
            return Err("Mosquitto is already running".to_string());
        }

        let config_file = self.config.read().await.config_file.clone();
        if !config_file.exists() {
            return Err(format!(
                "Config file not found at {}. Call write_config() first.",
                config_file.display()
            ));
        }

        let mut cmd = Command::new(&self.binary_path);
        cmd.args(["-c", &config_file.to_string_lossy()])
            .stderr(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .kill_on_drop(true);

        #[cfg(target_os = "linux")]
        unsafe {
            cmd.pre_exec(|| {
                libc::prctl(
                    libc::PR_SET_PDEATHSIG,
                    libc::SIGTERM as libc::c_ulong,
                    0, 0, 0,
                );
                Ok(())
            });
        }

        let mut child = cmd.spawn()
            .map_err(|e| format!("Failed to spawn mosquitto: {}", e))?;

        let pid = child.id().ok_or("Failed to get mosquitto PID")?;

        if let Some(stderr) = child.stderr.take() {
            let reader = tokio::io::BufReader::new(stderr);
            tokio::spawn(async move {
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    info!(target: "mosquitto", "{}", line);
                }
            });
        }

        self.child = Some(child);
        Ok(pid)
    }

    async fn stop(&mut self) -> Result<(), String> {
        if let Some(mut child) = self.child.take() {
            let pid = child.id();
            if let Some(pid) = pid {
                unsafe {
                    libc::kill(pid as i32, libc::SIGTERM);
                }
            }

            match tokio::time::timeout(
                std::time::Duration::from_secs(5),
                child.wait(),
            ).await {
                Ok(Ok(status)) => {
                    info!("Mosquitto exited with status: {}", status);
                }
                Ok(Err(e)) => {
                    error!("Error waiting for mosquitto: {}", e);
                }
                Err(_) => {
                    info!("Mosquitto didn't exit gracefully, killing");
                    let _ = child.kill().await;
                }
            }

            Ok(())
        } else {
            Err("Mosquitto is not running".to_string())
        }
    }

    async fn is_running(&mut self) -> bool {
        if let Some(child) = &mut self.child {
            match child.try_wait() {
                Ok(Some(_)) => {
                    self.child = None;
                    false
                }
                Ok(None) => true,
                Err(_) => false,
            }
        } else {
            false
        }
    }
}
