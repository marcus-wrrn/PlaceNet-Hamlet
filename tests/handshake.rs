use std::path::PathBuf;
use hamlet::config::MqttBrokerageConfig;
use hamlet::services::local_gateway::manager::build_brokerage_info;

fn make_config(tls_enabled: bool) -> MqttBrokerageConfig {
    MqttBrokerageConfig {
        port: 1883,
        mqtts_port: 8883,
        client_id: "test".to_string(),
        config_file: PathBuf::from("/tmp/test/mosquitto.conf"),
        password_file: PathBuf::from("/tmp/test/passwd"),
        username: "user".to_string(),
        password: "pass".to_string(),
        tls_enabled,
        cafile: PathBuf::from("/tmp/test/ca.crt"),
        certfile: PathBuf::from("/tmp/test/broker.crt"),
        keyfile: PathBuf::from("/tmp/test/broker.key"),
        san_ips: vec![],
        san_hostnames: vec![],
    }
}

#[test]
fn tls_disabled_uses_plain_port() {
    let config = make_config(false);
    let info = build_brokerage_info(&config, "test-ca-cert".to_string());
    assert_eq!(info.port, 1883);
}

#[test]
fn tls_enabled_uses_mqtts_port() {
    let config = make_config(true);
    let info = build_brokerage_info(&config, "test-ca-cert".to_string());
    assert_eq!(info.port, 8883);
}

#[test]
fn address_is_localhost() {
    let config = make_config(false);
    let info = build_brokerage_info(&config, "test-ca-cert".to_string());
    assert_eq!(info.address, "localhost");
}
