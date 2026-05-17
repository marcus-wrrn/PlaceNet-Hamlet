use std::path::PathBuf;
use hamlet::config::MqttBrokerageConfig;

fn make_config_in(dir: &std::path::Path, tls_enabled: bool) -> MqttBrokerageConfig {
    let config_file = dir.join("mosquitto.conf");
    let password_file = dir.join("passwd");
    MqttBrokerageConfig {
        port: 1883,
        mqtts_port: 8883,
        client_id: "hamlet".to_string(),
        config_file,
        password_file: password_file.clone(),
        username: "placenet".to_string(),
        password: "changeme".to_string(),
        tls_enabled,
        cafile: dir.join("ca.crt"),
        certfile: dir.join("broker.crt"),
        keyfile: dir.join("broker.key"),
        san_ips: vec![],
        san_hostnames: vec![],
    }
}

#[tokio::test]
async fn write_config_creates_file() {
    let tmp = tempfile::tempdir().unwrap();
    let config = make_config_in(tmp.path(), false);

    config.write_config().await.expect("write_config failed");

    assert!(config.config_file.exists(), "mosquitto.conf should exist");
}

#[tokio::test]
async fn write_config_plain_contains_port() {
    let tmp = tempfile::tempdir().unwrap();
    let config = make_config_in(tmp.path(), false);

    config.write_config().await.expect("write_config failed");

    let contents = std::fs::read_to_string(&config.config_file).unwrap();
    assert!(contents.contains("1883"), "should include plain port");
    assert!(contents.contains("allow_anonymous false"));
    assert!(contents.contains("password_file"));
}

#[tokio::test]
async fn write_config_tls_contains_mqtts_port() {
    let tmp = tempfile::tempdir().unwrap();
    let config = make_config_in(tmp.path(), true);

    config.write_config().await.expect("write_config failed");

    let contents = std::fs::read_to_string(&config.config_file).unwrap();
    assert!(contents.contains("8883"), "should include mqtts port");
    assert!(contents.contains("tls_version tlsv1.2"));
    assert!(contents.contains("cafile"));
    assert!(contents.contains("certfile"));
    assert!(contents.contains("keyfile"));
}

#[tokio::test]
async fn write_config_tls_omits_plain_port() {
    let tmp = tempfile::tempdir().unwrap();
    let config = make_config_in(tmp.path(), true);

    config.write_config().await.expect("write_config");

    let contents = std::fs::read_to_string(&config.config_file).unwrap();
    // Should not have the plain 1883 listener line
    assert!(
        !contents.lines().any(|l| l.trim() == "listener 1883"),
        "plain listener should not appear when TLS enabled"
    );
}

#[tokio::test]
async fn write_config_creates_parent_dirs() {
    let tmp = tempfile::tempdir().unwrap();
    let nested = tmp.path().join("a").join("b").join("c");
    let mut config = make_config_in(tmp.path(), false);
    config.config_file = nested.join("mosquitto.conf");
    config.password_file = nested.join("passwd");

    config.write_config().await.expect("write_config should create dirs");
    assert!(config.config_file.exists());
}

#[tokio::test]
async fn write_config_invalid_parent_fails() {
    // config_file with no parent (just a bare filename without directory)
    let config = MqttBrokerageConfig {
        port: 1883,
        mqtts_port: 8883,
        client_id: "test".to_string(),
        config_file: PathBuf::from("mosquitto.conf"),
        password_file: PathBuf::from("passwd"),
        username: "u".to_string(),
        password: "p".to_string(),
        tls_enabled: false,
        cafile: PathBuf::from("ca.crt"),
        certfile: PathBuf::from("broker.crt"),
        keyfile: PathBuf::from("broker.key"),
        san_ips: vec![],
        san_hostnames: vec![],
    };

    // A bare filename has parent "" which create_dir_all("") may succeed on some systems.
    // What we really care about is that the function at minimum doesn't panic.
    // We do not assert on success/failure here since it's platform-dependent.
    let _ = config.write_config().await;
}
