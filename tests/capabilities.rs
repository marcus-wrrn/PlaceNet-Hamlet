use std::collections::HashMap;
use hamlet::services::{BinaryInfo, ServiceCapabilities};

fn caps_with(name: &str, path: &str) -> ServiceCapabilities {
    let mut binaries = HashMap::new();
    binaries.insert(
        name.to_string(),
        Some(BinaryInfo { path: path.to_string(), version: Some("1.0".to_string()) }),
    );
    ServiceCapabilities { binaries }
}

fn caps_missing(name: &str) -> ServiceCapabilities {
    let mut binaries = HashMap::new();
    binaries.insert(name.to_string(), None);
    ServiceCapabilities { binaries }
}

#[test]
fn is_available_returns_true_when_present() {
    let caps = caps_with("mosquitto", "/usr/bin/mosquitto");
    assert!(caps.is_available("mosquitto"));
}

#[test]
fn is_available_returns_false_when_none() {
    let caps = caps_missing("mosquitto");
    assert!(!caps.is_available("mosquitto"));
}

#[test]
fn is_available_returns_false_when_key_missing() {
    let caps = ServiceCapabilities { binaries: HashMap::new() };
    assert!(!caps.is_available("mosquitto"));
}

#[test]
fn binary_path_returns_path_when_present() {
    let caps = caps_with("mosquitto", "/usr/bin/mosquitto");
    assert_eq!(caps.binary_path("mosquitto"), Some("/usr/bin/mosquitto"));
}

#[test]
fn binary_path_returns_none_when_missing() {
    let caps = caps_missing("mosquitto");
    assert_eq!(caps.binary_path("mosquitto"), None);
}

#[test]
fn binary_path_returns_none_when_key_absent() {
    let caps = ServiceCapabilities { binaries: HashMap::new() };
    assert_eq!(caps.binary_path("mosquitto"), None);
}

#[test]
fn multiple_binaries_tracked_independently() {
    let mut binaries = HashMap::new();
    binaries.insert(
        "mosquitto".to_string(),
        Some(BinaryInfo { path: "/usr/bin/mosquitto".to_string(), version: None }),
    );
    binaries.insert("mosquitto_passwd".to_string(), None);
    let caps = ServiceCapabilities { binaries };

    assert!(caps.is_available("mosquitto"));
    assert!(!caps.is_available("mosquitto_passwd"));
}
