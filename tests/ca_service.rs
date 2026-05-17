mod common;

use hamlet::infra::ca::CaService;

async fn initialized_service() -> CaService {
    let pool = common::setup_db().await;
    let svc = CaService::new(pool);
    svc.init().await.expect("init failed");
    svc
}

#[tokio::test]
async fn init_succeeds_and_ca_cert_is_available() {
    let svc = initialized_service().await;
    let pem = svc.ca_cert_pem().await.expect("ca_cert_pem");
    assert!(pem.contains("CERTIFICATE"));
}

#[tokio::test]
async fn ca_cert_pem_fails_before_init() {
    let pool = common::setup_db().await;
    let svc = CaService::new(pool);
    let result = svc.ca_cert_pem().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn sign_csr_returns_cert_pem() {
    let svc = initialized_service().await;
    let csr_pem = common::generate_test_csr();
    let cert = svc.sign_csr("device-1", &csr_pem).await.expect("sign_csr");
    assert!(cert.contains("CERTIFICATE"));
}

#[tokio::test]
async fn sign_csr_rejects_invalid_pem() {
    let svc = initialized_service().await;
    let result = svc.sign_csr("device-1", "garbage").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn get_cert_returns_none_for_unknown_device() {
    let svc = initialized_service().await;
    let result = svc.get_cert("unknown-device").await.expect("get_cert");
    assert!(result.is_none());
}

#[tokio::test]
async fn get_cert_returns_pem_after_signing() {
    let svc = initialized_service().await;
    let csr_pem = common::generate_test_csr();
    svc.sign_csr("device-2", &csr_pem).await.expect("sign");

    let cert = svc.get_cert("device-2").await.expect("get_cert");
    assert!(cert.is_some());
    assert!(cert.unwrap().contains("CERTIFICATE"));
}

#[tokio::test]
async fn sign_csr_upserts_on_second_call() {
    let svc = initialized_service().await;
    let csr1 = common::generate_test_csr();
    let csr2 = common::generate_test_csr();

    let cert1 = svc.sign_csr("device-3", &csr1).await.expect("first sign");
    let cert2 = svc.sign_csr("device-3", &csr2).await.expect("second sign");

    // Should succeed and update the record
    assert_ne!(cert1, cert2);
    let stored = svc.get_cert("device-3").await.expect("get").unwrap();
    assert_eq!(stored, cert2);
}

#[tokio::test]
async fn list_devices_returns_all_signed_certs() {
    let svc = initialized_service().await;

    svc.sign_csr("dev-a", &common::generate_test_csr()).await.expect("a");
    svc.sign_csr("dev-b", &common::generate_test_csr()).await.expect("b");

    let devices = svc.list_devices().await.expect("list");
    assert_eq!(devices.len(), 2);
    let ids: Vec<&str> = devices.iter().map(|d| d.device_id.as_str()).collect();
    assert!(ids.contains(&"dev-a"));
    assert!(ids.contains(&"dev-b"));
}

#[tokio::test]
async fn revoke_marks_device_as_revoked() {
    let svc = initialized_service().await;
    svc.sign_csr("dev-rev", &common::generate_test_csr()).await.expect("sign");

    assert!(!svc.is_revoked("dev-rev").await.expect("is_revoked before"));
    svc.revoke("dev-rev").await.expect("revoke");
    assert!(svc.is_revoked("dev-rev").await.expect("is_revoked after"));
}

#[tokio::test]
async fn revoke_fails_for_unknown_device() {
    let svc = initialized_service().await;
    let result = svc.revoke("nonexistent").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn is_revoked_returns_false_for_unknown_device() {
    let svc = initialized_service().await;
    let result = svc.is_revoked("nobody").await.expect("is_revoked");
    assert!(!result);
}

#[tokio::test]
async fn list_devices_reflects_revocation_status() {
    let svc = initialized_service().await;
    svc.sign_csr("dev-x", &common::generate_test_csr()).await.expect("sign");
    svc.revoke("dev-x").await.expect("revoke");

    let devices = svc.list_devices().await.expect("list");
    let record = devices.iter().find(|d| d.device_id == "dev-x").unwrap();
    assert!(record.revoked);
}
