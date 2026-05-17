mod common;

use hamlet::infra::ca::operations::{load_or_generate_ca, sign_csr};

#[tokio::test]
async fn generates_new_ca_when_db_empty() {
    let pool = common::setup_db().await;
    let state = load_or_generate_ca(&pool).await.expect("should generate CA");
    assert_eq!(state.subject_cn, "PlaceNet Root CA");
}

#[tokio::test]
async fn loads_existing_ca_on_second_call() {
    let pool = common::setup_db().await;

    let first = load_or_generate_ca(&pool).await.expect("first call");
    let second = load_or_generate_ca(&pool).await.expect("second call");

    // Both should have the same CN and use the same private key.
    assert_eq!(first.subject_cn, second.subject_cn);
    assert_eq!(first.key_pair.serialize_pem(), second.key_pair.serialize_pem());
}

#[tokio::test]
async fn sign_csr_returns_valid_pem() {
    let pool = common::setup_db().await;
    let ca = load_or_generate_ca(&pool).await.expect("CA");

    let csr_pem = common::generate_test_csr();
    let (cert_pem, issued_at, expires_at) = sign_csr(&ca, &csr_pem).expect("signing failed");

    assert!(cert_pem.contains("CERTIFICATE"), "cert PEM should contain CERTIFICATE header");
    assert!(expires_at > issued_at, "expires_at should be after issued_at");
    // ~1 year gap
    let one_year = 365 * 24 * 60 * 60_i64;
    assert!((expires_at - issued_at - one_year).abs() < 5, "expiry should be ~1 year");
}

#[tokio::test]
async fn sign_csr_rejects_invalid_pem() {
    let pool = common::setup_db().await;
    let ca = load_or_generate_ca(&pool).await.expect("CA");

    let result = sign_csr(&ca, "not a valid CSR");
    assert!(result.is_err(), "should fail with invalid CSR");
}

#[tokio::test]
async fn issued_at_is_recent_unix_timestamp() {
    let pool = common::setup_db().await;
    let ca = load_or_generate_ca(&pool).await.expect("CA");
    let csr_pem = common::generate_test_csr();

    let (_, issued_at, _) = sign_csr(&ca, &csr_pem).expect("sign");

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    assert!((now - issued_at).abs() < 5, "issued_at should be close to now");
}
