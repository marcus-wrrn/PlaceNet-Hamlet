/// Device handshake: sign CSR, return cert + brokerage info.
pub(super) const HEADER_INIT: &str = "x-placenet-init";

/// Client cert registration: sign CSR, return cert + CA cert.
pub(super) const HEADER_REGISTER: &str = "x-placenet-register";

/// Health check: returns 200 OK.
pub(super) const HEADER_HEALTH: &str = "x-placenet-health";
