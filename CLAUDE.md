# PlaceNet Home

A Rust service that acts as a local hub for the PlaceNet network. It manages a Certificate Authority, a Mosquitto MQTT broker, an MQTT client, and an HTTP/HTTPS reverse proxy — all orchestrated by an internal supervisor.

## Directory Structure

```
placenet-home/
├── Cargo.toml
├── Cargo.lock
├── .env
├── migrations/
│   ├── 0001_ca_keys.sql         ← CA root key/cert storage
│   ├── 0002_device_certs.sql    ← Issued device cert records
│   └── 0003_node_identity.sql   ← This node's own MQTT client cert + key
├── static/
│   └── index.html               ← (upstream app content, not served by placenet-home)
├── tests/
│   ├── common/mod.rs            ← Shared test helpers
│   ├── ca_service.rs            ← CaService integration tests
│   ├── ca_operations.rs         ← Root CA generation + CSR signing tests
│   ├── capabilities.rs          ← Binary detection tests
│   ├── config.rs                ← Config parsing tests
│   ├── handshake.rs             ← MqttBrokerageInfo / EnrichedRegistrationMessage tests
│   └── supervisor.rs            ← Supervisor lifecycle tests
└── src/
    ├── main.rs                  ← Startup: installs crypto provider, loads config, spawns AppContext
    ├── lib.rs                   ← Re-exports all public modules
    ├── config.rs                ← All Config structs (loaded from env)
    ├── app.rs                   ← AppContext — wires all services in initialize(); run_beacon_message_loop() processes inbound MQTT messages and connects to advertised gateways
    ├── supervisor.rs            ← Supervisor, ManagedService trait, SupervisorHandle
    ├── infra/
    │   ├── mod.rs
    │   └── ca/
    │       ├── mod.rs           ← re-exports CaService, DeviceCertRecord
    │       ├── ca_service.rs    ← CaService, DeviceCertRecord (sign_csr, get_cert, revoke, ca_cert_pem)
    │       ├── manager.rs       ← register() — creates CaService, runs init()
    │       └── operations.rs    ← load_or_generate_ca, sign_csr (pure crypto logic)
    ├── services/
    │   ├── mod.rs               ← ServiceId enum (Gateway, Mosquitto, MqttClient, CloudGateway)
    │   ├── capabilities.rs      ← detect_capabilities() — checks for system binaries
    │   ├── cloud_gateway/
    │   │   ├── mod.rs           ← re-exports CloudGatewayService, CloudGatewayHandle, connect_to_gateway
    │   │   ├── cloud_gateway_service.rs ← CloudGatewayService, CloudGatewayHandle, ManagedService impl
    │   │   ├── manager.rs       ← register_onto(), start_cloud_gateway()
    │   │   └── messages.rs      ← GatewayMessage enum (Register, Connect, ConnectRequest, Relay, Ack)
    │   ├── local_gateway/
    │   │   ├── mod.rs           ← re-exports GatewayService; brings AppState, BoxError, ProxyBody, constants into scope
    │   │   ├── gateway_service.rs ← GatewayService, AppState, ManagedService impl
    │   │   ├── manager.rs       ← register_onto(), start_gateway()
    │   │   ├── tls.rs           ← build_tls_config() — rustls ServerConfig from CaService
    │   │   ├── handshake.rs     ← DeviceInfo, MqttBrokerageInfo, EnrichedRegistrationMessage, build_brokerage_info()
    │   │   ├── handlers.rs      ← handle_device_init(), handle_client_register()
    │   │   ├── headers.rs       ← HEADER_INIT, HEADER_REGISTER — PlaceNet HTTP header name constants
    │   │   ├── proxy.rs         ← dispatch(), try_forward(), serve_connection(), serve_tls_connection()
    │   │   ├── requests.rs      ← DeviceInitRequest struct + process_request() — parses version, broker_host, device from Request
    │   │   └── response.rs      ← text_response(), json_response() helpers
    │   ├── mqtt_brokerage/
    │   │   ├── mod.rs           ← re-exports MosquittoBrokerageService, MqttBrokerageHandle, provision_broker_cert
    │   │   ├── mosquitto_brokerage_service.rs ← MosquittoBrokerageService, MqttBrokerageHandle (spawns mosquitto process)
    │   │   └── registration.rs  ← register_onto(), start_mosquitto_brokerage()
    │   ├── mqtt_client/
    │   │   ├── mod.rs           ← re-exports MqttClientService, MqttClientHandle, MqttMessage, required_subscriptions, etc.
    │   │   ├── mqtt_client_service.rs ← MqttClientService, MqttClientHandle, MqttMessage, required_subscriptions()
    │   │   ├── tasks.rs         ← spawn_eventloop_task(), spawn_command_task() — tokio task helpers
    │   │   └── manager.rs       ← register_onto(), start_mqtt_client(), MqttHandles
    │   └── peer/
    │       └── mod.rs           ← send_message() — plain HTTP POST to peer placenet-home node
    └── rendering/
        ├── mod.rs
        └── startup_screen.rs
```

## Architecture Overview

### Service Lifecycle Pattern

Every service follows a two-step pattern:
1. **`register_onto(&mut supervisor, ...)`** — constructs the service struct and calls `supervisor.register(id, service, available)`. Sets initial status to `Stopped` or `Unavailable`.
2. **`start_*(broker_available, &supervisor_handle).await`** — sends a `Start` command to the supervisor via `SupervisorHandle`.

Services implement the `ManagedService` trait (`start() → Result<u32, String>`, `stop()`, `is_running()`). The supervisor runs a single-threaded command loop in a `tokio::spawn`'d task, so all service mutations are serialized.

### Gateway (HTTP/HTTPS Reverse Proxy)

The gateway listens on `HTTP_HOST:HTTP_PORT` (default `0.0.0.0:8080`) and:
- Intercepts requests with `X-PlaceNet-Init` header → `handle_device_init()`: signs a device CSR and returns cert + MQTT broker info.
- Intercepts requests with `X-PlaceNet-Register` header → `handle_client_register()`: signs a client CSR, returns cert + CA cert.
- Proxies all other requests to `localhost:HTTP_UPSTREAM_PORT` (default `3000`).

When `HTTP_TLS_ENABLED=true`, the gateway builds a `rustls` `ServerConfig` from the CA-issued server certificate and uses `TlsAcceptor`. When disabled, it serves plain HTTP.

The current PlaceNet protocol version is `"0.0.1"` (checked via the `X-PlaceNet-Init` header value).

### Certificate Authority

`CaService` wraps a SQLite pool and an `Arc<RwLock<Option<CaState>>>`. On `init()`:
1. Runs SQLx migrations.
2. Calls `load_or_generate_ca()` — loads from `ca_keys` table or generates a new root CA via `rcgen`.

`sign_csr()` signs a PEM CSR, upserts the issued cert into `device_certs`, and returns the cert PEM. The CA database URL defaults to `sqlite://placenet_ca.db` (override with `CA_DATABASE_URL`).

### MQTT

- **Brokerage**: Spawns a `mosquitto` child process. Config is written by `MqttBrokerageConfig::write_config()` to `config/mosquitto.conf`. When `MQTT_TLS_ENABLED=true`, only the MQTTS port (default `8883`) is opened; otherwise plain port `1883` is used.
- **Client**: Uses `rumqttc`. On startup, subscribes to the `"registration"` topic (QoS 1). Inbound messages are forwarded through `inbound_rx`. Control is via `MqttClientHandle` (subscribe/unsubscribe/publish with oneshot acknowledgement).
- **Peer forwarding**: When `PEER_URL` is set, inbound registration messages are wrapped in `EnrichedRegistrationMessage` (adds `server_url` and `gateway_url`) and POSTed to the peer node.

### Cloud Gateway Client

`CloudGatewayService` opens a persistent WebSocket to `{PLACENET_GATEWAY_URL}/ws` on startup. It immediately sends a `Register { server_url }` frame to announce this server's identity. The service then loops, forwarding outbound messages from `CloudGatewayHandle` and dispatching inbound frames (`ConnectRequest`, `Relay`, `Ack`). On disconnect it reconnects with exponential backoff (2 s → 60 s). The service is skipped (registered as `Unavailable`) when `PLACENET_GATEWAY_URL` is unset.

### Supervisor

`Supervisor` owns all services as `HashMap<ServiceId, Box<dyn ManagedService + Send>>`. Commands (`Start`, `Stop`, `Restart`, `Status`) are sent over an `mpsc` channel to the supervisor task. State transitions: `Unavailable → (no change)`, `Stopped → Starting → Running{pid}` or `Failed{reason}`.

## Configuration (Environment Variables)

All config is loaded via `Config::from_env()`. Relevant variables:

| Variable | Default | Description |
|---|---|---|
| `PLACENET_CONFIG_DIR` | `config` | Directory for mosquitto.conf, passwd, certs |
| `CA_DATABASE_URL` | `sqlite://placenet_ca.db` | SQLite DB for CA keys and device certs |
| `HTTP_HOST` | `0.0.0.0` | Gateway bind address |
| `HTTP_PORT` | `8080` | Gateway listen port |
| `HTTP_TLS_ENABLED` | `true` | Enable TLS on the gateway |
| `HTTP_UPSTREAM_PORT` | `3000` | Upstream app port to proxy to |
| `MQTT_PORT` | `1883` | Plain MQTT port |
| `MQTTS_PORT` | `8883` | TLS MQTT port |
| `MQTT_TLS_ENABLED` | `false` | Enable TLS on the MQTT broker/client |
| `MQTT_CLIENT_ID` | `placenet-home` | MQTT client identifier |
| `MQTT_USERNAME` | `placenet` | MQTT auth username |
| `MQTT_PASSWORD` | `changeme` | MQTT auth password |
| `MQTT_CAFILE` | `certs/ca.crt` (relative to config dir) | CA cert for MQTT TLS |
| `MQTT_CERTFILE` | `certs/broker.crt` | Broker TLS cert |
| `MQTT_KEYFILE` | `certs/broker.key` | Broker TLS key |
| `MQTT_CLIENT_CERTFILE` | `certs/client.crt` | Client cert for home node's MQTT mutual TLS |
| `MQTT_CLIENT_KEYFILE` | `certs/client.key` | Client key for home node's MQTT mutual TLS |
| `PEER_URL` | _(unset)_ | Base URL of peer placenet-home node |
| `PLACENET_SERVER_URL` | `http://localhost:8080` | This server's identity URL (opaque ID sent to gateway) |
| `PLACENET_GATEWAY_URL` | _(unset)_ | Cloud gateway WebSocket URL — enables `CloudGatewayService` when set |

## Key Conventions

- **`manager.rs` files are registration-only** — they contain just the `register_onto()` and `start_*()` functions. All service logic lives in `mod.rs` or other focused modules.
- **`mod.rs` files are implementation** — the main service struct and its `ManagedService` impl live here.
- **Error handling**: Services return `Result<_, String>` with human-readable messages. No `anyhow` or `thiserror` — errors are surfaced as owned strings.
- **Async runtime**: Tokio with all features. Services spawn their own tasks internally; the supervisor task serializes lifecycle commands.
- **Tracing**: Use `tracing::{info, warn, error}` macros throughout. Structured fields via `%field` (Display) or `?field` (Debug) in the macros.
- **No HTTP framework**: The gateway uses raw `hyper` 1.x with `hyper-util` and `http-body-util`. No axum/actix.
- **SQLx**: Used with `sqlite` feature and compile-time checked queries. Migrations live in `./migrations/` and are embedded via `sqlx::migrate!("./migrations")`.

## Development Workflows

### Build and run
```bash
cargo build
cargo run
```

### Run tests
```bash
cargo test
```

Tests are integration-style (in `tests/`), using real SQLite in-memory or `tempfile` DBs. No mocking of the database layer.

### Adding a new service
1. Create `src/services/<name>/mod.rs` (service struct + `ManagedService` impl) and `src/services/<name>/manager.rs` (registration functions).
2. Add a variant to `ServiceId` in `src/services/mod.rs`.
3. Register and start the service in `main.rs`.
4. Update the directory tree in this file.

### Adding a new HTTP endpoint
Add a handler in `src/services/local_gateway/handlers.rs` and dispatch it from `src/services/local_gateway/proxy.rs::dispatch()` by checking for the appropriate header or path.

## Developer Instructions
- Always update the directory tree in CLAUDE.md after adding or removing files.
- See PLACENET.md for full project vision, protocol design, and architecture overview.
