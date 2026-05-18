---
name: service-management
description: Defines service architecture, guidelines and instructions for adding new services
---
# Services

A service is a PlaceNet module operating on a separate thread or process from the main tasks operating in the `App` struct.

All services are controlled/orchestrated via the `Supervisor` in [src/supervisor.rs]. The supervisor is responsible for starting, stopping, restarting, and retrieving status updates from each service.

All services must have a `ServiceId` variant in [src/services/mod.rs] to be registered with the supervisor.

## Service Components

Every service has three layers:

1. **Management layer** (`manager.rs`)
   - `register_onto()` — constructs the service and calls `supervisor.register()`
   - `start_*()` — sends a `Start` command via `SupervisorHandle`
   - Returns channel handles the caller needs to interact with the service

2. **Service layer** (`<name>_service.rs`)
   - Primary struct implementing `ManagedService`
   - `start()` — spawns internal tasks, stores shutdown handles, returns a PID (`0` for async tasks, real PID for process-managed)
   - `stop()` — signals shutdown
   - `is_running()` — checks running state

3. **Tasks** (`tasks.rs`)
   - Named `spawn_*` functions that call `tokio::spawn` and return nothing
   - Each function owns the resources it needs (channels, clients, shutdown receivers) — they are moved in, not borrowed
   - Called from `<name>_service.rs`'s `ManagedService::start()` implementation
   - Keep individual task functions focused on a single concern (e.g. one for the event loop, one for command dispatch)

4. **Internals** (`internals/` directory, optional)
   - `internals/types.rs` — message structs, command enums, and `mpsc`/`oneshot` channel type aliases
   - `internals/mod.rs` — re-exports everything from `types.rs` with `pub use types::*`
   - Nothing in internals is service-specific logic; it is shared data definitions only

---

## Self-Managed vs Process-Managed Services

Services fall into two categories based on how they track running state:

### Self-managed (async tasks)

Running state is tracked via `shutdown_tx: Option<oneshot::Sender<()>>`. `stop()` fires the sender; `is_running()` checks `shutdown_tx.is_some()`. Returns `Ok(0)` from `start()`. Each spawned task receives its own dedicated `oneshot::Receiver<()>` — never share a single shutdown signal across multiple tasks.

### Process-managed (child process)

Running state is tracked via `child: Option<Child>`. `stop()` sends SIGTERM and waits with a timeout before falling back to `kill()`. `is_running()` uses `child.try_wait()` to detect process exit. Returns the real PID from `start()`. No `tasks.rs` needed.

```rust
async fn is_running(&mut self) -> bool {
    if let Some(child) = &mut self.child {
        match child.try_wait() {
            Ok(Some(_)) => { self.child = None; false }
            Ok(None) => true,
            Err(_) => false,
        }
    } else {
        false
    }
}
```

---

## Step-by-Step: Creating a New Service

### 1. Add a `ServiceId` variant

Open [src/services/mod.rs] and add a variant to the `ServiceId` enum:

```rust
pub enum ServiceId {
    Gateway,
    Mosquitto,
    MqttClient,
    CloudGateway,
    MyNewService,   // ← add this
}
```

### 2. Create the module directory

```
src/services/my_new_service/
├── mod.rs                    ← re-exports only
├── my_new_service_service.rs ← service struct + ManagedService impl
├── manager.rs                ← register_onto() and start_my_new_service()
├── tasks.rs                  ← (optional) tokio::spawn wrappers for long-running sub-tasks
└── internals/                ← (optional) message types and channel aliases
    ├── mod.rs
    └── types.rs
```

Register the module in [src/services/mod.rs]:

```rust
pub mod my_new_service;
```

### 3. Define message/channel types (if needed)

In `internals`, define any command/message enums and type aliases for your channels:

```rust
use tokio::sync::oneshot;

pub enum MyServiceCommand {
    DoSomething { reply: oneshot::Sender<Result<(), String>> },
}

pub type MyCommandSender   = tokio::sync::mpsc::Sender<MyServiceCommand>;
pub type MyCommandReceiver = tokio::sync::mpsc::Receiver<MyServiceCommand>;
```

### 4. Implement the service struct and `ManagedService`

In `my_new_service_service.rs`:

```rust
use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};
use crate::supervisor::ManagedService;

pub struct MyNewService {
    cmd_tx: mpsc::Sender<MyServiceCommand>,     // kept for handle()
    cmd_rx: Option<mpsc::Receiver<MyServiceCommand>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    cmd_shutdown_tx: Option<oneshot::Sender<()>>,
}

impl MyNewService {
    pub fn new(cmd_tx: mpsc::Sender<MyServiceCommand>, cmd_rx: mpsc::Receiver<MyServiceCommand>) -> Self {
        Self { cmd_tx, cmd_rx: Some(cmd_rx), shutdown_tx: None, cmd_shutdown_tx: None }
    }

    pub fn handle(&self) -> MyServiceHandle {
        MyServiceHandle { tx: self.cmd_tx.clone() }
    }
}

#[async_trait]
impl ManagedService for MyNewService {
    async fn start(&mut self) -> Result<u32, String> {
        if self.shutdown_tx.is_some() {
            return Err("MyNewService is already running".to_string());
        }

        let cmd_rx = self.cmd_rx.take()
            .ok_or("command receiver already consumed")?;

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let (cmd_shutdown_tx, cmd_shutdown_rx) = oneshot::channel::<()>();
        self.shutdown_tx = Some(shutdown_tx);
        self.cmd_shutdown_tx = Some(cmd_shutdown_tx);

        tasks::spawn_event_loop_task(shutdown_rx);
        tasks::spawn_command_task(cmd_rx, cmd_shutdown_rx);

        Ok(0)
    }

    async fn stop(&mut self) -> Result<(), String> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
            if let Some(cmd_tx) = self.cmd_shutdown_tx.take() {
                let _ = cmd_tx.send(());
            }
            Ok(())
        } else {
            Err("MyNewService is not running".to_string())
        }
    }

    async fn is_running(&mut self) -> bool {
        self.shutdown_tx.is_some()
    }
}
```

When a service consumes multiple `Option<T>` fields on `start()`, extract a private `take_channels()` helper to keep `start()` readable. See `MqttClientService` for an example.

### 5. Write `tasks.rs` (if needed)

Each task receives its own `shutdown_rx` so `stop()` can cleanly terminate all tasks independently:

```rust
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info};
use super::internals::{MyServiceCommand, MyMessageSender};

pub fn spawn_event_loop_task(
    msg_tx: MyMessageSender,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    info!("MyNewService event loop shutting down");
                    break;
                }
                // handle events, forward via msg_tx
            }
        }
    });
}

pub fn spawn_command_task(
    mut cmd_rx: mpsc::Receiver<MyServiceCommand>,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    info!("MyNewService command task shutting down");
                    break;
                }
                cmd = cmd_rx.recv() => match cmd {
                    Some(MyServiceCommand::DoSomething { reply }) => {
                        let _ = reply.send(Ok(()));
                    }
                    None => break,
                }
            }
        }
    });
}
```

### 6. Implement the handle

```rust
#[derive(Clone)]
pub struct MyServiceHandle {
    tx: mpsc::Sender<MyServiceCommand>,
}

impl MyServiceHandle {
    pub async fn do_something(&self) -> Result<(), String> {
        let (reply, rx) = oneshot::channel();
        self.tx.send(MyServiceCommand::DoSomething { reply })
            .await
            .map_err(|_| "service channel closed".to_string())?;
        rx.await.map_err(|_| "service dropped reply".to_string())?
    }
}
```

Add a `handle()` method on the service struct (see Step 4) and call it from `register_onto()`.

#### Ready signals

If the caller needs to wait until the service is fully initialised (e.g. connected to a broker), add a `connected_tx: Option<oneshot::Sender<()>>` field. Fire it from the event loop task once ready, and return `connected_rx: oneshot::Receiver<()>` from `register_onto()`. Currently used by `MqttClientService` — consider whether a new service with a similar async startup phase should follow the same pattern.

### 7. Write `manager.rs`

```rust
use tokio::sync::mpsc;
use tracing::{info, error, warn};
use crate::supervisor::{Supervisor, SupervisorHandle};
use crate::services::ServiceId;
use super::{MyNewService, MyServiceHandle, MyServiceCommand};

const CMD_CAPACITY: usize = 64;

pub struct MyServiceHandles {
    pub handle: MyServiceHandle,
}

pub fn register_onto(supervisor: &mut Supervisor, available: bool) -> MyServiceHandles {
    let (cmd_tx, cmd_rx) = mpsc::channel::<MyServiceCommand>(CMD_CAPACITY);
    let service = MyNewService::new(cmd_tx, cmd_rx);
    let handle = service.handle();

    supervisor.register(ServiceId::MyNewService, Box::new(service), available);

    MyServiceHandles { handle }
}

pub async fn start_my_new_service(available: bool, supervisor_handle: &SupervisorHandle) {
    if available {
        match supervisor_handle.start_service(ServiceId::MyNewService).await {
            Ok(()) => info!("MyNewService started"),
            Err(e) => error!("Failed to start MyNewService: {}", e),
        }
    } else {
        warn!("MyNewService dependency not available — skipping start");
    }
}
```

### 8. Export from `mod.rs`

`mod.rs` is re-exports only — no implementation lives here:

```rust
pub mod internals;
pub mod manager;
mod my_new_service_service;
mod tasks;   // private: visible within this module and its submodules, not exported
pub use my_new_service_service::{MyNewService, MyServiceHandle};
```

### 9. Wire into `App::initialize()`

In [src/app.rs](../../../src/app.rs):

```rust
use crate::services::my_new_service::manager::{register_onto as register_my_service, start_my_new_service};

// inside App::initialize(), before supervisor.spawn():
let my_handles = register_my_service(&mut supervisor, available_flag);

// after supervisor.spawn():
start_my_new_service(available_flag, &supervisor_handle).await;
```

Store any handles you need on `App`:

```rust
pub struct App {
    my_service_handle: MyServiceHandle,
    // ...
}
```

---

## Availability vs. Always-on Services

| Pattern | When to use | How |
|---|---|---|
| `available = true` always | Service has no external dependency | Pass `true` to `register_onto()` |
| Capability-gated | Requires a system binary | Run `detect_capabilities()`, pass the result |
| Config-gated | Requires an env var to be set | Check the config value, pass `bool` |

When `available = false`, the supervisor marks the service `Unavailable` and `start_service()` returns an error — no code change needed.

---

## Supervisor State Machine

```
Unavailable ──(no transitions)
Stopped     ──start──► Starting ──ok──► Running { pid }
                                └─err─► Failed { reason }
Running     ──stop──► Stopped
Failed      ──start──► Starting   (retry is allowed)
```

---

## Checklist

- [ ] `ServiceId` variant added in `src/services/mod.rs`
- [ ] Module declared in `src/services/mod.rs`
- [ ] Service struct and `ManagedService` impl in `<name>_service.rs` (not `mod.rs`)
- [ ] Channels stored as `Option<T>`, consumed via `.take()` in `start()`
- [ ] Running state: `shutdown_tx: Option<oneshot::Sender<()>>` (self-managed) or `child: Option<Child>` (process-managed)
- [ ] Each spawned task receives its own `oneshot::Receiver<()>` for shutdown; `stop()` fires all of them
- [ ] Long-running spawns extracted into `spawn_*` functions in `tasks.rs`
- [ ] `internals/types.rs` holds all message types and channel aliases; `internals/mod.rs` re-exports them
- [ ] `register_onto()` calls `service.handle()` to obtain the handle
- [ ] `start_*()` in `manager.rs`
- [ ] Wired into `App::initialize()` in `src/app.rs`
- [ ] Directory tree in `CLAUDE.md` updated
