use async_trait::async_trait;
use hamlet::supervisor::{ManagedService, ServiceStatus, Supervisor};
use hamlet::services::ServiceId;

struct MockService {
    running: bool,
    fail_start: bool,
    pid: u32,
}

impl MockService {
    fn new(pid: u32) -> Self {
        Self { running: false, fail_start: false, pid }
    }

    fn failing() -> Self {
        Self { running: false, fail_start: true, pid: 0 }
    }
}

#[async_trait]
impl ManagedService for MockService {
    async fn start(&mut self) -> Result<u32, String> {
        if self.fail_start {
            return Err("mock start failure".to_string());
        }
        self.running = true;
        Ok(self.pid)
    }

    async fn stop(&mut self) -> Result<(), String> {
        self.running = false;
        Ok(())
    }

    async fn is_running(&mut self) -> bool {
        self.running
    }
}

fn make_supervisor_with_service(id: ServiceId, available: bool) -> Supervisor {
    let mut s = Supervisor::new();
    s.register(id, Box::new(MockService::new(1234)), available);
    s
}

#[tokio::test]
async fn start_available_service_succeeds() {
    let s = make_supervisor_with_service(ServiceId::Gateway, true);
    let handle = s.spawn();
    handle.start_service(ServiceId::Gateway).await.expect("start failed");
}

#[tokio::test]
async fn start_unavailable_service_fails() {
    let s = make_supervisor_with_service(ServiceId::Mosquitto, false);
    let handle = s.spawn();
    let result = handle.start_service(ServiceId::Mosquitto).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("not installed"));
}

#[tokio::test]
async fn start_unregistered_service_fails() {
    let s = Supervisor::new();
    let handle = s.spawn();
    let result = handle.start_service(ServiceId::Gateway).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("not registered"));
}

#[tokio::test]
async fn start_already_running_service_fails() {
    let s = make_supervisor_with_service(ServiceId::Gateway, true);
    let handle = s.spawn();
    handle.start_service(ServiceId::Gateway).await.expect("first start");
    let result = handle.start_service(ServiceId::Gateway).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("already running"));
}

#[tokio::test]
async fn stop_running_service_succeeds() {
    let s = make_supervisor_with_service(ServiceId::Gateway, true);
    let handle = s.spawn();
    handle.start_service(ServiceId::Gateway).await.expect("start");
    handle.stop_service(ServiceId::Gateway).await.expect("stop failed");
}

#[tokio::test]
async fn stop_already_stopped_service_fails() {
    let s = make_supervisor_with_service(ServiceId::Gateway, true);
    let handle = s.spawn();
    let result = handle.stop_service(ServiceId::Gateway).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("already stopped"));
}

#[tokio::test]
async fn start_failing_service_transitions_to_failed() {
    let mut s = Supervisor::new();
    s.register(ServiceId::Gateway, Box::new(MockService::failing()), true);
    let handle = s.spawn();

    let result = handle.start_service(ServiceId::Gateway).await;
    assert!(result.is_err());

    let status = handle.get_status().await.expect("status");
    assert!(matches!(status[&ServiceId::Gateway], ServiceStatus::Failed { .. }));
}

#[tokio::test]
async fn restart_running_service_succeeds() {
    let s = make_supervisor_with_service(ServiceId::Gateway, true);
    let handle = s.spawn();
    handle.start_service(ServiceId::Gateway).await.expect("start");
    handle.restart_service(ServiceId::Gateway).await.expect("restart failed");

    let status = handle.get_status().await.expect("status");
    assert!(matches!(status[&ServiceId::Gateway], ServiceStatus::Running { .. }));
}

#[tokio::test]
async fn restart_stopped_service_succeeds() {
    let s = make_supervisor_with_service(ServiceId::Gateway, true);
    let handle = s.spawn();
    handle.restart_service(ServiceId::Gateway).await.expect("restart from stopped");

    let status = handle.get_status().await.expect("status");
    assert!(matches!(status[&ServiceId::Gateway], ServiceStatus::Running { .. }));
}

#[tokio::test]
async fn get_status_reflects_registration() {
    let mut s = Supervisor::new();
    s.register(ServiceId::Gateway, Box::new(MockService::new(1)), true);
    s.register(ServiceId::Mosquitto, Box::new(MockService::new(2)), false);
    let handle = s.spawn();

    let status = handle.get_status().await.expect("status");
    assert_eq!(status[&ServiceId::Gateway], ServiceStatus::Stopped);
    assert_eq!(status[&ServiceId::Mosquitto], ServiceStatus::Unavailable);
}

#[tokio::test]
async fn get_status_running_after_start() {
    let s = make_supervisor_with_service(ServiceId::Gateway, true);
    let handle = s.spawn();
    handle.start_service(ServiceId::Gateway).await.expect("start");

    let status = handle.get_status().await.expect("status");
    assert!(matches!(status[&ServiceId::Gateway], ServiceStatus::Running { pid: 1234 }));
}

#[tokio::test]
async fn multiple_services_independent() {
    let mut s = Supervisor::new();
    s.register(ServiceId::Gateway, Box::new(MockService::new(1)), true);
    s.register(ServiceId::MqttClient, Box::new(MockService::new(2)), true);
    let handle = s.spawn();

    handle.start_service(ServiceId::Gateway).await.expect("start gateway");

    let status = handle.get_status().await.expect("status");
    assert!(matches!(status[&ServiceId::Gateway], ServiceStatus::Running { .. }));
    assert_eq!(status[&ServiceId::MqttClient], ServiceStatus::Stopped);
}
