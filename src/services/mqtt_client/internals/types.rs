use bytes::Bytes;
use rumqttc::QoS;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Clone)]
pub struct MqttMessage {
    pub topic: String,
    pub payload: Bytes,
}

pub type MqttMessageSender = mpsc::Sender<MqttMessage>;
pub type MqttMessageReceiver = mpsc::Receiver<MqttMessage>;
pub type MqttOutboundSender = mpsc::Sender<MqttMessage>;
pub type MqttOutboundReceiver = mpsc::Receiver<MqttMessage>;

#[derive(Debug, Clone)]
pub struct TopicSubscription {
    pub topic: &'static str,
    pub qos: QoS,
}

pub enum MqttCommand {
    Subscribe {
        topic: String,
        qos: QoS,
        reply: oneshot::Sender<Result<(), String>>,
    },
    Unsubscribe {
        topic: String,
        reply: oneshot::Sender<Result<(), String>>,
    },
    Publish {
        topic: String,
        qos: QoS,
        payload: Vec<u8>,
        reply: oneshot::Sender<Result<(), String>>,
    },
    Shutdown,
}
