use crate::config::AppConfig;
use crate::nonsense::Nonsense;
use crossbeam_channel::{Receiver, select};
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, error_span};

pub struct KafkaSink {
    config: Arc<AppConfig>,
    producer: FutureProducer,
}

impl KafkaSink {
    pub fn new(config: Arc<AppConfig>) -> anyhow::Result<Self> {
        let future_producer = future_producer(config.as_ref())?;
        Ok(Self {
            config,
            producer: future_producer,
        })
    }

    pub async fn dispatch_loop(
        &self,
        rx: Receiver<Nonsense>,
        shutdown_rx: Receiver<()>,
    ) -> anyhow::Result<()> {
        let mut buffer: Vec<Nonsense> = Vec::new();

        loop {
            select! {
                recv(shutdown_rx) -> _ => {
                    tracing::info!("Shutdown signal received. Flushing remaining messages.");
                    return Ok(());
                }

                recv(rx) -> msg => {
                    if let Ok(message) = msg {
                        buffer.push(message);
                    } else {
                        return Ok(()); // sender hung up
                    }
                }

                default(Duration::from_millis(self.config.dispatch.flush_interval_ms)) => {
                    self.config.dispatch.batch_size;
                }
            }

            if buffer.len() >= self.config.dispatch.batch_size {
                let len = self.config.dispatch.batch_size;
                self.flush_n(&mut buffer, len).await?;
            }
        }
    }

    async fn flush_n(&self, buffer: &mut Vec<Nonsense>, n: usize) -> anyhow::Result<usize> {
        let count = n.min(buffer.len());
        let to_send = buffer.drain(..count).collect::<Vec<_>>();
        self.send_batch(to_send).await?;
        Ok(count)
    }

    async fn send_batch(&self, messages: Vec<Nonsense>) -> anyhow::Result<()> {
        for message in messages {
            let payload = message.to_json()?;
            let key = message.id.to_string();

            let record = FutureRecord::to(&self.config.producer.topic)
                .payload(&payload)
                .key(&key);

            self.producer
                .send(
                    record,
                    Duration::from_millis(self.config.producer.timeout_ms),
                )
                .await
                .map_err(|(e, _)| {
                    error_span!(
                        "kafka_send_error",
                        error = %e,
                        topic = %self.config.producer.topic,
                        thread_id = ?std::thread::current().id(),
                    )
                    .in_scope(|| {
                        error!("Failed to send message to Kafka");
                    });
                    anyhow::Error::msg(e.to_string())
                })?;
        }
        Ok(())
    }
}

fn future_producer(config: &AppConfig) -> anyhow::Result<FutureProducer> {
    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", &config.producer.brokers)
        .set("compression.type", &config.producer.compression)
        .set("acks", &config.producer.acks)
        .set(
            "message.timeout.ms",
            &config.producer.timeout_ms.to_string(),
        )
        .set(
            "queue.buffering.max.ms",
            &config.producer.buffering_max_ms.to_string(),
        );

    client_config.create().map_err(|e| e.into())
}
