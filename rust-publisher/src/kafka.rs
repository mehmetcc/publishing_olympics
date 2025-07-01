use crate::config::AppConfig;
use crate::nonsense::Nonsense;
use crossbeam_channel::{Receiver, select, tick};
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
        let producer = future_producer(config.as_ref())?;
        Ok(Self { config, producer })
    }

    pub async fn dispatch_loop(
        &self,
        rx: Receiver<Nonsense>,
        shutdown_rx: Receiver<()>,
    ) -> anyhow::Result<()> {
        let mut buffer = Vec::with_capacity(self.config.dispatch.batch_size);
        let ticker = tick(Duration::from_millis(
            self.config.dispatch.flush_interval_ms,
        ));

        loop {
            select! {
                recv(shutdown_rx) -> _ => break,
                recv(rx) -> msg => match msg {
                    Ok(m) => buffer.push(m),
                    Err(_) => break, // upstream hung up
                },

                recv(ticker) -> _ => {
                    if !buffer.is_empty() {
                        let to_flush = buffer.len().min(self.config.dispatch.batch_size);
                        self.flush_n(&mut buffer, to_flush).await?;
                    }
                }
            }

            if buffer.len() >= self.config.dispatch.batch_size {
                self.flush_n(&mut buffer, self.config.dispatch.batch_size)
                    .await?;
            }
        }

        let remaining = buffer.len();
        if remaining > 0 {
            self.flush_n(&mut buffer, remaining).await?;
        }

        Ok(())
    }

    async fn flush_n(&self, buffer: &mut Vec<Nonsense>, count: usize) -> anyhow::Result<()> {
        let count = count.min(buffer.len());
        let to_send = buffer.drain(..count).collect::<Vec<_>>();
        self.send_batch(to_send).await
    }

    async fn send_batch(&self, messages: Vec<Nonsense>) -> anyhow::Result<()> {
        for msg in messages {
            let payload = msg.to_json()?;
            let key = msg.id.to_string();

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
                        error     = %e,
                        topic     = %self.config.producer.topic,
                        thread_id = ?std::thread::current().id(),
                    )
                    .in_scope(|| error!("Failed to send message to Kafka"));
                    anyhow::Error::msg(e.to_string())
                })?;
        }
        Ok(())
    }
}

fn future_producer(config: &AppConfig) -> anyhow::Result<FutureProducer> {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", &config.producer.brokers)
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
    cfg.create().map_err(|e| e.into())
}
