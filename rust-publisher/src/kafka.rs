use crate::config::AppConfig;
use crate::nonsense::Nonsense;
use crossbeam_channel::Receiver;
use futures::stream::{FuturesUnordered, StreamExt};
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct KafkaSink {
    config: Arc<AppConfig>,
    producer: FutureProducer,
}

impl KafkaSink {
    pub fn new(config: Arc<AppConfig>) -> anyhow::Result<Self> {
        let producer = future_producer(config.as_ref())?;
        Ok(Self { config, producer })
    }

    pub async fn dispatch_loop(&self, rx: Receiver<Nonsense>) -> anyhow::Result<()> {
        let mut buffer = Vec::with_capacity(self.config.dispatch.batch_size);
        let mut last_flush = Instant::now();
        let timeout_ms = self.config.dispatch.flush_interval_ms;

        loop {
            match rx.recv_timeout(Duration::from_millis(timeout_ms)) {
                Ok(msg) => {
                    buffer.push(msg);

                    if buffer.len() >= self.config.dispatch.batch_size {
                        let to_flush = buffer.len().min(self.config.dispatch.batch_size);
                        self.flush_n(&mut buffer, to_flush).await?;
                        last_flush = Instant::now();
                    }
                }

                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    if !buffer.is_empty() {
                        let to_flush = buffer.len().min(self.config.dispatch.batch_size);
                        self.flush_n(&mut buffer, to_flush).await?;
                        last_flush = Instant::now();
                    }
                }

                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    if !buffer.is_empty() {
                        let remaining = buffer.len();
                        self.flush_n(&mut buffer, remaining).await?;
                    }
                    return Ok(());
                }
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
        let timeout = Duration::from_millis(self.config.producer.timeout_ms);
        let topic_base = self.config.producer.topic.clone();
        let mut in_flight = FuturesUnordered::new();

        for msg in messages {
            let payload = msg.to_json()?;
            let key = msg.id.to_string();
            let topic = topic_base.clone();
            let producer = self.producer.clone();

            in_flight.push(async move {
                let record = FutureRecord::to(&topic).payload(&payload).key(&key);

                producer
                    .send(record, timeout)
                    .await
                    .map_err(|(e, _)| anyhow::anyhow!(e))
            });
        }

        while let Some(result) = in_flight.next().await {
            if let Err(e) = result {
                tracing::error!("Kafka send failed: {}", e);
            }
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
    client_config.create().map_err(Into::into)
}
