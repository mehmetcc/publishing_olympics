use crate::config::AppConfig;
use crate::generator::start_generators;
use crate::kafka::KafkaSink;
use crate::nonsense::Nonsense;
use crossbeam_channel::bounded;
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::{fmt, prelude::*};

mod config;
mod generator;
mod kafka;
mod nonsense;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env, set up logging & Rayon
    dotenv::dotenv().ok();
    init_logger();
    configure_pool()?;

    // Load config and optional throttle
    let config = Arc::new(AppConfig::new()?);
    let throttle = config
        .concurrency
        .throttling_enabled
        .then(|| Duration::from_millis(config.concurrency.throttling_ms));

    // Channel for generated Nonsense
    let (tx, rx) = bounded::<Nonsense>(10_000);

    // Start your generators on a clone of tx
    start_generators(available_cores(), tx.clone(), throttle);
    tracing::info!("Started {} generator threads", available_cores());

    // Create the Kafka sink and its dispatch future
    let sink = KafkaSink::new(config.clone())?;
    let mut dispatch_fut = sink.dispatch_loop(rx);
    tokio::pin!(dispatch_fut);

    // Phase 1: wait for either Ctrl+C or dispatch_loop to finish naturally
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("SIGINT received — shutting down generators");
            // Closing the channel signals dispatch_loop to flush+exit
            drop(tx);
        }
        res = &mut dispatch_fut => {
            // dispatch_loop ended on its own (e.g. generators all hung up)
            res?;
            return Ok(());
        }
    }

    // Phase 2: after Ctrl+C, let dispatch_loop finish flushing
    dispatch_fut.await?;

    Ok(())
}

fn init_logger() {
    tracing_subscriber::registry()
        .with(fmt::layer().json())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();
}

fn configure_pool() -> anyhow::Result<()> {
    rayon::ThreadPoolBuilder::new()
        .num_threads(available_cores())
        .build_global()
        .map_err(Into::into)
}

fn available_cores() -> usize {
    num_cpus::get_physical().saturating_sub(1).max(1)
}
