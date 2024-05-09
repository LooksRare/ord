use anyhow::{anyhow, Context, Result};
use lapin::{
  options::{BasicPublishOptions, BasicQosOptions},
  BasicProperties, Channel,
};
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio::time::sleep;

use crate::index::event::Event;
use crate::indexer::rmq_con::setup_rabbitmq_connection;
use crate::settings::Settings;
use crate::shutdown_process;

async fn rabbit_qos_setup(channel: Channel) -> Result<Channel, anyhow::Error> {
  channel
    .basic_qos(2, BasicQosOptions::default())
    .await
    .context("failed to set basic qos")?;

  Ok(channel)
}

pub struct EventPublisher {
  pub(crate) sender: mpsc::Sender<Event>,
}

impl EventPublisher {
  pub fn run(settings: &Settings) -> Result<Self> {
    let addr = settings
      .rabbitmq_addr()
      .context("rabbitmq amqp credentials and url must be defined")?
      .to_owned();

    let exchange = settings
      .rabbitmq_exchange()
      .context("rabbitmq exchange path must be defined")?
      .to_owned();

    let (tx, rx) = mpsc::channel::<Event>(1);

    std::thread::spawn(move || {
      Runtime::new().expect("runtime is setup").block_on(async {
        match EventPublisher::consume_channel(addr, exchange, rx).await {
          Ok(_) => log::info!("Channel closed."),
          Err(e) => {
            log::error!("Fatal error publishing to RMQ, exiting {}", e);
            shutdown_process();
          }
        }
      })
    });

    Ok(EventPublisher { sender: tx })
  }

  /// Publishes an event to the message exchange.
  /// On failures it will retry up to `n` times with exponential backoff.
  /// After `n` attempts, the retry loop will break and the main loop continues.
  ///
  /// TODO: Consider how to handle failure without skipping.
  async fn consume_channel(
    addr: String,
    exchange: String,
    mut rx: mpsc::Receiver<Event>,
  ) -> Result<()> {
    let channel = setup_rabbitmq_connection(&addr).await?;
    let mut channel = rabbit_qos_setup(channel).await?;

    while let Some(event) = rx.recv().await {
      let message = serde_json::to_vec(&event)?;
      let routing_key = EventPublisher::type_name(&event);

      let mut backoff_delay = Duration::from_secs(1);
      let mut max_attempts = 8;

      let publish_retried = loop {
        match EventPublisher::publish_message(&channel, &exchange, routing_key, &message).await {
          Ok(value) => break Ok(value),
          Err(e) => {
            if max_attempts == 0 {
              break Err(e);
            }

            max_attempts -= 1;

            log::error!(
              "Error: {e}. Retries left: {max_attempts}. Retrying in {}s.",
              backoff_delay.as_secs()
            );

            sleep(backoff_delay).await;

            channel = rabbit_qos_setup(setup_rabbitmq_connection(&addr).await?)
              .await
              .inspect_err(|e| log::error!("error reconnecting rmq: {e}"))
              .unwrap_or(channel);

            backoff_delay *= 2;
          }
        }
      };

      publish_retried?
    }

    Ok(())
  }

  async fn publish_message(
    channel: &Channel,
    exchange: &str,
    routing_key: &str,
    message: &[u8],
  ) -> Result<()> {
    let publish = channel
      .basic_publish(
        exchange,
        routing_key,
        BasicPublishOptions::default(),
        message,
        BasicProperties::default(),
      )
      .await?
      .await?;

    if !publish.is_ack() {
      return Err(anyhow!("message was not acknowledged"));
    }

    Ok(())
  }

  fn type_name(event: &Event) -> &'static str {
    match event {
      Event::InscriptionCreated { .. } => "InscriptionCreated",
      Event::InscriptionTransferred { .. } => "InscriptionTransferred",
      Event::RuneBurned { .. } => "RuneBurned",
      Event::RuneEtched { .. } => "RuneEtched",
      Event::RuneMinted { .. } => "RuneMinted",
      Event::RuneTransferred { .. } => "RuneTransferred",
      Event::BlockCommitted { .. } => "BlockCommitted",
    }
  }
}
