use anyhow::{Context, Result};
use lapin::{options::BasicPublishOptions, BasicProperties, Channel};
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio::time::sleep;

use crate::index::event::Event;
use crate::indexer::rmq_con::setup_rabbitmq_connection;
use crate::settings::Settings;
use crate::shutdown_process;

pub struct EventPublisher {
  pub(crate) sender: mpsc::Sender<Event>,
}

impl EventPublisher {
  pub fn run(settings: &Settings) -> Result<Self, anyhow::Error> {
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

  async fn consume_channel(
    addr: String,
    exchange: String,
    mut rx: mpsc::Receiver<Event>,
  ) -> Result<()> {
    let mut channel = setup_rabbitmq_connection(&addr).await?;

    while let Some(event) = rx.recv().await {
      let message = serde_json::to_vec(&event)?;
      let routing_key = EventPublisher::type_name(&event);

      let mut attempts = 3;
      let mut backoff_delay = Duration::from_secs(1);
      while attempts > 0 {
        let result =
          EventPublisher::publish_message(&channel, &exchange, routing_key, &message).await;
        if let Err(e) = result {
          attempts -= 1;
          log::error!("Error publishing message: {}. Attempting to recreate the RMQ channel. Retries left: {}. Retrying in {} seconds.", e, attempts, backoff_delay.as_secs());
          sleep(backoff_delay).await;
          backoff_delay *= 2;
          channel = setup_rabbitmq_connection(&addr).await?;
        } else {
          break;
        }
      }

      if attempts == 0 {
        return Err(anyhow::Error::new(std::io::Error::new(
          std::io::ErrorKind::Other,
          "Failed to publish message after retries",
        )));
      }
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
      return Err(anyhow::Error::new(std::io::Error::new(
        std::io::ErrorKind::Other,
        "Message was not acknowledged",
      )));
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
