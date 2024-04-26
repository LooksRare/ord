use anyhow::Context;
use clap::Parser;
use futures::StreamExt;
use lapin::{options::*, types::FieldTable, Connection, ConnectionProperties};
use tokio::runtime::Runtime;

use crate::index::event::Event;
use crate::settings::Settings;
use crate::subcommand::SubcommandResult;

#[derive(Debug, Parser, Clone)]
pub struct EventConsumer {
  #[arg(long, help = "RMQ queue to consume index events.")]
  pub(crate) rabbitmq_queue: Option<String>,
}

impl EventConsumer {
  pub fn run(self, settings: &Settings) -> SubcommandResult {
    Runtime::new()?.block_on(async {
      let addr = settings
        .rabbitmq_addr()
        .context("rabbitmq amqp credentials and url must be defined")?;

      let queue = self
        .rabbitmq_queue
        .context("rabbitmq queue path must be defined")?
        .to_owned();

      let conn = Connection::connect(&addr, ConnectionProperties::default())
        .await
        .expect("connects to rabbitmq ok");

      let channel = conn
        .create_channel()
        .await
        .expect("creates rmq connection channel");

      channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .expect("enable msg confirms");

      let mut consumer = channel
        .basic_consume(
          &queue,
          "lr-ord", //TODO pod name
          BasicConsumeOptions::default(),
          FieldTable::default(),
        )
        .await
        .expect("creates rmq consumer");

      log::info!("Starting to consume messages from {}", queue);
      while let Some(delivery) = consumer.next().await {
        match delivery {
          Ok(delivery) => {
            let event: Result<Event, _> = serde_json::from_slice(&delivery.data);
            match event {
              Ok(event) => {
                log::info!("Received event: {:?}", event);
                delivery
                  .ack(BasicAckOptions::default())
                  .await
                  .expect("confirms rmq msg received");
              }
              Err(e) => {
                log::error!("Error deserializing event: {}", e);
                delivery
                  .reject(BasicRejectOptions { requeue: false })
                  .await?;
              }
            }
          }
          Err(e) => {
            log::error!("Error receiving delivery: {}", e);
          }
        }
      }

      Ok(None)
    })
  }
}
