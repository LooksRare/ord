use anyhow::Context;
use clap::Parser;
use futures::StreamExt;
use lapin::{Connection, ConnectionProperties, options::*, types::FieldTable};
use tokio::runtime::Runtime;

use crate::index::event::Event;
use crate::ord_db_client::OrdDbClient;
use crate::settings::Settings;
use crate::subcommand::SubcommandResult;

#[derive(Debug, Parser, Clone)]
pub struct EventConsumer {
  #[arg(long, help = "RMQ queue to consume index events.")]
  pub(crate) rabbitmq_queue: Option<String>,
  #[arg(long, help = "DB url to persist inscriptions.")]
  pub(crate) database_url: Option<String>,
}

impl EventConsumer {
  pub fn run(self, settings: &Settings) -> SubcommandResult {
    Runtime::new()?.block_on(async {
      let addr = settings
        .rabbitmq_addr()
        .context("rabbitmq amqp credentials and url must be defined")?;

      let queue = self
        .rabbitmq_queue
        .as_deref()
        .context("rabbitmq queue path must be defined")?;

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

      let database_url = self
        .database_url
        .as_deref()
        .context("db url must be defined")?;
      let ord_db_client = OrdDbClient::run(database_url).await?;

      log::info!("Starting to consume messages from {}", queue);
      while let Some(delivery) = consumer.next().await {
        match delivery {
          Ok(delivery) => {
            let event: Result<Event, _> = serde_json::from_slice(&delivery.data);
            match event {
              Ok(event) => {
                self
                  .handle_event(&ord_db_client, &event)
                  .await
                  .expect("confirms rmq msg processed");
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

  async fn handle_event(
    &self,
    ord_db_client: &OrdDbClient,
    event: &Event,
  ) -> Result<(), anyhow::Error> {
    match event {
      Event::InscriptionCreated {
        block_height,
        charms,
        inscription_id,
        location,
        parent_inscription_ids,
        sequence_number,
      } => {
        ord_db_client.save_inscription_created(block_height, inscription_id, location).await?;
        Ok(())
      }
      Event::InscriptionTransferred {
        block_height,
        inscription_id,
        new_location,
        old_location,
        sequence_number,
      } => {
        ord_db_client.save_inscription_transferred(block_height, inscription_id, new_location, old_location).await?;
        Ok(())
      }
      _ => Ok(()),
    }
  }

}
