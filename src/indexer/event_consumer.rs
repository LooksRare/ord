use anyhow::Context;
use clap::Parser;
use futures::StreamExt;
use lapin::{options::*, types::FieldTable};
use tokio::runtime::Runtime;

use crate::index::event::Event;
use crate::indexer::db_client::DbClient;
use crate::indexer::rmq_con::{generate_consumer_tag, setup_rabbitmq_connection};
use crate::settings::Settings;
use crate::subcommand::SubcommandResult;

#[derive(Debug, Parser, Clone)]
pub struct EventConsumer {
  #[arg(long, help = "RMQ queue to consume inscription events.")]
  pub(crate) inscriptions_queue: Option<String>,
  #[arg(long, help = "DB url to persist inscriptions.")]
  pub(crate) database_url: Option<String>,
}

impl EventConsumer {
  pub fn run(self, settings: &Settings) -> SubcommandResult {
    Runtime::new()?.block_on(async {
      let database_url = self.database_url.context("db url is required")?;
      let db_client = DbClient::new(database_url, 2).await?;

      let tag = generate_consumer_tag("lr-ord");
      let addr = settings.rabbitmq_addr().context("rmq url is required")?;
      let queue_name = self.inscriptions_queue.context("rmq queue is required")?;
      let channel = setup_rabbitmq_connection(addr).await?;

      let mut consumer = channel
        .basic_consume(
          &queue_name,
          tag.as_str(),
          BasicConsumeOptions::default(),
          FieldTable::default(),
        )
        .await?;

      loop {
        if let Some(message) = consumer.next().await {
          match message {
            Ok(delivery) => EventConsumer::handle_delivery(delivery, &db_client).await?,
            Err(e) => log::error!("error consuming message: {}", e),
          }
        }
      }
    })
  }

  async fn handle_delivery(
    delivery: lapin::message::Delivery,
    db_client: &DbClient,
  ) -> Result<(), lapin::Error> {
    let event: Result<Event, _> = serde_json::from_slice(&delivery.data);

    match event {
      Ok(event) => {
        if let Err(err) = EventConsumer::process_event(event, db_client).await {
          log::error!("failed to process event: {}", err);
          delivery.reject(BasicRejectOptions { requeue: false }).await
        } else {
          delivery.ack(BasicAckOptions::default()).await
        }
      }

      Err(e) => {
        log::error!("failed to deserialize event, rejecting: {}", e);
        delivery.reject(BasicRejectOptions { requeue: false }).await
      }
    }
  }

  async fn process_event(event: Event, db_client: &DbClient) -> Result<(), sqlx::Error> {
    match &event {
      Event::InscriptionCreated {
        block_height,
        inscription_id,
        location,
        ..
      } => {
        db_client
          .save_inscription_created(block_height, inscription_id, location)
          .await
      }

      Event::InscriptionTransferred {
        block_height,
        inscription_id,
        new_location,
        old_location,
        ..
      } => {
        db_client
          .save_inscription_transferred(block_height, inscription_id, new_location, old_location)
          .await
      }

      _ => {
        log::warn!("skipped unhandled event type {:?}", event);
        Ok(())
      }
    }
  }
}
