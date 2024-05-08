use anyhow::Context;
use clap::Parser;
use futures::StreamExt;
use lapin::{message::Delivery, options::*, types::FieldTable};
use tokio::runtime::Runtime;

use crate::index::event::Event;
use crate::indexer::db_client::DbClient;
use crate::indexer::rmq_con::{generate_consumer_tag, setup_rabbitmq_connection};
use crate::settings::Settings;
use crate::subcommand::SubcommandResult;

#[derive(Debug, Parser)]
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
      let db = DbClient::new(database_url, 2).await?;

      let tag = generate_consumer_tag("lr-ord-evts");
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

      log::info!("started event consumer {tag} for {queue_name}");

      loop {
        if let Some(msg) = consumer.next().await {
          match msg {
            Ok(d) => EventConsumer::handle_delivery(d, &db).await?,
            Err(e) => log::error!("error consuming message: {}", e),
          }
        }
      }
    })
  }

  async fn handle_delivery(delivery: Delivery, db: &DbClient) -> Result<(), lapin::Error> {
    let reject = BasicRejectOptions { requeue: false };
    let event: Result<Event, _> = serde_json::from_slice(&delivery.data);

    match event {
      Ok(event) => {
        if let Err(err) = EventConsumer::process_event(event, db).await {
          log::error!("failed to process event: {}", err);
          delivery.reject(reject).await
        } else {
          delivery.ack(BasicAckOptions::default()).await
        }
      }

      Err(e) => {
        log::error!("failed to deserialize event, rejecting: {e}");
        delivery.reject(reject).await
      }
    }
  }

  async fn process_event(event: Event, db: &DbClient) -> Result<(), sqlx::Error> {
    match &event {
      Event::InscriptionCreated {
        block_height,
        inscription_id,
        location,
        ..
      } => {
        db.save_inscription_created(
          //
          block_height,
          inscription_id,
          location,
        )
        .await
      }

      Event::InscriptionTransferred {
        block_height,
        inscription_id,
        new_location,
        old_location,
        ..
      } => {
        db.save_inscription_transferred(
          //
          block_height,
          inscription_id,
          new_location,
          old_location,
        )
        .await
      }

      _ => Ok(log::warn!("skipped unhandled event type {:?}", event)),
    }
  }
}
