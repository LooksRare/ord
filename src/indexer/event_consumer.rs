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

  /// Handle the persistence of incoming queue "event" messages.
  ///
  /// Re-enqueues the message up to `max_delivery` times for processing failures.
  /// Bubbles up the `lapin::Error` only if the ack/nack/reject itself fails.
  async fn handle_delivery(delivery: Delivery, db: &DbClient) -> Result<(), lapin::Error> {
    let max_delivery = 3;
    let reject = BasicRejectOptions { requeue: false };
    let requeue = BasicNackOptions {
      multiple: false,
      requeue: true,
    };

    let delivery_count = delivery
      .properties
      .headers()
      .as_ref()
      .and_then(|h| h.inner().get("x-delivery-count")?.as_short_uint())
      .unwrap_or(0);

    if delivery_count > max_delivery {
      delivery.reject(reject).await?
    }

    let event = serde_json::from_slice::<Event>(&delivery.data).context("should deserialize evt");

    if let Ok(ref e) = event {
      if let Ok(_) = EventConsumer::process_event(e, db).await {
        delivery.ack(BasicAckOptions::default()).await?
      };
    };

    log::error!("failed event requeued {:?}", event);
    delivery.nack(requeue).await
  }

  async fn process_event(event: &Event, db: &DbClient) -> Result<(), sqlx::Error> {
    match event {
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
