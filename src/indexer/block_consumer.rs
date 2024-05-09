use anyhow::Context;
use clap::Parser;
use futures::StreamExt;
use lapin::{message::Delivery, options::*, types::FieldTable, Channel};
use tokio::runtime::Runtime;

use crate::index::event::Event;
use crate::indexer::api_client::ApiClient;
use crate::indexer::db_client::DbClient;
use crate::indexer::inscription_indexation::InscriptionIndexation;
use crate::indexer::rmq_con::{
  generate_consumer_tag, republish_to_queue, setup_rabbitmq_connection,
};
use crate::settings::Settings;
use crate::subcommand::SubcommandResult;

#[derive(Debug, Parser)]
pub struct BlockConsumer {
  #[arg(long, help = "RMQ queue to consume blocks.")]
  pub(crate) blocks_queue: Option<String>,

  #[arg(long, help = "DB url to persist inscriptions.")]
  pub(crate) database_url: Option<String>,

  #[arg(long, help = "Ord api url to fetch inscriptions.")]
  pub(crate) ord_api_url: Option<String>,
}

impl BlockConsumer {
  pub fn run(self, settings: &Settings) -> SubcommandResult {
    Runtime::new()?.block_on(async {
      let database_url = self.database_url.context("db url is required")?;
      let db = DbClient::new(database_url, 2).await?;

      let api_url = self.ord_api_url.context("api url must be defined")?;
      let api_client = ApiClient::new(api_url.clone()).context("Failed to create API client")?;

      let tag = generate_consumer_tag("lr-ord-evts");
      let addr = settings.rabbitmq_addr().context("rmq url is required")?;
      let queue_name = self.blocks_queue.context("rmq queue is required")?;
      let channel = setup_rabbitmq_connection(addr).await?;
      channel
        .basic_qos(2, BasicQosOptions::default())
        .await
        .context("Failed to set basic_qos")?;

      let inscription_indexer = InscriptionIndexation::new(settings, db, api_client);

      let mut consumer = channel
        .basic_consume(
          &queue_name,
          tag.as_str(),
          BasicConsumeOptions::default(),
          FieldTable::default(),
        )
        .await?;

      log::info!("started block consumer {tag} for {queue_name}");

      loop {
        if let Some(msg) = consumer.next().await {
          match msg {
            Ok(d) => BlockConsumer::handle_delivery(&channel, d, &inscription_indexer).await?,
            Err(err) => log::error!("error consuming message: {err}"),
          }
        }
      }
    })
  }

  /// Handle the persistence of incoming queue "block" messages.
  ///
  /// Re-enqueues the message up to `max_delivery` times for processing failures.
  /// Bubbles up the `lapin::Error` only if the ack/reject itself fails.
  async fn handle_delivery(
    channel: &Channel,
    delivery: Delivery,
    indexer: &InscriptionIndexation,
  ) -> Result<(), lapin::Error> {
    let max_delivery = 3;
    let reject = BasicRejectOptions { requeue: false };

    let delivery_count = delivery
      .properties
      .headers()
      .as_ref()
      .and_then(|h| h.inner().get("x-delivery-count")?.as_short_uint())
      .unwrap_or(0);

    let event = serde_json::from_slice::<Event>(&delivery.data).context("should deserialize evt");

    if delivery_count > max_delivery {
      log::error!("failed event dropped {:?}", event);
      return delivery.reject(reject).await;
    }

    if let Ok(ref e) = event {
      if BlockConsumer::process_event(e, indexer).await.is_ok() {
        return delivery.ack(BasicAckOptions::default()).await;
      };
    };

    log::warn!("failed event requeued {:?}", event);
    republish_to_queue(channel, &delivery, &delivery_count).await?;
    delivery.reject(reject).await
  }

  async fn process_event(
    event: &Event,
    indexer: &InscriptionIndexation,
  ) -> Result<(), anyhow::Error> {
    match event {
      Event::BlockCommitted { height } => indexer.sync_blocks(height).await,
      _ => Ok(log::warn!("skipped unhandled event type {:?}", event)),
    }
  }
}
