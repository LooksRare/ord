use anyhow::Context;
use clap::Parser;
use futures::StreamExt;
use lapin::{message::Delivery, options::*, types::FieldTable};
use tokio::runtime::Runtime;

use crate::index::event::Event;
use crate::indexer::api_client::ApiClient;
use crate::indexer::db_client::DbClient;
use crate::indexer::inscription_indexation::InscriptionIndexation;
use crate::indexer::rmq_con::{generate_consumer_tag, setup_rabbitmq_connection};
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
            Ok(d) => BlockConsumer::handle_delivery(d, &inscription_indexer).await?,
            Err(err) => log::error!("error consuming message: {err}"),
          }
        }
      }
    })
  }

  async fn handle_delivery(
    delivery: Delivery,
    indexer: &InscriptionIndexation,
  ) -> Result<(), lapin::Error> {
    let reject = BasicRejectOptions { requeue: false };
    let event: Result<Event, _> = serde_json::from_slice(&delivery.data);

    match event {
      Ok(event) => {
        if let Err(err) = BlockConsumer::process_event(&event, indexer).await {
          log::error!("failed to process event: {err}");
          delivery.reject(reject).await
        } else {
          delivery.ack(BasicAckOptions::default()).await
        }
      }

      Err(e) => {
        log::error!("failed to deserialize event: {:?}", e);
        delivery.reject(reject).await
      }
    }
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
