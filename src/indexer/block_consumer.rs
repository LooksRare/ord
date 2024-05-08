use std::process;
use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use futures::StreamExt;
use lapin::types::AMQPValue;
use lapin::{options::*, types::FieldTable};
use tokio::runtime::Runtime;
use tokio::sync::oneshot;

use crate::index::event::Event;
use crate::indexer::api_client::ApiClient;
use crate::indexer::db_client::DbClient;
use crate::indexer::inscription_indexation::InscriptionIndexation;
use crate::indexer::rmq_con::{generate_consumer_tag, setup_rabbitmq_connection};
use crate::settings::Settings;
use crate::subcommand::SubcommandResult;

#[derive(Debug, Parser, Clone)]
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
      let addr = settings
        .rabbitmq_addr()
        .context("rabbitmq amqp credentials and url must be defined")?;
      let channel = setup_rabbitmq_connection(addr).await?;

      let database_url = self.database_url.context("db url is required")?;
      let db_client = Arc::new(DbClient::new(database_url, 2).await?);

      let api_url = self.ord_api_url.context("api url must be defined")?;
      let api_c = ApiClient::new(api_url.clone()).context("Failed to create API client")?;
      let api_client = Arc::new(api_c);

      let inscription_indexation = Arc::new(InscriptionIndexation::new(
        settings,
        db_client.clone(),
        api_client,
      ));

      let blocks_queue = self
        .blocks_queue
        .as_deref()
        .context("rabbitmq blocks queue path must be defined")?;
      let blocks_queue_str = blocks_queue.to_string();
      let blocks_channel = channel.clone();
      let blocks_consumer_tag = generate_consumer_tag("lr-ord");
      let (blocks_shutdown_tx, blocks_shutdown_rx) = oneshot::channel::<()>();

      let blocks_consumer_handle = tokio::spawn(async move {
        BlockConsumer::consume_queue(
          blocks_channel,
          blocks_queue_str,
          blocks_consumer_tag,
          inscription_indexation,
          blocks_shutdown_rx,
        )
        .await
        .map_err(|e| {
          log::error!("Error consuming blocks queue: {}", e);
          process::exit(1);
        })
      });

      let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
      sigterm.recv().await;
      let _ = blocks_shutdown_tx.send(());
      let _ = tokio::try_join!(blocks_consumer_handle);

      db_client.close().await;

      Ok(None)
    })
  }

  async fn consume_queue(
    channel: lapin::Channel,
    queue_name: String,
    consumer_tag: String,
    inscription_indexation: Arc<InscriptionIndexation>,
    mut shutdown_signal: oneshot::Receiver<()>,
  ) -> Result<(), anyhow::Error> {
    let mut consumer = channel
      .basic_consume(
        &queue_name,
        consumer_tag.as_str(),
        BasicConsumeOptions::default(),
        FieldTable::default(),
      )
      .await?;

    log::info!("Starting to consume messages from {}", queue_name);
    while let Some(result) = consumer.next().await {
      let delivery = result?;
      tokio::select! {
          process_result = BlockConsumer::handle_delivery(delivery, &inscription_indexation) => {
              process_result?;
          },
          _ = &mut shutdown_signal => {
              log::info!("Shutdown signal received, stopping consumer.");
              break;
          },
      }
    }

    log::info!("Closing consumer channel {}", queue_name);
    channel
      .close(200, "Closing channel due to shutdown")
      .await?;

    Ok(())
  }

  async fn handle_delivery(
    delivery: lapin::message::Delivery,
    inscription_indexation: &Arc<InscriptionIndexation>,
  ) -> Result<(), lapin::Error> {
    let event: Result<Event, _> = serde_json::from_slice(&delivery.data);

    let mut delivery_count = 0;
    if let Some(headers) = delivery.properties.headers() {
      if let Some(AMQPValue::LongLongInt(count)) = headers.inner().get("x-delivery-count") {
        delivery_count = *count;
      }
    }

    match event {
      Ok(event) => {
        if let Err(err) = BlockConsumer::process_event(event, inscription_indexation).await {
          if delivery_count >= 3 {
            log::error!("Failed to process event: {}, rejecting", err);
            delivery.reject(BasicRejectOptions { requeue: false }).await
          } else {
            log::warn!("Failed to process event: {}, requeuing", err);
            delivery
              .nack(BasicNackOptions {
                multiple: false,
                requeue: true,
              })
              .await
          }
        } else {
          delivery.ack(BasicAckOptions::default()).await
        }
      }

      Err(e) => {
        log::error!("Failed to deserialize event, rejecting: {}", e);
        delivery.reject(BasicRejectOptions { requeue: false }).await
      }
    }
  }

  async fn process_event(
    event: Event,
    inscription_indexation: &Arc<InscriptionIndexation>,
  ) -> Result<(), anyhow::Error> {
    match &event {
      Event::BlockCommitted { height } => inscription_indexation.sync_blocks(height).await?,
      _ => {
        log::warn!("Received an unhandled event type {:?}", event);
      }
    }
    Ok(())
  }
}
