use anyhow::Context;
use clap::Parser;
use futures::StreamExt;
use lapin::{options::*, types::FieldTable};
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;

use crate::index::event::Event;
use crate::indexer::db_client::DbClient;
use crate::indexer::db_con::setup_db_connection;
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
      let addr = settings
        .rabbitmq_addr()
        .context("rabbitmq amqp credentials and url must be defined")?;
      let channel = setup_rabbitmq_connection(addr).await?;

      let database_url = self
        .database_url
        .as_deref()
        .context("db url must be defined")?;
      let pool = setup_db_connection(database_url).await?;

      let shared_pool = Arc::new(pool);
      let db_client = Arc::new(DbClient::new(shared_pool.clone()));

      let inscriptions_queue = self
        .inscriptions_queue
        .as_deref()
        .context("rabbitmq inscriptions queue path must be defined")?;
      let inscriptions_queue_str = inscriptions_queue.to_string();
      let inscriptions_channel = channel.clone();
      let inscriptions_consumer_tag = generate_consumer_tag("lr-ord");
      let (inscriptions_shutdown_tx, inscriptions_shutdown_rx) = oneshot::channel::<()>();

      let inscriptions_consumer_handle = tokio::spawn(async move {
        EventConsumer::consume_queue(
          inscriptions_channel,
          inscriptions_queue_str,
          inscriptions_consumer_tag,
          db_client,
          inscriptions_shutdown_rx,
        )
        .await
        .expect("error consuming inscriptions queue")
      });

      let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
      sigterm.recv().await;
      let _ = inscriptions_shutdown_tx.send(());
      let _ = tokio::try_join!(inscriptions_consumer_handle);

      shared_pool.close().await;

      Ok(None)
    })
  }

  async fn consume_queue(
    channel: lapin::Channel,
    queue_name: String,
    consumer_tag: String,
    db_client: Arc<DbClient>,
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
          process_result = EventConsumer::handle_delivery(delivery, &db_client) => {
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
    db_client: &Arc<DbClient>,
  ) -> Result<(), anyhow::Error> {
    let event: Result<Event, _> = serde_json::from_slice(&delivery.data);
    match event {
      Ok(event) => {
        if let Err(err) = EventConsumer::process_event(event, db_client).await {
          log::error!("Failed to process event: {}", err);
          delivery
            .reject(BasicRejectOptions { requeue: false })
            .await?;
        } else {
          delivery.ack(BasicAckOptions::default()).await?;
        }
      }
      Err(e) => {
        log::error!("Failed to deserialize event, rejecting: {}", e);
        delivery
          .reject(BasicRejectOptions { requeue: false })
          .await?;
      }
    }

    Ok(())
  }

  async fn process_event(event: Event, db_client: &Arc<DbClient>) -> Result<(), anyhow::Error> {
    match &event {
      Event::InscriptionCreated {
        block_height,
        charms: _charms,
        inscription_id,
        location,
        parent_inscription_ids: _parent_inscription_ids,
        sequence_number: _sequence_number,
      } => {
        db_client
          .save_inscription_created(block_height, inscription_id, location)
          .await?
      }
      Event::InscriptionTransferred {
        block_height,
        inscription_id,
        new_location,
        old_location,
        sequence_number: _sequence_number,
      } => {
        db_client
          .save_inscription_transferred(block_height, inscription_id, new_location, old_location)
          .await?;
      }
      _ => {
        log::warn!("Received an unhandled event type {:?}", event);
      }
    }
    Ok(())
  }
}
