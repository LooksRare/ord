use std::sync::Arc;

use anyhow::Context;
use bitcoin::secp256k1::rand::distributions::Alphanumeric;
use chrono::Utc;
use clap::Parser;
use futures::StreamExt;
use lapin::{Connection, ConnectionProperties, options::*, types::FieldTable};
use rand::distributions::DistString;
use sqlx::postgres::PgPoolOptions;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;

use crate::index::event::Event;
use crate::ord_db_client::OrdDbClient;
use crate::settings::Settings;
use crate::subcommand::SubcommandResult;

#[derive(Debug, Parser, Clone)]
pub struct EventConsumer {
  #[arg(long, help = "RMQ queue to consume blocks.")]
  pub(crate) blocks_queue: Option<String>,
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

      let database_url = self
        .database_url
        .as_deref()
        .context("db url must be defined")?;
      let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await?;
      let shared_pool = Arc::new(pool);
      let ord_db_client = Arc::new(OrdDbClient::new(shared_pool.clone()));

      let blocks_queue = self.blocks_queue.as_deref().context("rabbitmq blocks queue path must be defined")?;
      let blocks_queue_str = blocks_queue.to_string();
      let blocks_channel = channel.clone();
      let blocks_ord_db_client = Arc::clone(&ord_db_client);
      let blocks_consumer_tag = Self::generate_consumer_tag();
      let (blocks_shutdown_tx, blocks_shutdown_rx) = oneshot::channel::<()>();

      let inscriptions_queue = self.inscriptions_queue.as_deref().context("rabbitmq inscriptions queue path must be defined")?;
      let inscriptions_queue_str = inscriptions_queue.to_string();
      let inscriptions_channel = channel.clone();
      let inscriptions_ord_db_client = Arc::clone(&ord_db_client);
      let inscriptions_consumer_tag = Self::generate_consumer_tag();
      let (inscriptions_shutdown_tx, inscriptions_shutdown_rx) = oneshot::channel::<()>();

      let blocks_consumer_handle = tokio::spawn(async move {
        EventConsumer::consume_queue(blocks_channel, blocks_queue_str, blocks_consumer_tag, blocks_ord_db_client, blocks_shutdown_rx).await
      });

      let inscriptions_consumer_handle = tokio::spawn(async move {
        EventConsumer::consume_queue(inscriptions_channel, inscriptions_queue_str, inscriptions_consumer_tag, inscriptions_ord_db_client, inscriptions_shutdown_rx).await
      });

      let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
      sigterm.recv().await;
      let _ = blocks_shutdown_tx.send(());
      let _ = inscriptions_shutdown_tx.send(());
      let _ = tokio::try_join!(blocks_consumer_handle, inscriptions_consumer_handle);

      shared_pool.close().await;

      Ok(None)
    })
  }

  async fn consume_queue(channel: lapin::Channel,
                         queue_name: String,
                         consumer_tag: String,
                         ord_db_client: Arc<OrdDbClient>,
                         mut shutdown_signal: oneshot::Receiver<()>,
  ) -> Result<(), anyhow::Error> {
    let mut consumer = channel
      .basic_consume(
        &queue_name,
        consumer_tag.as_str(),
        BasicConsumeOptions::default(),
        FieldTable::default(),
      )
      .await
      .expect("creates rmq consumer");

    log::info!("Starting to consume messages from {}", queue_name);
    while let Some(result) = consumer.next().await {
      match result {
        Ok(delivery) => {
          tokio::select! {
                _ = EventConsumer::handle_delivery(delivery, &ord_db_client) => {},
                _ = &mut shutdown_signal => {
                    log::info!("Shutdown signal received, stopping consumer.");
                    break;
                },
            }
        }
        Err(e) => {
          log::error!("Error receiving delivery: {}", e);
        }
      }
    }

    log::info!("Closing consumer channel {}", queue_name);
    channel.close(200, "Closing channel due to shutdown").await?;

    Ok(())
  }

  async fn handle_delivery(delivery: lapin::message::Delivery,
                           ord_db_client: &Arc<OrdDbClient>) -> Result<(), anyhow::Error> {
    let event: Result<Event, _> = serde_json::from_slice(&delivery.data);
    match event {
      Ok(event) => {
        if let Err(err) = EventConsumer::process_event(event, ord_db_client).await {
          log::error!("Failed to process event: {}", err);
          delivery.reject(BasicRejectOptions { requeue: false }).await?;
        } else {
          delivery.ack(BasicAckOptions::default()).await?;
        }
      }
      Err(e) => {
        log::error!("Failed to deserialize event, rejecting: {}", e);
        delivery.reject(BasicRejectOptions { requeue: false }).await?;
      }
    }

    Ok(())
  }

  async fn process_event(event: Event, ord_db_client: &Arc<OrdDbClient>) -> Result<(), anyhow::Error> {
    match &event {
      Event::BlockCommitted {
        from_height,
        to_height,
      } => {
        ord_db_client.sync_blocks(from_height, to_height).await?
      }
      Event::InscriptionCreated {
        block_height,
        charms,
        inscription_id,
        location,
        parent_inscription_ids,
        sequence_number,
      } => {
        ord_db_client.save_inscription_created(block_height, inscription_id, location).await?
      }
      Event::InscriptionTransferred {
        block_height,
        inscription_id,
        new_location,
        old_location,
        sequence_number,
      } => {
        ord_db_client.save_inscription_transferred(block_height, inscription_id, new_location, old_location).await?;
      }
      _ => {
        log::warn!("Received an unhandled event type");
      }
    }
    Ok(())
  }

  fn generate_consumer_tag() -> String {
    // TODO get pod name from k8s?
    let timestamp = Utc::now().format("%Y%m%d%H%M%S");
    format!("{}-{}-{}", "lr-ord", timestamp, Alphanumeric.sample_string(&mut rand::thread_rng(), 16))
  }
}
