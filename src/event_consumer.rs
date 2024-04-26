use anyhow::Context;
use clap::Parser;
use futures::StreamExt;
use lapin::{options::*, types::FieldTable, Connection, ConnectionProperties};
use tokio::runtime::Runtime;

use ordinals::SatPoint;

use crate::index::event::Event;
use crate::ord_api_client::OrdApiClient;
use crate::settings::Settings;
use crate::subcommand::SubcommandResult;
use crate::InscriptionId;

#[derive(Debug, Parser, Clone)]
pub struct EventConsumer {
  #[arg(long, help = "RMQ queue to consume index events.")]
  pub(crate) rabbitmq_queue: Option<String>,
}

impl EventConsumer {
  pub fn run(self, settings: &Settings, ord_api_client: OrdApiClient) -> SubcommandResult {
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

      log::info!("Starting to consume messages from {}", queue);
      while let Some(delivery) = consumer.next().await {
        match delivery {
          Ok(delivery) => {
            let event: Result<Event, _> = serde_json::from_slice(&delivery.data);
            match event {
              Ok(event) => {
                self
                  .handle_event(&ord_api_client, &event)
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
    ord_api_client: &OrdApiClient,
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
        self
          .handle_inscription_created(
            ord_api_client,
            block_height,
            charms,
            inscription_id,
            location,
            parent_inscription_ids,
            sequence_number,
          )
          .await
      }
      Event::InscriptionTransferred {
        block_height,
        inscription_id,
        new_location,
        old_location,
        sequence_number,
      } => {
        self
          .handle_inscription_transferred(
            ord_api_client,
            block_height,
            inscription_id,
            new_location,
            old_location,
            sequence_number,
          )
          .await
      }
      _ => Ok(()),
    }
  }

  async fn handle_inscription_created(
    &self,
    ord_api_client: &OrdApiClient,
    block_height: &u32,
    charms: &u16,
    inscription_id: &InscriptionId,
    location: &Option<SatPoint>,
    parent_inscription_ids: &Vec<InscriptionId>,
    sequence_number: &u32,
  ) -> Result<(), anyhow::Error> {
    log::info!("Received inscription created event: {:?}", inscription_id);
    match ord_api_client
      .fetch_inscription_details(inscription_id)
      .await
    {
      Ok(inscription) => {
        log::info!("Received inscription detailed response: {:?}", inscription);
      }
      Err(e) => {
        log::error!("Failed to fetch: {:?}", e);
      }
    };
    Ok(())
  }

  async fn handle_inscription_transferred(
    &self,
    ord_api_client: &OrdApiClient,
    block_height: &u32,
    inscription_id: &InscriptionId,
    new_location: &SatPoint,
    old_location: &SatPoint,
    sequence_number: &u32,
  ) -> Result<(), anyhow::Error> {
    log::info!(
      "Received inscription transferred event: {:?}",
      inscription_id
    );
    Ok(())
  }
}
