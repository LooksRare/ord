use std::sync::Arc;

use ordinals::SatPoint;

use crate::ord_api_client::OrdApiClient;
use crate::ord_db_client::{Event, OrdDbClient};
use crate::settings::Settings;
use crate::InscriptionId;

pub struct OrdIndexation {
  settings: Settings,
  ord_db_client: Arc<OrdDbClient>,
  ord_api_client: Arc<OrdApiClient>,
}

impl OrdIndexation {
  pub fn new(
    settings: &Settings,
    ord_db_client: Arc<OrdDbClient>,
    ord_api_client: Arc<OrdApiClient>,
  ) -> Self {
    Self {
      settings: settings.clone(),
      ord_api_client,
      ord_db_client,
    }
  }

  pub async fn sync_blocks(&self, from_height: &u32, to_height: &u32) -> Result<(), anyhow::Error> {
    log::info!("Blocks committed event from={from_height} (excluded), to={to_height} (included)");

    for block_height in *from_height + 1..=*to_height {
      let events = self
        .ord_db_client
        .fetch_events_by_block_height(block_height)
        .await?;
      for event in events {
        match event.type_id {
          1 => {
            if let Err(e) = self.process_inscription_created(&event).await {
              log::error!(
                "Error processing inscription creation for event {:?}: {}",
                event,
                e
              );
            }
          }
          2 => {
            // TODO: Handle type 2 events
          }
          _ => {
            log::warn!("Unhandled event type: {}", event.type_id);
          }
        }
      }
    }

    Ok(())
  }

  async fn process_inscription_created(&self, event: &Event) -> Result<(), anyhow::Error> {
    let inscription_details = self
      .ord_api_client
      .fetch_inscription_details(event.inscription_id.clone())
      .await?;
    self
      .ord_db_client
      .save_inscription(&inscription_details)
      .await?;

    let block_time = 0; //TODO need to fetch

    let mut to_location_details: Option<(String, u64)> = None;
    if let Some(location) = &event.location {
      to_location_details = self.process_location(location).await?;
    }

    let mut from_location_details: Option<(String, u64)> = None;
    if let Some(location) = &event.old_location {
      from_location_details = self.process_location(location).await?;
    }

    self
      .ord_db_client
      .save_location(
        inscription_details.id.clone(),
        event.block_height,
        block_time,
        event.location.as_ref().map(|loc| loc.outpoint.txid.clone()),
        to_location_details
          .as_ref()
          .map(|details| details.0.clone()),
        event.location.as_ref().map(|loc| loc.outpoint.clone()),
        event.location.as_ref().map(|loc| loc.offset),
        from_location_details
          .as_ref()
          .map(|details| details.0.clone()),
        event.old_location.as_ref().map(|loc| loc.outpoint.clone()),
        event.old_location.as_ref().map(|loc| loc.offset),
        to_location_details.as_ref().map(|details| details.1),
      )
      .await?;

    Ok(())
  }

  async fn process_location(
    &self,
    location: &SatPoint,
  ) -> Result<Option<(String, u64)>, anyhow::Error> {
    let tx_details = self
      .ord_api_client
      .fetch_tx(location.outpoint.txid.clone())
      .await?;

    let output = tx_details
      .transaction
      .output
      .into_iter()
      .nth(location.outpoint.vout.try_into().unwrap());

    let address = output
      .as_ref()
      .and_then(|o| {
        self
          .settings
          .chain()
          .address_from_script(&o.script_pubkey)
          .ok()
      })
      .map(|address| address.to_string());

    match address {
      Some(addr) => Ok(Some((addr, output.unwrap().value))),
      None => Ok(None),
    }
  }

  pub async fn save_inscription_created(
    &self,
    block_height: &u32,
    inscription_id: &InscriptionId,
    location: &Option<SatPoint>,
  ) -> Result<(), anyhow::Error> {
    self
      .ord_db_client
      .save_inscription_created(block_height, inscription_id, location)
      .await?;
    Ok(())
  }

  pub async fn save_inscription_transferred(
    &self,
    block_height: &u32,
    inscription_id: &InscriptionId,
    new_location: &SatPoint,
    old_location: &SatPoint,
  ) -> Result<(), anyhow::Error> {
    self
      .ord_db_client
      .save_inscription_transferred(block_height, inscription_id, new_location, old_location)
      .await?;
    Ok(())
  }
}
