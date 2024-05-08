use std::io::Cursor;
use std::sync::Arc;

use ciborium::from_reader;
use serde_json::Value;
use sha3::digest::const_oid::db::rfc4519::L;

use ordinals::SatPoint;

use crate::api::BlockInfo;
use crate::indexer::api_client::ApiClient;
use crate::indexer::db_client::{DbClient, Event};
use crate::settings::Settings;

pub struct InscriptionIndexation {
  settings: Settings,
  ord_db_client: Arc<DbClient>,
  ord_api_client: Arc<ApiClient>,
}

impl InscriptionIndexation {
  pub fn new(
    settings: &Settings,
    ord_db_client: Arc<DbClient>,
    ord_api_client: Arc<ApiClient>,
  ) -> Self {
    Self {
      settings: settings.clone(),
      ord_api_client,
      ord_db_client,
    }
  }

  pub async fn sync_blocks(&self, block_height: &u32) -> Result<(), anyhow::Error> {
    log::info!("Blocks committed event for={block_height}");

    let events = self
      .ord_db_client
      .fetch_events_by_block_height(block_height)
      .await?;

    if (events.is_empty()) {
      return Ok(());
    }

    let block_info = self.ord_api_client.fetch_block_info(block_height).await?;
    for event in events {
      match event.type_id {
        1 => {
          if let Err(e) = self.process_inscription_created(&event, &block_info).await {
            log::error!(
              "Error processing inscription creation for event {:?}: {}",
              event,
              e
            );
          }
        }
        2 => {
          if let Err(e) = self
            .process_inscription_transferred(&event, &block_info)
            .await
          {
            log::error!(
              "Error processing inscription transferred for event {:?}: {}",
              event,
              e
            );
          }
        }
        _ => {
          log::warn!("Unhandled event type: {}", event.type_id);
        }
      }
    }

    Ok(())
  }

  async fn process_inscription_created(
    &self,
    event: &Event,
    block_info: &BlockInfo,
  ) -> Result<(), anyhow::Error> {
    let inscription_details = self
      .ord_api_client
      .fetch_inscription_details(event.inscription_id.clone())
      .await?;

    let metadata = self.try_and_extract_metadata(&inscription_details.metadata);

    let inscription_id = self
      .ord_db_client
      .save_inscription(&inscription_details, metadata)
      .await?;

    let mut to_location_details: Option<(String, u64)> = None;
    if let Some(location) = &event.location {
      to_location_details = self.process_location(location).await?;
    }

    self
      .ord_db_client
      .save_location(
        inscription_id,
        event.block_height,
        block_info.timestamp,
        event.location.as_ref().map(|loc| loc.outpoint.txid),
        to_location_details
          .as_ref()
          .map(|details| details.0.clone()),
        event.location.as_ref().map(|loc| loc.outpoint),
        event.location.as_ref().map(|loc| loc.offset),
        None,
        None,
        None,
        to_location_details.as_ref().map(|details| details.1),
      )
      .await?;

    Ok(())
  }

  fn try_and_extract_metadata(&self, metadata: &Option<Vec<u8>>) -> Option<String> {
    metadata.as_ref().and_then(|bytes| {
      let cursor = Cursor::new(bytes);
      let result = from_reader(cursor);
      match result {
        Ok(value) => match &value {
          Value::Object(obj) if obj.is_empty() => None,
          Value::Object(_) | Value::Array(_) => serde_json::to_string(&value).ok(),
          _ => Some(hex::encode(bytes)),
        },
        Err(_) => Some(hex::encode(bytes)),
      }
    })
  }

  async fn process_inscription_transferred(
    &self,
    event: &Event,
    block_info: &BlockInfo,
  ) -> Result<(), anyhow::Error> {
    let inscription_id = self
      .ord_db_client
      .fetch_inscription_id_by_genesis_id(event.inscription_id.clone())
      .await?
      .ok_or_else(|| {
        anyhow::anyhow!(
          "No inscription found for genesis_id: {}",
          event.inscription_id
        )
      })?;

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
        inscription_id,
        event.block_height,
        block_info.timestamp,
        event.location.as_ref().map(|loc| loc.outpoint.txid),
        to_location_details
          .as_ref()
          .map(|details| details.0.clone()),
        event.location.as_ref().map(|loc| loc.outpoint),
        event.location.as_ref().map(|loc| loc.offset),
        from_location_details
          .as_ref()
          .map(|details| details.0.clone()),
        event.old_location.as_ref().map(|loc| loc.outpoint),
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
    let tx_details = self.ord_api_client.fetch_tx(location.outpoint.txid).await?;

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
}
