use anyhow::anyhow;
use ciborium::from_reader;
use ordinals::SatPoint;
use serde_json::Value;
use std::io::Cursor;

use crate::api::BlockInfo;
use crate::indexer::api_client::ApiClient;
use crate::indexer::db_client::{DbClient, Event, EventType};
use crate::settings::Settings;

pub struct InscriptionIndexation {
  settings: Settings,
  db: DbClient,
  api: ApiClient,
}

impl InscriptionIndexation {
  pub fn new(settings: &Settings, db: DbClient, api: ApiClient) -> Self {
    Self {
      settings: settings.clone(),
      db,
      api,
    }
  }

  pub async fn sync_blocks(&self, block_height: &u32) -> Result<(), anyhow::Error> {
    let events = self.db.fetch_events_by_block_height(block_height).await?;

    if !events.is_empty() {
      let block_info = self.api.fetch_block_info(block_height).await?;

      for event in events {
        match event.get_type() {
          EventType::InscriptionCreated => self
            .process_inscription_created(&event, &block_info)
            .await
            .inspect_err(|e| log::error!("error with inscription_created {:?}: {e}", event)),

          EventType::InscriptionTransferred => self
            .process_inscription_transferred(&event, &block_info)
            .await
            .inspect_err(|e| log::error!("error with inscription_transferred {:?}: {e}", event)),
        }?;
      }
    }

    // log::info!("Block {block_height} consumed");

    Ok(())
  }

  async fn process_inscription_created(
    &self,
    event: &Event,
    block_info: &BlockInfo,
  ) -> anyhow::Result<()> {
    let inscription = self
      .api
      .fetch_inscription_details(&event.inscription_id)
      .await?;
    let metadata = self.try_and_extract_metadata(&inscription.metadata);
    let location = event.location.as_ref();
    let to_location = match location {
      Some(loc) => Some(self.process_location(loc).await?),
      None => None,
    };

    let id = self.db.save_inscription(&inscription, metadata).await?;

    self
      .db
      .save_location(
        id,
        event.block_height,
        block_info.timestamp,
        location.map(|loc| loc.outpoint.txid),
        to_location.as_ref().and_then(|d| d.2),
        to_location.as_ref().map(|d| d.0.clone()),
        location.map(|loc| loc.outpoint),
        location.map(|loc| loc.offset),
        None,
        None,
        None,
        to_location.as_ref().map(|d| d.1),
      )
      .await
      .map_err(|err| anyhow!("error saving inscription_created location: {err}"))
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
    let inscription_id = match self
      .db
      .fetch_inscription_id_by_genesis_id(&event.inscription_id)
      .await?
    {
      Some(id) => id,
      None => {
        let inscription = self
          .api
          .fetch_inscription_details(&event.inscription_id)
          .await?;
        let metadata = self.try_and_extract_metadata(&inscription.metadata);
        self.db.save_inscription(&inscription, metadata).await?
      }
    };

    let location = event.location.as_ref();
    let old_location = event.location.as_ref();

    let to_location = match &event.location {
      Some(location) => Some(self.process_location(location).await?),
      None => None,
    };
    let from_location = match &event.old_location {
      Some(location) => Some(self.process_location(location).await?),
      None => None,
    };

    self
      .db
      .save_location(
        inscription_id,
        event.block_height,
        block_info.timestamp,
        location.map(|loc| loc.outpoint.txid),
        to_location.as_ref().and_then(|d| d.2),
        to_location.as_ref().map(|d| d.0.clone()),
        location.map(|loc| loc.outpoint),
        location.map(|loc| loc.offset),
        from_location.as_ref().map(|d| d.0.clone()),
        old_location.map(|loc| loc.outpoint),
        old_location.map(|loc| loc.offset),
        to_location.as_ref().map(|d| d.1),
      )
      .await
      .map_err(|err| anyhow!("error saving inscription_transferred location: {err}"))
  }

  async fn process_location(
    &self,
    location: &SatPoint,
  ) -> Result<(String, u64, Option<usize>), anyhow::Error> {
    let tx_details = self.api.fetch_tx(location.outpoint.txid).await?;

    let output = tx_details
      .transaction
      .output
      .into_iter()
      .nth(location.outpoint.vout.try_into()?)
      .ok_or_else(|| anyhow!("output not found in tx: {}", location.outpoint.txid))?;

    let address = self
      .settings
      .chain()
      .address_from_script(&output.script_pubkey)?
      .to_string();

    Ok((address, output.value, tx_details.tx_index))
  }
}
