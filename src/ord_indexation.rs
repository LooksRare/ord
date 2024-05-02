use std::sync::Arc;

use ordinals::SatPoint;

use crate::ord_api_client::OrdApiClient;
use crate::ord_db_client::OrdDbClient;
use crate::InscriptionId;

pub struct OrdIndexation {
  ord_db_client: Arc<OrdDbClient>,
  ord_api_client: Arc<OrdApiClient>,
}

impl OrdIndexation {
  pub fn new(ord_db_client: Arc<OrdDbClient>, ord_api_client: Arc<OrdApiClient>) -> Self {
    Self {
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
        log::info!("Event: {:?}", event);
        // let inscription_details = self.ord_api_client.fetch_inscription_details(event.inscription_id).await?;
      }
    }

    Ok(())
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
