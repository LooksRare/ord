use std::sync::Arc;

use sqlx::PgPool;

use ordinals::SatPoint;

use crate::InscriptionId;

pub struct OrdDbClient {
  pool: Arc<PgPool>,
}

impl OrdDbClient {
  pub fn new(pool: Arc<PgPool>) -> Self {
    Self { pool }
  }

  pub async fn sync_blocks(&self,
                           from_height: &u32,
                           to_height: &u32) -> Result<(), sqlx::Error> {
    log::info!("Blocks committed event from={from_height} (excluded), to={to_height} (included)");
    // TODO consume all blocks from this to last consumed
    Ok(())
  }

  pub async fn save_inscription_created(&self,
                                        block_height: &u32,
                                        inscription_id: &InscriptionId,
                                        location: &Option<SatPoint>) -> Result<(), sqlx::Error> {
    let query = "INSERT INTO events (type_id, block_height, inscription_id, location) VALUES ($1, $2, $3, $4)";
    sqlx::query(query)
      .bind(1_i32) // Type ID for InscriptionCreated
      .bind(*block_height as i64)
      .bind(inscription_id.to_string())
      .bind(location.map(|loc| loc.to_string()))
      .execute(&*self.pool)
      .await?;
    Ok(())
  }

  pub async fn save_inscription_transferred(&self,
                                            block_height: &u32,
                                            inscription_id: &InscriptionId,
                                            new_location: &SatPoint,
                                            old_location: &SatPoint) -> Result<(), sqlx::Error> {
    let query = "INSERT INTO events (type_id, block_height, inscription_id, location, old_location) VALUES ($1, $2, $3, $4, $5)";
    sqlx::query(query)
      .bind(2_i32) // Type ID for InscriptionTransferred
      .bind(*block_height as i64)
      .bind(inscription_id.to_string())
      .bind(new_location.to_string())
      .bind(old_location.to_string())
      .execute(&*self.pool)
      .await?;
    Ok(())
  }
}
