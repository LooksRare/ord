use std::sync::Arc;

use futures::TryStreamExt;
use sqlx::{PgPool, Row};

use ordinals::SatPoint;

use crate::InscriptionId;

#[derive(Debug, Clone)]
pub struct Event {
  pub type_id: i16,
  pub block_height: i64,
  pub inscription_id: String,
  pub location: Option<String>,
  pub old_location: Option<String>,
}

impl Event {
  fn from_row(row: sqlx::postgres::PgRow) -> Self {
    Event {
      type_id: row.get("type_id"),
      block_height: row.get("block_height"),
      inscription_id: row.get("inscription_id"),
      location: row.get("location"),
      old_location: row.get("old_location"),
    }
  }
}

pub struct OrdDbClient {
  pool: Arc<PgPool>,
}

impl OrdDbClient {
  pub fn new(pool: Arc<PgPool>) -> Self {
    Self { pool }
  }

  pub async fn fetch_events_by_block_height(
    &self,
    block_height: u32,
  ) -> Result<Vec<Event>, sqlx::Error> {
    let query = "
        SELECT type_id, block_height, inscription_id, location, old_location
        FROM events
        WHERE block_height = $1
        ORDER BY type_id ASC, id ASC";

    let mut rows = sqlx::query(query)
      .bind(block_height as i64)
      .fetch(&*self.pool);

    let mut events = Vec::new();

    while let Some(row) = rows.try_next().await? {
      let event = Event::from_row(row);
      events.push(event);
    }

    Ok(events)
  }

  pub async fn save_inscription_created(
    &self,
    block_height: &u32,
    inscription_id: &InscriptionId,
    location: &Option<SatPoint>,
  ) -> Result<(), sqlx::Error> {
    let query = "
        INSERT INTO events (type_id, block_height, inscription_id, location)
        SELECT $1, $2, $3, $4
        WHERE NOT EXISTS (
            SELECT 1 FROM events
            WHERE type_id = $1 AND block_height = $2 AND inscription_id = $3 AND location = $4
        )";
    sqlx::query(query)
      .bind(1_i32) // Type ID for InscriptionCreated
      .bind(*block_height as i64)
      .bind(inscription_id.to_string())
      .bind(location.map(|loc| loc.to_string()))
      .execute(&*self.pool)
      .await?;
    Ok(())
  }

  pub async fn save_inscription_transferred(
    &self,
    block_height: &u32,
    inscription_id: &InscriptionId,
    new_location: &SatPoint,
    old_location: &SatPoint,
  ) -> Result<(), sqlx::Error> {
    let query = "
        INSERT INTO events (type_id, block_height, inscription_id, location, old_location)
        SELECT $1, $2, $3, $4, $5
        WHERE NOT EXISTS (
            SELECT 1 FROM events
            WHERE type_id = $1 AND block_height = $2 AND inscription_id = $3 AND location = $4 AND old_location = $5
        )";
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
