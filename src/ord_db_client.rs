use std::str::FromStr;
use std::sync::Arc;

use bitcoin::{OutPoint, Txid};
use futures::TryStreamExt;
use sqlx::{PgPool, Row};
use sqlx::types::Json;

use ordinals::SatPoint;

use crate::api::InscriptionDetails;
use crate::InscriptionId;

#[derive(Debug, Clone)]
pub struct Event {
  pub type_id: i16,
  pub block_height: i64,
  pub inscription_id: String,
  pub location: Option<SatPoint>,
  pub old_location: Option<SatPoint>,
}

impl Event {
  fn from_row(row: sqlx::postgres::PgRow) -> Self {
    Event {
      type_id: row.get("type_id"),
      block_height: row.get("block_height"),
      inscription_id: row.get("inscription_id"),
      location: {
        let location_str: Option<String> = row.get("location");
        location_str.and_then(|s| SatPoint::from_str(&s).ok())
      },
      old_location: {
        let old_location_str: Option<String> = row.get("old_location");
        old_location_str.and_then(|s| SatPoint::from_str(&s).ok())
      },
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

  pub async fn fetch_inscription_id_by_genesis_id(
    &self,
    genesis_id: String,
  ) -> Result<Option<i32>, sqlx::Error> {
    let query = "
            SELECT id FROM inscription
            WHERE genesis_id = $1;
        ";
    let result = sqlx::query(query)
      .bind(genesis_id)
      .fetch_optional(&*self.pool)
      .await?
      .map(|row| row.get(0));
    Ok(result)
  }

  pub async fn save_inscription(
    &self,
    inscription_details: &InscriptionDetails,
    metadata: Option<String>,
  ) -> Result<i32, sqlx::Error> {
    let query = "
        INSERT INTO inscription (
            genesis_id,
            number,
            content_type,
            content_length,
            metadata,
            genesis_block_height,
            genesis_block_time,
            sat_number,
            sat_rarity,
            sat_block_height,
            sat_block_time,
            fee,
            charms,
            children,
            parents
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        ON CONFLICT (genesis_id) DO UPDATE SET
            number = EXCLUDED.number,
            content_type = EXCLUDED.content_type,
            content_length = COALESCE(EXCLUDED.content_length, inscription.content_length),
            metadata = COALESCE(EXCLUDED.metadata, inscription.metadata),
            genesis_block_height = EXCLUDED.genesis_block_height,
            genesis_block_time = EXCLUDED.genesis_block_time,
            sat_number = COALESCE(EXCLUDED.sat_number, inscription.sat_number),
            sat_rarity = COALESCE(EXCLUDED.sat_rarity, inscription.sat_rarity),
            sat_block_height = COALESCE(EXCLUDED.sat_block_height, inscription.sat_block_height),
            sat_block_time = COALESCE(EXCLUDED.sat_block_time, inscription.sat_block_time),
            fee = EXCLUDED.fee,
            charms = EXCLUDED.charms,
            children = COALESCE(EXCLUDED.children, inscription.children),
            parents = COALESCE(EXCLUDED.parents, inscription.parents)
        RETURNING id;
    ";
    let id = sqlx::query_as::<_, (i32, )>(query)
      .bind(inscription_details.id.to_string())
      .bind(inscription_details.number)
      .bind(inscription_details.content_type.as_deref())
      .bind(inscription_details.content_length.map(|n| n as i32))
      .bind(metadata)
      .bind(inscription_details.genesis_block_height as i32)
      .bind(inscription_details.genesis_block_time)
      .bind(inscription_details.sat_number.map(|n| n as i64))
      .bind(inscription_details.sat_rarity.map(|r| r.to_i16()))
      .bind(inscription_details.sat_block_height.map(|n| n as i32))
      .bind(inscription_details.sat_block_time)
      .bind(inscription_details.fee as i64)
      .bind(inscription_details.charms as i16)
      .bind(Json(&inscription_details.children))
      .bind(Json(&inscription_details.parents))
      .fetch_one(&*self.pool)
      .await?;
    Ok(id.0)
  }

  pub async fn save_location(
    &self,
    id: i32,
    block_height: i64,
    block_time: u64,
    tx_id: Option<Txid>,
    to_address: Option<String>,
    to_outpoint: Option<OutPoint>,
    to_offset: Option<u64>,
    from_address: Option<String>,
    from_outpoint: Option<OutPoint>,
    from_offset: Option<u64>,
    value: Option<u64>,
  ) -> Result<(), sqlx::Error> {
    let query = "
        INSERT INTO location (
            inscription_id,
            block_height,
            block_time,
            tx_id,
            to_address,
            cur_output,
            cur_offset,
            from_address,
            prev_output,
            prev_offset,
            value
        )
        SELECT
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
        WHERE NOT EXISTS (
            SELECT 1 FROM location
            WHERE
                inscription_id = $1 AND
                block_height = $2 AND
                block_time = $3 AND
                tx_id = $4 AND
                to_address = $5 AND
                cur_output = $6 AND
                cur_offset = $7 AND
                from_address = $8 AND
                prev_output = $9 AND
                prev_offset = $10 AND
                value = $11
        );
    ";
    sqlx::query(query)
      .bind(id)
      .bind(block_height)
      .bind(block_time as i64)
      .bind(tx_id.map(|n| n.to_string()))
      .bind(to_address)
      .bind(to_outpoint.map(|n| n.to_string()))
      .bind(to_offset.map(|n| n as i64))
      .bind(from_address)
      .bind(from_outpoint.map(|n| n.to_string()))
      .bind(from_offset.map(|n| n as i64))
      .bind(value.map(|n| n as i64))
      .execute(&*self.pool)
      .await?;
    Ok(())
  }
}
