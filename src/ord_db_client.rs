use bitcoin::{OutPoint, Txid};
use ordinals::SatPoint;
use sqlx::types::Json;
use sqlx::PgPool;
use std::str::FromStr;
use std::sync::Arc;

use crate::api::InscriptionDetails;
use crate::InscriptionId;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Event {
  pub type_id: i16,
  pub block_height: i32,
  pub inscription_id: String,
  pub location: Option<SatPoint>,
  pub old_location: Option<SatPoint>,
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
    sqlx::query!(
      r#"
      SELECT type_id, block_height, inscription_id, location, old_location
      FROM event WHERE block_height = $1
      ORDER BY type_id ASC, id ASC
      "#,
      i32::try_from(block_height).expect("block_height should fit in pg integer"),
    )
    .map(|r| Event {
      type_id: r.type_id,
      block_height: r.block_height,
      inscription_id: r.inscription_id,
      location: r.location.and_then(|s| SatPoint::from_str(&s).ok()),
      old_location: r.old_location.and_then(|s| SatPoint::from_str(&s).ok()),
    })
    .fetch_all(&*self.pool)
    .await
  }

  pub async fn save_inscription_created(
    &self,
    block_height: &u32,
    inscription_id: &InscriptionId,
    location: &Option<SatPoint>,
  ) -> Result<(), sqlx::Error> {
    sqlx::query!(
      r#"
      INSERT INTO event (type_id, block_height, inscription_id, location)
      SELECT $1, $2, $3, $4
      WHERE NOT EXISTS (
          SELECT 1 FROM event
          WHERE type_id = $1 AND block_height = $2 AND inscription_id = $3 AND location = $4
      )
      "#,
      1, // Type ID for `InscriptionCreated`
      block_height.to_owned() as i64,
      inscription_id.to_string(),
      location.map(|loc| loc.to_string())
    )
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
    sqlx::query!(
      r#"
      INSERT INTO event (type_id, block_height, inscription_id, location, old_location)
      SELECT $1, $2, $3, $4, $5
      WHERE NOT EXISTS (
          SELECT 1 FROM event
          WHERE type_id = $1 AND block_height = $2 AND inscription_id = $3 AND location = $4 AND old_location = $5
      )
      "#,
      2, // Type ID for `InscriptionCreated`
      block_height.to_owned() as i64,
      inscription_id.to_string(),
      new_location.to_string(),
      old_location.to_string()
    )
    .execute(&*self.pool)
    .await?;

    Ok(())
  }

  pub async fn fetch_inscription_id_by_genesis_id(
    &self,
    genesis_id: String,
  ) -> Result<Option<i32>, sqlx::Error> {
    sqlx::query!(
      r#"SELECT id FROM inscription WHERE genesis_id = $1"#,
      genesis_id
    )
    .map(|r| r.id)
    .fetch_optional(&*self.pool)
    .await
  }

  pub async fn save_inscription(
    &self,
    inscription_details: &InscriptionDetails,
    metadata: Option<String>,
  ) -> Result<i32, sqlx::Error> {
    sqlx::query!(
      r#"
      INSERT INTO inscription (
          genesis_id
        , number
        , content_type
        , content_length
        , metadata
        , genesis_block_height
        , genesis_block_time
        , sat_number
        , sat_rarity
        , sat_block_height
        , sat_block_time
        , fee
        , charms
        , children
        , parents
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
      ON CONFLICT (genesis_id) DO UPDATE SET
          number = EXCLUDED.number
        , content_type = EXCLUDED.content_type
        , content_length = COALESCE(EXCLUDED.content_length, inscription.content_length)
        , metadata = COALESCE(EXCLUDED.metadata, inscription.metadata)
        , genesis_block_height = EXCLUDED.genesis_block_height
        , genesis_block_time = EXCLUDED.genesis_block_time
        , sat_number = COALESCE(EXCLUDED.sat_number, inscription.sat_number)
        , sat_rarity = COALESCE(EXCLUDED.sat_rarity, inscription.sat_rarity)
        , sat_block_height = COALESCE(EXCLUDED.sat_block_height, inscription.sat_block_height)
        , sat_block_time = COALESCE(EXCLUDED.sat_block_time, inscription.sat_block_time)
        , fee = EXCLUDED.fee
        , charms = EXCLUDED.charms
        , children = COALESCE(EXCLUDED.children, inscription.children)
        , parents = COALESCE(EXCLUDED.parents, inscription.parents)
      RETURNING id
      "#,
      inscription_details.id.to_string(),
      inscription_details.number,
      inscription_details.content_type.as_deref(),
      inscription_details
        .content_length
        .map(|n| i32::try_from(n).expect("content_length should fit in pg integer")),
      metadata,
      i32::try_from(inscription_details.genesis_block_height)
        .expect("genesis_block_height should fit in pg integer"),
      inscription_details.genesis_block_time,
      inscription_details
        .sat_number
        .map(|n| i64::try_from(n).expect("sat_number should fit in pg bigint")),
      inscription_details.sat_rarity.map(|r| r as i32),
      inscription_details
        .sat_block_height
        .map(|n| i32::try_from(n).expect("sat_block_height should fit in pg integer")),
      inscription_details.sat_block_time,
      i64::try_from(inscription_details.fee).expect("fee should fit in pg bigint"),
      i16::try_from(inscription_details.charms).expect("charts should fit in pg smallint"),
      Json(&inscription_details.children).encode_to_string(),
      Json(&inscription_details.parents).encode_to_string()
    )
    .map(|r| r.id)
    .fetch_one(&*self.pool)
    .await
  }

  pub async fn save_location(
    &self,
    id: i32,
    block_height: i32,
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
    sqlx::query!(
      r#"
      INSERT INTO location (
          inscription_id
        , block_height
        , block_time
        , tx_id
        , to_address
        , cur_output
        , cur_offset
        , from_address
        , prev_output
        , prev_offset
        , value
      )
      SELECT
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
      WHERE NOT EXISTS (
        SELECT 1 FROM location
        WHERE inscription_id = $1
          AND block_height = $2
          AND block_time = $3
          AND tx_id = $4
          AND to_address = $5
          AND cur_output = $6
          AND cur_offset = $7
          AND from_address = $8
          AND prev_output = $9
          AND prev_offset = $10
          AND value = $11
      )
      "#,
      id,
      block_height,
      i64::try_from(block_time).expect("block_time should fit in pg bigint"),
      tx_id.map(|n| n.to_string()),
      to_address,
      to_outpoint.map(|n| n.to_string()),
      to_offset.map(|n| i64::try_from(n).expect("to_offset should fit in pg bigint")),
      from_address,
      from_outpoint.map(|n| n.to_string()),
      from_offset.map(|n| i64::try_from(n).expect("from_offset should fit in pg bigint")),
      value.map(|n| i64::try_from(n).expect("value should fit in pg bigint")),
    )
    .execute(&*self.pool)
    .await?;

    Ok(())
  }
}
