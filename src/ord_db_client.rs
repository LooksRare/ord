use sqlx::PgPool;

use crate::api::Inscription;

pub struct OrdDbClient {
  pool: PgPool,
}

impl OrdDbClient {
  pub async fn run(database_url: &str) -> anyhow::Result<Self, anyhow::Error> {
    let pool = PgPool::connect(database_url)
      .await
      .expect("connects to db ok");

    Ok(OrdDbClient { pool })
  }

  pub async fn save_inscription(&self, inscription: Inscription) -> Result<(), sqlx::Error> {
    log::info!("Saving inscription detailed response: {:?}", inscription);

    let genesis_id = format!("{}i{}", inscription.id.txid, inscription.id.index);
    let parents = inscription
      .parents
      .iter()
      .map(|pid| format!("{}i{}", pid.txid, pid.index))
      .collect::<Vec<_>>()
      .join(",");

    let sql = "
        INSERT INTO inscriptions (
            genesis_id,
            number,
            sat_ordinal,
            sat_rarity,
            sat_coinbase_height,
            mime_type,
            content_type,
            content_length,
            content,
            fee,
            curse_type,
            classic_number,
            metadata,
            parent
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
    ";

    sqlx::query(sql)
      .bind(genesis_id)
      .bind(inscription.number as i64)
      .bind(inscription.sat.map_or(0, |s| s.0 as i64))
      .bind("")
      .bind(inscription.height as i64)
      .bind(
        inscription
          .effective_content_type
          .clone()
          .unwrap_or_default(),
      )
      .bind(inscription.content_type.clone().unwrap_or_default())
      .bind(inscription.content_length.map_or(0, |len| len as i64))
      .bind(vec![] as Vec<u8>)
      .bind(inscription.fee as i64)
      .bind("")
      .bind(0)
      .bind("")
      .bind(parents)
      .execute(&self.pool)
      .await?;

    Ok(())
  }
}
