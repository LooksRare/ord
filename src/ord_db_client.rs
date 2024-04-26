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
    // let query = sqlx::query!(
    //         "INSERT INTO inscriptions (genesis_id, number, sat_ordinal, sat_rarity, sat_coinbase_height, mime_type, content_type, content_length, content, fee, curse_type, classic_number, metadata, parent) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    //         ??
    //     )
    //   .execute(&self.pool)
    //   .await?;
    Ok(())
  }
}
