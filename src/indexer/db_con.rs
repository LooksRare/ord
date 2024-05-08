use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};
use urlencoding::encode;

pub async fn setup_db_connection(database_url: &str) -> Result<Pool<Postgres>, anyhow::Error> {
  let encoded_database_url = encode_password_in_url(database_url);
  let pool = PgPoolOptions::new()
    .max_connections(5)
    .connect(encoded_database_url.as_ref())
    .await?;
  Ok(pool)
}

fn encode_password_in_url(url: &str) -> String {
  let re = regex::Regex::new(r"(\w+://)([^:]+):([^@]+)@(.*)").unwrap();
  if let Some(caps) = re.captures(url) {
    let protocol_and_user = caps.get(1).map_or("", |m| m.as_str());
    let username = caps.get(2).map_or("", |m| m.as_str());
    let password = caps.get(3).map_or("", |m| m.as_str());
    let rest_of_url = caps.get(4).map_or("", |m| m.as_str());
    format!(
      "{}{}:{}@{}",
      protocol_and_user,
      username,
      encode(password),
      rest_of_url
    )
  } else {
    url.to_string()
  }
}
