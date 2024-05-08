use std::time::Duration;

use anyhow::{Context, Error};
use chrono::Utc;
use lapin::options::ConfirmSelectOptions;
use lapin::tcp::{AMQPUriTcpExt, NativeTlsConnector};
use lapin::uri::AMQPUri;
use lapin::{Connection, ConnectionProperties};
use rand::distributions::{Alphanumeric, DistString};
use tokio::time::sleep;

pub async fn setup_rabbitmq_connection(addr: &str) -> Result<lapin::Channel, anyhow::Error> {
  let mut attempts = 10;
  let mut backoff_delay = Duration::from_secs(1);
  while attempts > 0 {
    match create_channel(addr).await {
      Ok(channel) => return Ok(channel),
      Err(e) => {
        attempts -= 1;
        if attempts < 0 {
          return Err(e);
        } else {
          log::error!(
            "Error creating RabbitMQ channel: {}. Retries left: {}. Retrying in {} seconds.",
            e,
            attempts,
            backoff_delay.as_secs()
          );
          sleep(backoff_delay).await;
          backoff_delay *= 2;
        }
      }
    }
  }
  Err(anyhow::Error::new(std::io::Error::new(
    std::io::ErrorKind::Other,
    "Failed to establish RabbitMQ channel after retries",
  )))
}

async fn create_channel(addr: &str) -> Result<lapin::Channel, anyhow::Error> {
  let conn = connect_to_rabbitmq(addr).await?;
  let channel = conn
    .create_channel()
    .await
    .context("creates rmq connection channel")?;
  channel
    .confirm_select(ConfirmSelectOptions::default())
    .await
    .context("enable msg confirms")?;
  Ok(channel)
}

async fn connect_to_rabbitmq(addr: &str) -> Result<Connection, anyhow::Error> {
  let opt = ConnectionProperties::default();
  let uri = addr
    .parse::<AMQPUri>()
    .map_err(Error::msg)
    .context("failed to parse ampq uri")?;

  match uri.authority.host.as_str() {
    "localhost" => Connection::connect(addr, opt)
      .await
      .context("failed to establish an unsecure ampq connection"),

    _ => {
      let connect = move |uri: &AMQPUri| {
        uri.connect().and_then(|stream| {
          let mut tls_builder = NativeTlsConnector::builder();
          tls_builder.danger_accept_invalid_certs(true);
          let connector = &tls_builder.build().expect("tls configuration failed");
          stream.into_native_tls(connector, &uri.authority.host)
        })
      };

      Connection::connector(uri, Box::new(connect), opt)
        .await
        .context("failed to establish a secure ampq connection")
    }
  }
}

// TODO get pod name from k8s?
pub fn generate_consumer_tag(prefix: &str) -> String {
  let timestamp = Utc::now().format("%Y%m%d%H%M%S");
  format!(
    "{}-{}-{}",
    prefix,
    timestamp,
    Alphanumeric.sample_string(&mut rand::thread_rng(), 16)
  )
}
