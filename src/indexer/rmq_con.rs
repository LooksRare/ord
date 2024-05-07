use std::collections::HashSet;

use anyhow::Context;
use chrono::Utc;
use lapin::options::ConfirmSelectOptions;
use lapin::tcp::{AMQPUriTcpExt, NativeTlsConnector};
use lapin::uri::AMQPUri;
use lapin::{Connection, ConnectionProperties};
use rand::distributions::{Alphanumeric, DistString};

pub async fn setup_rabbitmq_connection(addr: &str) -> Result<lapin::Channel, anyhow::Error> {
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
  let uri = addr
    .parse::<AMQPUri>()
    .map_err(anyhow::Error::msg)
    .context("Failed to parse AMQP URI")?;
  let host = uri.authority.host.clone();

  let non_tls_hosts: HashSet<String> = vec!["localhost".to_string()].into_iter().collect();

  let is_secure = !non_tls_hosts.contains(&host);

  if is_secure {
    let connect = move |uri: &AMQPUri| {
      let conn = uri.connect().and_then(|stream| {
        let mut tls_builder = NativeTlsConnector::builder();
        tls_builder.danger_accept_invalid_certs(true);
        let connector = &tls_builder.build().expect("TLS configuration failed");
        stream.into_native_tls(connector, &uri.authority.host)
      });
      conn
    };
    Connection::connector(uri, Box::new(connect), ConnectionProperties::default())
      .await
      .context("Failed to establish a secure AMQP connection")
  } else {
    Connection::connect(addr, ConnectionProperties::default())
      .await
      .context("Failed to establish an unsecure AMQP connection")
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
