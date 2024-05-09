use anyhow::{Context, Error};
use chrono::Utc;
use lapin::message::Delivery;
use lapin::options::{BasicPublishOptions, ConfirmSelectOptions};
use lapin::publisher_confirm::Confirmation;
use lapin::tcp::{AMQPUriTcpExt, NativeTlsConnector};
use lapin::types::{FieldTable, ShortUInt};
use lapin::uri::AMQPUri;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};
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

pub async fn republish_to_queue(
  channel: &Channel,
  delivery: &Delivery,
  delivery_count: &ShortUInt,
) -> lapin::Result<Confirmation> {
  let mut new_headers = delivery
    .properties
    .headers()
    .as_ref()
    .cloned()
    .unwrap_or_else(FieldTable::default);
  new_headers.insert(
    "x-delivery-count".into(),
    ShortUInt::from(delivery_count + 1).into(),
  );

  channel
    .basic_publish(
      "",
      delivery.routing_key.as_str(),
      BasicPublishOptions::default(),
      &delivery.data,
      BasicProperties::default().with_headers(new_headers),
    )
    .await?
    .await
}
