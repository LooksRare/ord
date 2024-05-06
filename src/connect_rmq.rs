use std::collections::HashSet;

use anyhow::Context;
use lapin::tcp::{AMQPUriTcpExt, NativeTlsConnector};
use lapin::uri::AMQPUri;
use lapin::{Connection, ConnectionProperties};

pub async fn connect_to_rabbitmq(addr: &str) -> Result<Connection, anyhow::Error> {
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
