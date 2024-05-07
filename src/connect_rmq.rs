use anyhow::{Context, Error};
use lapin::tcp::{AMQPUriTcpExt, NativeTlsConnector};
use lapin::uri::AMQPUri;
use lapin::{Connection, ConnectionProperties};

pub async fn connect_to_rabbitmq(addr: &str) -> Result<Connection, anyhow::Error> {
  let opt = ConnectionProperties::default();
  let uri = addr
    .parse::<AMQPUri>()
    .map_err(Error::msg)
    .context("failed to parse AMQP URI")?;

  match uri.authority.host.as_str() {
    "localhost" => Connection::connect(addr, opt)
      .await
      .context("failed to establish an unsecure AMQP connection"),

    _ => {
      let connect = move |uri: &AMQPUri| {
        uri.connect().and_then(|stream| {
          let mut tls_builder = NativeTlsConnector::builder();
          tls_builder.danger_accept_invalid_certs(true);
          let connector = &tls_builder.build().expect("TLS configuration failed");
          stream.into_native_tls(connector, &uri.authority.host)
        })
      };

      Connection::connector(uri, Box::new(connect), opt)
        .await
        .context("failed to establish a secure AMQP connection")
    }
  }
}
