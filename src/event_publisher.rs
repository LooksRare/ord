use anyhow::{Context, Result};
use lapin::options::ConfirmSelectOptions;
use lapin::tcp::{AMQPUriTcpExt, NativeTlsConnector};
use lapin::uri::AMQPUri;
use lapin::{options::BasicPublishOptions, BasicProperties, Connection, ConnectionProperties};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

use crate::index::event::Event;
use crate::settings::Settings;

pub struct EventPublisher {
  pub(crate) sender: mpsc::Sender<Event>,
}

impl EventPublisher {
  pub fn run(settings: &Settings) -> Result<Self, anyhow::Error> {
    let addr = settings
      .rabbitmq_addr()
      .context("rabbitmq amqp credentials and url must be defined")?
      .to_owned();

    let exchange = settings
      .rabbitmq_exchange()
      .context("rabbitmq exchange path must be defined")?
      .to_owned();

    let (tx, mut rx) = mpsc::channel::<Event>(128);

    std::thread::spawn(move || {
      Runtime::new().expect("runtime is setup").block_on(async {
        let uri = addr.parse::<AMQPUri>().unwrap();

        let connect = move |uri: &AMQPUri| {
          uri.connect().and_then(|stream| {
            let mut tls_builder = NativeTlsConnector::builder();
            tls_builder.danger_accept_invalid_certs(true);
            stream.into_native_tls(
              &tls_builder.build().expect("TLS configuration failed"),
              &uri.authority.host,
            )
          })
        };

        let conn = Connection::connector(uri, Box::new(connect), ConnectionProperties::default())
          .await
          .expect("connects to rabbitmq ok");

        let channel = conn
          .create_channel()
          .await
          .expect("creates rmq connection channel");

        channel
          .confirm_select(ConfirmSelectOptions::default())
          .await
          .expect("enable msg confirms");

        while let Some(event) = rx.recv().await {
          // TODO we might want to panic if rmq is down so we don't miss any messages
          //  if we miss messages only way to replay them is run `ord` instance from scratch
          //  maybe we can trigger fake reorg to force it to reindex from savepoint?
          let message = serde_json::to_vec(&event).expect("failed to serialize event");

          let publish = channel
            .basic_publish(
              &exchange,
              EventPublisher::type_name(&event),
              BasicPublishOptions::default(),
              &message,
              BasicProperties::default(),
            )
            .await
            .expect("published rmq msg")
            .await
            .expect("confirms rmq msg received");

          assert!(publish.is_ack());
        }
      })
    });

    Ok(EventPublisher { sender: tx })
  }

  fn type_name(event: &Event) -> &'static str {
    match event {
      Event::InscriptionCreated { .. } => "InscriptionCreated",
      Event::InscriptionTransferred { .. } => "InscriptionTransferred",
      Event::RuneBurned { .. } => "RuneBurned",
      Event::RuneEtched { .. } => "RuneEtched",
      Event::RuneMinted { .. } => "RuneMinted",
      Event::RuneTransferred { .. } => "RuneTransferred",
      Event::BlockCommitted { .. } => "BlockCommitted",
    }
  }
}
