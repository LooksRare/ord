use anyhow::{Context, Result};
use lapin::options::ConfirmSelectOptions;
use lapin::{options::BasicPublishOptions, BasicProperties};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

use crate::connect_rmq::connect_to_rabbitmq;
use crate::index::event::Event;
use crate::settings::Settings;
use crate::shutdown_process;

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

    let (tx, rx) = mpsc::channel::<Event>(128);

    std::thread::spawn(move || {
      Runtime::new().expect("runtime is setup").block_on(async {
        match EventPublisher::consume_channel(addr, exchange, rx).await {
          Ok(_) => log::info!("Channel closed."),
          Err(e) => {
            log::error!("Fatal error publishing to RMQ, exiting {}", e);
            shutdown_process();
          }
        }
      })
    });

    Ok(EventPublisher { sender: tx })
  }

  async fn consume_channel(
    addr: String,
    exchange: String,
    mut rx: mpsc::Receiver<Event>,
  ) -> Result<()> {
    let conn = connect_to_rabbitmq(&addr).await?;

    let channel = conn.create_channel().await?;

    channel
      .confirm_select(ConfirmSelectOptions::default())
      .await?;

    while let Some(event) = rx.recv().await {
      let message = serde_json::to_vec(&event)?;

      let publish = channel
        .basic_publish(
          &exchange,
          EventPublisher::type_name(&event),
          BasicPublishOptions::default(),
          &message,
          BasicProperties::default(),
        )
        .await?
        .await?;

      if !publish.is_ack() {
        return Err(anyhow::Error::new(std::io::Error::new(
          std::io::ErrorKind::Other,
          "Message was not acknowledged",
        )));
      }
    }
    Ok(())
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
