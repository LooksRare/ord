use anyhow::{Context, Result};
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
      .context("rabbitmq amqp credentials and url must be defined")?;

    let exchange = settings
      .rabbitmq_exchange()
      .context("rabbitmq exchange path must be defined")?
      .to_owned();

    let (tx, mut rx) = mpsc::channel::<Event>(128);

    let receiver = std::thread::spawn(move || {
      Runtime::new().expect("runtime is setup").block_on(async {
        let conn = Connection::connect(&addr, ConnectionProperties::default())
          .await
          .expect("connects to rabbitmq ok");

        let channel = conn
          .create_channel()
          .await
          .expect("creates rmq connection channel");

        while let Some(event) = rx.recv().await {
          let message = serde_json::to_vec(&event).expect("failed to serialize event");

          channel
            .basic_publish(
              &exchange,
              "",
              BasicPublishOptions::default(),
              &message,
              BasicProperties::default(),
            )
            .await
            .expect("published message to rmq");
        }
      })
    });

    receiver.join().expect("spawn blocking event rx thread");

    Ok(EventPublisher { sender: tx })
  }
}
