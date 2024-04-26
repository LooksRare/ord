use crate::event_publisher::EventPublisher;
use crate::ord_api_client::OrdApiClient;
use crate::ord_db_client::OrdDbClient;

use super::*;

pub mod balances;
pub mod decode;
pub mod env;
pub mod epochs;
pub mod find;
pub mod index;
pub mod list;
pub mod parse;
pub mod runes;
pub(crate) mod server;
mod settings;
pub mod subsidy;
pub mod supply;
pub mod teleburn;
pub mod traits;
pub mod wallet;

#[derive(Debug, Parser)]
pub(crate) enum Subcommand {
  #[command(about = "List all rune balances")]
  Balances,
  #[command(about = "Decode a transaction")]
  Decode(decode::Decode),
  #[command(about = "Start a regtest ord and bitcoind instance")]
  Env(env::Env),
  #[command(about = "List the first satoshis of each reward epoch")]
  Epochs,
  #[command(about = "Find a satoshi's current location")]
  Find(find::Find),
  #[command(subcommand, about = "Index commands")]
  Index(index::IndexSubcommand),
  #[command(about = "List the satoshis in an output")]
  List(list::List),
  #[command(about = "Parse a satoshi from ordinal notation")]
  Parse(parse::Parse),
  #[command(about = "List all runes")]
  Runes,
  #[command(about = "Run the explorer server")]
  Server(server::Server),
  #[command(about = "Run the explorer server in event emit mode")]
  EventServer(server::Server),
  #[command(about = "Run the index event consumer")]
  EventConsumer(event_consumer::EventConsumer),
  #[command(about = "Display settings")]
  Settings,
  #[command(about = "Display information about a block's subsidy")]
  Subsidy(subsidy::Subsidy),
  #[command(about = "Display Bitcoin supply information")]
  Supply,
  #[command(about = "Generate teleburn addresses")]
  Teleburn(teleburn::Teleburn),
  #[command(about = "Display satoshi traits")]
  Traits(traits::Traits),
  #[command(about = "Wallet commands")]
  Wallet(wallet::WalletCommand),
}

impl Subcommand {
  pub(crate) fn run(self, settings: Settings) -> SubcommandResult {
    match self {
      Self::Balances => balances::run(settings),
      Self::Decode(decode) => decode.run(settings),
      Self::Env(env) => env.run(),
      Self::Epochs => epochs::run(),
      Self::Find(find) => find.run(settings),
      Self::Index(index) => index.run(settings),
      Self::List(list) => list.run(settings),
      Self::Parse(parse) => parse.run(),
      Self::Runes => runes::run(settings),
      Self::Server(server) => {
        let index = Arc::new(Index::open(&settings)?);
        let handle = axum_server::Handle::new();
        LISTENERS.lock().unwrap().push(handle.clone());
        server.run(settings, index, handle)
      }
      Self::EventServer(server) => {
        let publisher = EventPublisher::run(&settings)?;
        let handle = axum_server::Handle::new();
        let index = Arc::new(Index::open_with_event_sender(
          &settings,
          Some(publisher.sender.clone()),
        )?);

        LISTENERS.lock().unwrap().push(handle.clone());
        server.run(settings, index, handle)
      }
      Self::EventConsumer(event_consumer) => {
        event_consumer.run(&settings)
      }
      Self::Settings => settings::run(settings),
      Self::Subsidy(subsidy) => subsidy.run(),
      Self::Supply => supply::run(),
      Self::Teleburn(teleburn) => teleburn.run(),
      Self::Traits(traits) => traits.run(),
      Self::Wallet(wallet) => wallet.run(settings),
    }
  }
}

pub trait Output: Send {
  fn print_json(&self, minify: bool);
}

impl<T> Output for T
where
  T: Serialize + Send,
{
  fn print_json(&self, minify: bool) {
    if minify {
      serde_json::to_writer(io::stdout(), self).ok();
    } else {
      serde_json::to_writer_pretty(io::stdout(), self).ok();
    }
    println!();
  }
}

pub(crate) type SubcommandResult = Result<Option<Box<dyn Output>>>;
