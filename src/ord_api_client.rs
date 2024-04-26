use anyhow::Context;
use reqwest::Client;

use crate::api::Inscription;
use crate::settings::Settings;
use crate::InscriptionId;

pub struct OrdApiClient {
  ord_api_url: String,
  client: Client,
}

impl OrdApiClient {
  pub fn run(settings: &Settings) -> anyhow::Result<Self, anyhow::Error> {
    let ord_api_url = settings
      .ord_api_url()
      .context("ord api url must be defined")?
      .to_owned();

    let client = Client::builder()
      .timeout(std::time::Duration::from_secs(30))
      // TODO add retries
      // .timeout_retry_times(5)
      // .timeout_retry_strategy(ExponentialBackoff)
      .build()?;

    Ok(OrdApiClient {
      ord_api_url,
      client,
    })
  }

  pub async fn fetch_inscription_details(
    &self,
    inscription_id: &InscriptionId,
  ) -> Result<Inscription, reqwest::Error> {
    let response = self
      .client
      .get(format!(
        "{}/inscription/{}",
        self.ord_api_url, inscription_id
      ))
      .header("Accept", "application/json")
      .send()
      .await?
      .error_for_status()?
      .json::<Inscription>()
      .await?;
    Ok(response)
  }
}
