use reqwest::Client;

use crate::api::Inscription;
use crate::InscriptionId;

pub struct OrdApiClient {
  ord_api_url: String,
  client: Client,
}

impl OrdApiClient {
  pub fn run(ord_api_url: String) -> anyhow::Result<Self, anyhow::Error> {
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
