use std::time::Duration;

use anyhow::anyhow;
use bitcoin::Txid;
use http::StatusCode;
use reqwest::{Client, RequestBuilder};
use tokio::time::sleep;

use crate::api::{BlockInfo, InscriptionDetails, Transaction};

pub struct OrdApiClient {
  ord_api_url: String,
  client: Client,
}

impl OrdApiClient {
  pub fn new(ord_api_url: String) -> anyhow::Result<Self, anyhow::Error> {
    let client = Client::builder()
      .timeout(std::time::Duration::from_secs(30))
      .build()?;

    Ok(OrdApiClient {
      ord_api_url,
      client,
    })
  }

  async fn execute_with_retries<T>(
    &self,
    request_builder: RequestBuilder,
    max_attempts: u32,
  ) -> Result<T, anyhow::Error>
  where
    T: for<'de> serde::Deserialize<'de> + 'static,
  {
    let mut attempts = 0;
    let mut delay = Duration::from_secs(1);

    while attempts < max_attempts {
      let mut request = request_builder
        .try_clone()
        .ok_or_else(|| anyhow!("Failed to clone request"))?;

      let response = request.send().await;

      match response {
        Ok(resp) => match resp.error_for_status() {
          Ok(valid_response) => {
            return valid_response
              .json::<T>()
              .await
              .map_err(anyhow::Error::from);
          }
          Err(e)
            if e.status() == Some(StatusCode::TOO_MANY_REQUESTS)
              || e
                .status()
                .map_or_else(|| false, |status_code| status_code.is_server_error()) =>
          {
            attempts += 1;
            sleep(delay).await;
            delay *= 2;
          }
          Err(e) => return Err(anyhow!(e)),
        },
        Err(e) => {
          attempts += 1;
          sleep(delay).await;
          delay *= 2;
        }
      }
    }

    Err(anyhow!("Exceeded maximum retry attempts"))
  }

  pub async fn fetch_inscription_details(
    &self,
    inscription_id: String,
  ) -> Result<InscriptionDetails, anyhow::Error> {
    let request_builder = self
      .client
      .get(format!(
        "{}/inscription/{}",
        self.ord_api_url, inscription_id
      ))
      .header("Accept", "application/json");

    self.execute_with_retries(request_builder, 3).await
  }

  pub async fn fetch_tx(&self, tx_id: Txid) -> Result<Transaction, anyhow::Error> {
    let request_builder = self
      .client
      .get(format!("{}/tx/{}", self.ord_api_url, tx_id))
      .header("Accept", "application/json");

    self.execute_with_retries(request_builder, 3).await
  }

  pub async fn fetch_block_info(&self, block_height: u32) -> Result<BlockInfo, anyhow::Error> {
    let request_builder = self
      .client
      .get(format!("{}/r/blockinfo/{}", self.ord_api_url, block_height))
      .header("Accept", "application/json");

    self.execute_with_retries(request_builder, 3).await
  }
}
