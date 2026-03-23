// Copyright (C) 2019-2024 The apca Developers
// SPDX-License-Identifier: GPL-3.0-or-later

use std::env::var_os;
use std::ffi::OsString;

use url::Url;

use crate::Error;

/// Default Trading API base URL (paper trading).
pub const API_BASE_URL: &str = "https://paper-api.alpaca.markets";
/// Default Market Data API base URL.
pub const DATA_BASE_URL: &str = "https://data.alpaca.markets";
/// Default WebSocket base URL for market data streaming.
pub const DATA_STREAM_BASE_URL: &str = "wss://stream.data.alpaca.markets";

const ENV_API_BASE_URL: &str = "APCA_API_BASE_URL";
const ENV_API_STREAM_URL: &str = "APCA_API_STREAM_URL";
const ENV_KEY_ID: &str = "APCA_API_KEY_ID";
const ENV_SECRET: &str = "APCA_API_SECRET_KEY";

fn make_api_stream_url(base_url: Url) -> Result<Url, Error> {
  let mut url = base_url;
  url.set_scheme("wss").map_err(|()| {
    Error::Str(format!("unable to change URL scheme for {url}: invalid URL?").into())
  })?;
  url.set_path("stream");
  Ok(url)
}

/// Configuration for connecting to the Alpaca API.
///
/// Contains API credentials and base URLs for both REST and WebSocket endpoints.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ApiInfo {
  /// The base URL for the Trading API.
  pub api_base_url: Url,
  /// The WebSocket stream URL for order updates.
  pub api_stream_url: Url,
  /// The base URL for the Market Data REST API.
  pub data_base_url: Url,
  /// The WebSocket base URL for streaming market data.
  pub data_stream_base_url: Url,
  /// The API key ID for authentication.
  pub key_id: String,
  /// The API secret for authentication.
  pub secret: String,
}

impl ApiInfo {
  /// Create an `ApiInfo` from explicit parts.
  ///
  /// The WebSocket URL is inferred from the base URL by changing the
  /// scheme to `wss` and appending `/stream`.
  pub fn from_parts(
    api_base_url: impl AsRef<str>,
    key_id: impl ToString,
    secret: impl ToString,
  ) -> Result<Self, Error> {
    let api_base_url = Url::parse(api_base_url.as_ref())?;
    let api_stream_url = make_api_stream_url(api_base_url.clone())?;

    Ok(Self {
      api_base_url,
      api_stream_url,
      data_base_url: Url::parse(DATA_BASE_URL).unwrap(),
      data_stream_base_url: Url::parse(DATA_STREAM_BASE_URL).unwrap(),
      key_id: key_id.to_string(),
      secret: secret.to_string(),
    })
  }

  /// Create an `ApiInfo` from environment variables.
  ///
  /// Uses:
  /// - `APCA_API_BASE_URL` (optional, defaults to paper trading)
  /// - `APCA_API_STREAM_URL` (optional, inferred from base URL)
  /// - `APCA_API_KEY_ID` (required)
  /// - `APCA_API_SECRET_KEY` (required)
  #[allow(unused_qualifications)]
  pub fn from_env() -> Result<Self, Error> {
    let api_base_url = var_os(ENV_API_BASE_URL)
      .unwrap_or_else(|| OsString::from(API_BASE_URL))
      .into_string()
      .map_err(|_| {
        Error::Str(format!("{ENV_API_BASE_URL} environment variable is not a valid string").into())
      })?;
    let api_base_url = Url::parse(&api_base_url)?;

    let api_stream_url = var_os(ENV_API_STREAM_URL)
      .map(Result::<_, Error>::Ok)
      .unwrap_or_else(|| {
        let url = make_api_stream_url(api_base_url.clone())?;
        Ok(OsString::from(url.as_str()))
      })?
      .into_string()
      .map_err(|_| {
        Error::Str(
          format!("{ENV_API_STREAM_URL} environment variable is not a valid string").into(),
        )
      })?;
    let api_stream_url = Url::parse(&api_stream_url)?;

    let key_id = var_os(ENV_KEY_ID)
      .ok_or_else(|| Error::Str(format!("{ENV_KEY_ID} environment variable not found").into()))?
      .into_string()
      .map_err(|_| {
        Error::Str(format!("{ENV_KEY_ID} environment variable is not a valid string").into())
      })?;

    let secret = var_os(ENV_SECRET)
      .ok_or_else(|| Error::Str(format!("{ENV_SECRET} environment variable not found").into()))?
      .into_string()
      .map_err(|_| {
        Error::Str(format!("{ENV_SECRET} environment variable is not a valid string").into())
      })?;

    Ok(Self {
      api_base_url,
      api_stream_url,
      data_base_url: Url::parse(DATA_BASE_URL).unwrap(),
      data_stream_base_url: Url::parse(DATA_STREAM_BASE_URL).unwrap(),
      key_id,
      secret,
    })
  }

  /// Build the auth headers used by the Alpaca REST APIs.
  pub(crate) fn auth_headers(&self) -> reqwest::header::HeaderMap {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
      "APCA-API-KEY-ID",
      self.key_id.parse().expect("invalid key_id header value"),
    );
    headers.insert(
      "APCA-API-SECRET-KEY",
      self.secret.parse().expect("invalid secret header value"),
    );
    headers
  }
}


#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn from_parts() {
    let api_base_url = "https://paper-api.alpaca.markets/";
    let key_id = "XXXXXXXXXXXXXXXXXXXX";
    let secret = "YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY";

    let api_info = ApiInfo::from_parts(api_base_url, key_id, secret).unwrap();
    assert_eq!(api_info.api_base_url.as_str(), api_base_url);
    assert_eq!(api_info.key_id, key_id);
    assert_eq!(api_info.secret, secret);
  }
}
