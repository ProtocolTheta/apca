// Copyright (C) 2019-2024 The apca Developers
// SPDX-License-Identifier: GPL-3.0-or-later

use crate::api_info::ApiInfo;
use crate::subscribable::Subscribable;
use crate::Error;

/// The main client for interacting with Alpaca APIs.
///
/// Provides access to:
/// - REST endpoints via [`trading()`](Client::trading) and
///   [`market_data()`](Client::market_data)
/// - WebSocket streaming via [`subscribe()`](Client::subscribe)
#[derive(Debug, Clone)]
pub struct Client {
  pub(crate) api_info: ApiInfo,
  trading: crate::trading::Client,
  market_data: crate::market_data::Client,
}

impl Client {
  /// Create a new `Client` from API credentials.
  pub fn new(api_info: ApiInfo) -> Self {
    let headers = api_info.auth_headers();

    let http_client = reqwest::ClientBuilder::new()
      .default_headers(headers)
      .build()
      .expect("failed to build reqwest client");

    let trading_url = api_info.api_base_url.as_str().trim_end_matches('/');
    let trading = crate::trading::Client::new_with_client(trading_url, http_client.clone());

    let data_url = api_info.data_base_url.as_str().trim_end_matches('/');
    let market_data = crate::market_data::Client::new_with_client(data_url, http_client);

    Self {
      api_info,
      trading,
      market_data,
    }
  }

  /// Get the Trading API client.
  #[inline]
  pub fn trading(&self) -> &crate::trading::Client {
    &self.trading
  }

  /// Get the Market Data API client.
  #[inline]
  pub fn market_data(&self) -> &crate::market_data::Client {
    &self.market_data
  }

  /// Subscribe to a WebSocket stream.
  pub async fn subscribe<S>(&self) -> Result<(S::Stream, S::Subscription), Error>
  where
    S: Subscribable<Input = ApiInfo>,
  {
    S::connect(&self.api_info).await
  }

  /// Get the underlying API configuration.
  #[inline]
  pub fn api_info(&self) -> &ApiInfo {
    &self.api_info
  }
}
