// Copyright (C) 2019-2024 The apca Developers
// SPDX-License-Identifier: GPL-3.0-or-later

#![allow(clippy::let_unit_value, clippy::unreadable_literal)]

//! A crate for interacting with the [Alpaca
//! API](https://alpaca.markets/docs/).
//!
//! The crate provides:
//! - [`trading`] — Generated client for the Alpaca Trading API
//! - [`market_data`] — Generated client for the Alpaca Market Data API
//! - [`streaming`] — WebSocket streaming for real-time market data
//!   (stocks and crypto)
//! - [`order_updates`] — WebSocket streaming for order status updates
//!
//! # Quick Start
//!
//! ```no_run
//! use apca::ApiInfo;
//! use apca::Client;
//!
//! let api_info = ApiInfo::from_env().unwrap();
//! let client = Client::new(api_info);
//!
//! # tokio::runtime::Runtime::new().unwrap().block_on(async move {
//! // REST: query account info
//! let account = client.trading().get_account().await.unwrap();
//! println!("buying power: {}", account.into_inner().buying_power.unwrap_or_default());
//!
//! // WebSocket: stream real-time crypto data (available 24/7)
//! use apca::streaming::{RealtimeData, CryptoUs, MarketData, drive};
//! use futures::StreamExt as _;
//! use futures::FutureExt as _;
//!
//! let (mut stream, mut subscription) = client
//!     .subscribe::<RealtimeData<CryptoUs>>()
//!     .await
//!     .unwrap();
//!
//! let mut data = MarketData::default();
//! data.set_trades(["BTC/USD"]);
//! let sub = subscription.subscribe(&data).boxed();
//! let () = drive(sub, &mut stream).await.unwrap().unwrap().unwrap();
//! # });
//! ```

use std::borrow::Cow;

mod api_info;
mod client;
pub(crate) mod subscribable;
pub(crate) mod websocket;

/// Generated client and types for the Alpaca Trading API.
#[allow(
    unused_imports,
    clippy::all,
    missing_docs,
    unreachable_pub,
    rustdoc::broken_intra_doc_links
)]
pub mod trading {
    include!(concat!(env!("OUT_DIR"), "/trading_api.rs"));
}

/// Generated client and types for the Alpaca Market Data API.
#[allow(
    unused_imports,
    clippy::all,
    missing_docs,
    unreachable_pub,
    rustdoc::broken_intra_doc_links
)]
pub mod market_data {
    include!(concat!(env!("OUT_DIR"), "/market_data_api.rs"));
}

/// Real-time market data streaming over WebSocket.
pub mod streaming;

/// Real-time order update streaming over WebSocket.
pub mod order_updates;

pub use crate::api_info::ApiInfo;
pub use crate::client::Client;
pub use crate::subscribable::Subscribable;

type Str = Cow<'static, str>;

/// Common error type for WebSocket and connection errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A URL parsing error.
    #[error("failed to parse URL: {0}")]
    Url(#[from] url::ParseError),
    /// A WebSocket error.
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] websocket_util::tungstenite::Error),
    /// A JSON (de)serialization error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    /// A generic string error.
    #[error("{0}")]
    Str(Str),
}
