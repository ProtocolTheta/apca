// Copyright (C) 2021-2024 The apca Developers
// SPDX-License-Identifier: GPL-3.0-or-later

//! Real-time market data streaming over WebSocket.
//!
//! Supports stock data via IEX/SIP (v2) and crypto data via v1beta3.
//! Crypto streams are available 24/7, making them ideal for testing.

use std::borrow::Borrow as _;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Deref;

use async_trait::async_trait;

use chrono::DateTime;
use chrono::Utc;

use futures::stream::Fuse;
use futures::stream::FusedStream;
use futures::stream::Map;
use futures::stream::SplitSink;
use futures::stream::SplitStream;
use futures::Future;
use futures::FutureExt as _;
use futures::Sink;
use futures::StreamExt as _;

use num_decimal::Num;

use serde::de::DeserializeOwned;
use serde::de::Deserializer;
use serde::ser::SerializeSeq as _;
use serde::ser::Serializer;
use serde::Deserialize;
use serde::Serialize;
use serde_json::from_slice as json_from_slice;
use serde_json::from_str as json_from_str;
use serde_json::to_string as to_json;
use serde_json::Error as JsonError;

use thiserror::Error as ThisError;

use tokio::net::TcpStream;

use tungstenite::MaybeTlsStream;
use tungstenite::WebSocketStream;

use url::Url;

use websocket_util::subscribe;
use websocket_util::subscribe::MessageStream;
use websocket_util::tungstenite::Error as WebSocketError;
use websocket_util::wrap;
use websocket_util::wrap::Wrapper;

mod unfold;
use unfold::Unfold;

use crate::subscribable::Subscribable;
use crate::websocket::connect;
use crate::websocket::MessageResult;
use crate::ApiInfo;
use crate::Error;
use crate::Str;


type UserMessage<B, Q, T> = <ParsedMessage<B, Q, T> as subscribe::Message>::UserMessage;

/// Drive a [`Subscription`] future to completion while polling the stream.
#[inline]
pub async fn drive<F, S, B, Q, T>(
  future: F,
  stream: &mut S,
) -> Result<F::Output, UserMessage<B, Q, T>>
where
  F: Future + Unpin,
  S: FusedStream<Item = UserMessage<B, Q, T>> + Unpin,
{
  subscribe::drive::<ParsedMessage<B, Q, T>, _, _>(future, stream).await
}


mod private {
  pub trait Sealed {}
}

#[doc(hidden)]
#[derive(Clone, Debug)]
pub enum SourceVariant {
  PathComponent(&'static str),
  Url(String),
}

/// A trait representing the data source to stream from.
pub trait Source: private::Sealed {
  #[doc(hidden)]
  fn source() -> SourceVariant;
}


// --- Stock Sources ---

/// Use the Investors Exchange (IEX) as the data source.
/// Available with the free plan.
#[derive(Clone, Copy, Debug)]
pub enum IEX {}

impl Source for IEX {
  #[inline]
  fn source() -> SourceVariant {
    SourceVariant::PathComponent("iex")
  }
}

impl private::Sealed for IEX {}


/// Use CTA/UTP SIPs as the data source.
/// Requires the unlimited market data plan.
#[derive(Clone, Copy, Debug)]
pub enum SIP {}

impl Source for SIP {
  #[inline]
  fn source() -> SourceVariant {
    SourceVariant::PathComponent("sip")
  }
}

impl private::Sealed for SIP {}


// --- Crypto Sources ---

/// Stream crypto data from Alpaca US (`v1beta3/crypto/us`).
/// Available 24/7.
#[derive(Clone, Copy, Debug)]
pub enum CryptoUs {}

impl Source for CryptoUs {
  #[inline]
  fn source() -> SourceVariant {
    SourceVariant::PathComponent("v1beta3/crypto/us")
  }
}

impl private::Sealed for CryptoUs {}


/// Stream crypto data from Kraken US (`v1beta3/crypto/us-1`).
/// Available 24/7.
#[derive(Clone, Copy, Debug)]
pub enum CryptoUs1 {}

impl Source for CryptoUs1 {
  #[inline]
  fn source() -> SourceVariant {
    SourceVariant::PathComponent("v1beta3/crypto/us-1")
  }
}

impl private::Sealed for CryptoUs1 {}


/// Stream crypto data from Kraken EU (`v1beta3/crypto/eu-1`).
/// Available 24/7.
#[derive(Clone, Copy, Debug)]
pub enum CryptoEu1 {}

impl Source for CryptoEu1 {
  #[inline]
  fn source() -> SourceVariant {
    SourceVariant::PathComponent("v1beta3/crypto/eu-1")
  }
}

impl private::Sealed for CryptoEu1 {}


/// A data source using a custom URL (must follow the v2/v1beta3 protocol).
#[derive(Clone, Copy, Debug)]
pub struct CustomUrl<URL> {
  _phantom: PhantomData<URL>,
}

impl<URL> Source for CustomUrl<URL>
where
  URL: Default + ToString,
{
  #[inline]
  fn source() -> SourceVariant {
    let url = URL::default();
    SourceVariant::Url(url.to_string())
  }
}

impl<URL> private::Sealed for CustomUrl<URL> {}


/// A symbol.
pub type Symbol = Str;


fn is_normalized(symbols: &[Symbol]) -> bool {
  #[inline]
  fn check<'a>(last: &'a mut &'a Symbol) -> impl FnMut(&'a Symbol) -> bool + 'a {
    move |curr| {
      if let Some(Ordering::Greater) | None = PartialOrd::partial_cmp(last, &curr) {
        return false
      }
      *last = curr;
      true
    }
  }

  let mut it = symbols.iter();
  let mut last = match it.next() {
    Some(e) => e,
    None => return true,
  };

  it.all(check(&mut last))
}

fn normalize(symbols: Cow<'static, [Symbol]>) -> Cow<'static, [Symbol]> {
  fn normalize_now(symbols: Cow<'static, [Symbol]>) -> Cow<'static, [Symbol]> {
    let mut symbols = symbols.into_owned();
    symbols.sort_by(|x, y| x.partial_cmp(y).unwrap());
    symbols.dedup();
    Cow::from(symbols)
  }

  if !is_normalized(&symbols) {
    let symbols = normalize_now(symbols);
    debug_assert!(is_normalized(&symbols));
    symbols
  } else {
    symbols
  }
}


// --- Data Types ---

/// Aggregate bar data (works for both stocks and crypto).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Bar {
  #[serde(rename = "S")]
  pub symbol: String,
  #[serde(rename = "o")]
  pub open_price: Num,
  #[serde(rename = "h")]
  pub high_price: Num,
  #[serde(rename = "l")]
  pub low_price: Num,
  #[serde(rename = "c")]
  pub close_price: Num,
  #[serde(rename = "v")]
  pub volume: Num,
  #[serde(rename = "t")]
  pub timestamp: DateTime<Utc>,
  /// Trade count (present in crypto bars).
  #[serde(rename = "n", default, skip_serializing_if = "Option::is_none")]
  pub trade_count: Option<u64>,
  /// Volume-weighted average price (present in crypto bars).
  #[serde(rename = "vw", default, skip_serializing_if = "Option::is_none")]
  pub vwap: Option<Num>,
}


/// A quote (works for both stocks and crypto).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Quote {
  #[serde(rename = "S")]
  pub symbol: String,
  #[serde(rename = "bp")]
  pub bid_price: Num,
  #[serde(rename = "bs")]
  pub bid_size: Num,
  #[serde(rename = "ap")]
  pub ask_price: Num,
  #[serde(rename = "as")]
  pub ask_size: Num,
  #[serde(rename = "t")]
  pub timestamp: DateTime<Utc>,
}


/// A trade (works for both stocks and crypto).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Trade {
  #[serde(rename = "S")]
  pub symbol: String,
  #[serde(rename = "i")]
  pub trade_id: u64,
  #[serde(rename = "p")]
  pub trade_price: Num,
  #[serde(rename = "s")]
  pub trade_size: Num,
  #[serde(rename = "t")]
  pub timestamp: DateTime<Utc>,
  /// Taker side: `"B"` for buyer, `"S"` for seller (crypto only).
  #[serde(rename = "tks", default, skip_serializing_if = "Option::is_none")]
  pub taker_side: Option<String>,
}


/// A single orderbook entry (price + size).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OrderbookEntry {
  /// Price level.
  #[serde(rename = "p")]
  pub price: Num,
  /// Size at this price level (0 means removal).
  #[serde(rename = "s")]
  pub size: Num,
}


/// An orderbook update (crypto only, tag `"o"`).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Orderbook {
  #[serde(rename = "S")]
  pub symbol: String,
  #[serde(rename = "t")]
  pub timestamp: DateTime<Utc>,
  /// Bid entries.
  #[serde(rename = "b", default)]
  pub bids: Vec<OrderbookEntry>,
  /// Ask entries.
  #[serde(rename = "a", default)]
  pub asks: Vec<OrderbookEntry>,
  /// If `true`, this is a full orderbook reset.
  #[serde(rename = "r", default)]
  pub reset: bool,
}


/// An error reported by the Alpaca Stream API.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ThisError)]
#[error("{message} ({code})")]
pub struct StreamApiError {
  #[serde(rename = "code")]
  pub code: u64,
  #[serde(rename = "msg")]
  pub message: String,
}


/// A raw message received over the WebSocket.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[doc(hidden)]
#[serde(tag = "T")]
#[allow(clippy::large_enum_variant)]
pub enum DataMessage<B = Bar, Q = Quote, T = Trade> {
  /// Minute bar.
  #[serde(rename = "b")]
  Bar(B),
  /// Quote.
  #[serde(rename = "q")]
  Quote(Q),
  /// Trade.
  #[serde(rename = "t")]
  Trade(T),
  /// Daily bar (crypto).
  #[serde(rename = "d")]
  DailyBar(B),
  /// Updated/corrected bar (crypto).
  #[serde(rename = "u")]
  UpdatedBar(B),
  /// Orderbook update (crypto).
  #[serde(rename = "o")]
  Orderbook(Orderbook),
  /// Subscription confirmation.
  #[serde(rename = "subscription")]
  Subscription(MarketData),
  /// Success message.
  #[serde(rename = "success")]
  Success,
  /// Error message.
  #[serde(rename = "error")]
  Error(StreamApiError),
}


/// A data item yielded from the stream.
#[derive(Debug)]
#[non_exhaustive]
pub enum Data<B = Bar, Q = Quote, T = Trade> {
  /// A minute bar.
  Bar(B),
  /// A quote.
  Quote(Q),
  /// A trade.
  Trade(T),
  /// A daily bar (crypto).
  DailyBar(B),
  /// An updated/corrected bar (crypto).
  UpdatedBar(B),
  /// An orderbook update (crypto).
  Orderbook(Orderbook),
}

impl<B, Q, T> Data<B, Q, T> {
  pub fn is_bar(&self) -> bool {
    matches!(self, Self::Bar(..))
  }

  pub fn is_quote(&self) -> bool {
    matches!(self, Self::Quote(..))
  }

  pub fn is_trade(&self) -> bool {
    matches!(self, Self::Trade(..))
  }

  pub fn is_daily_bar(&self) -> bool {
    matches!(self, Self::DailyBar(..))
  }

  pub fn is_updated_bar(&self) -> bool {
    matches!(self, Self::UpdatedBar(..))
  }

  pub fn is_orderbook(&self) -> bool {
    matches!(self, Self::Orderbook(..))
  }
}


#[derive(Debug)]
#[doc(hidden)]
pub enum ControlMessage {
  Subscription(MarketData),
  Success,
  Error(StreamApiError),
}


type ParsedMessage<B, Q, T> =
  MessageResult<Result<DataMessage<B, Q, T>, JsonError>, WebSocketError>;

impl<B, Q, T> subscribe::Message for ParsedMessage<B, Q, T> {
  type UserMessage = Result<Result<Data<B, Q, T>, JsonError>, WebSocketError>;
  type ControlMessage = ControlMessage;

  fn classify(self) -> subscribe::Classification<Self::UserMessage, Self::ControlMessage> {
    match self {
      MessageResult::Ok(Ok(message)) => match message {
        DataMessage::Bar(bar) => subscribe::Classification::UserMessage(Ok(Ok(Data::Bar(bar)))),
        DataMessage::Quote(quote) => {
          subscribe::Classification::UserMessage(Ok(Ok(Data::Quote(quote))))
        },
        DataMessage::Trade(trade) => {
          subscribe::Classification::UserMessage(Ok(Ok(Data::Trade(trade))))
        },
        DataMessage::DailyBar(bar) => {
          subscribe::Classification::UserMessage(Ok(Ok(Data::DailyBar(bar))))
        },
        DataMessage::UpdatedBar(bar) => {
          subscribe::Classification::UserMessage(Ok(Ok(Data::UpdatedBar(bar))))
        },
        DataMessage::Orderbook(ob) => {
          subscribe::Classification::UserMessage(Ok(Ok(Data::Orderbook(ob))))
        },
        DataMessage::Subscription(data) => {
          subscribe::Classification::ControlMessage(ControlMessage::Subscription(data))
        },
        DataMessage::Success => subscribe::Classification::ControlMessage(ControlMessage::Success),
        DataMessage::Error(error) => {
          subscribe::Classification::ControlMessage(ControlMessage::Error(error))
        },
      },
      MessageResult::Ok(Err(err)) => subscribe::Classification::UserMessage(Ok(Err(err))),
      MessageResult::Err(err) => subscribe::Classification::UserMessage(Err(err)),
    }
  }

  #[inline]
  fn is_error(user_message: &Self::UserMessage) -> bool {
    user_message
      .as_ref()
      .map(|result| result.is_err())
      .unwrap_or(true)
  }
}


// --- Symbol types ---

#[inline]
fn normalized_from_str<'de, D>(deserializer: D) -> Result<Cow<'static, [Symbol]>, D::Error>
where
  D: Deserializer<'de>,
{
  Cow::<'static, [Symbol]>::deserialize(deserializer).map(normalize)
}


/// A normalized list of symbols.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct SymbolList(#[serde(deserialize_with = "normalized_from_str")] Cow<'static, [Symbol]>);

impl Deref for SymbolList {
  type Target = [Symbol];

  fn deref(&self) -> &Self::Target {
    self.0.borrow()
  }
}

impl From<Cow<'static, [Symbol]>> for SymbolList {
  #[inline]
  fn from(symbols: Cow<'static, [Symbol]>) -> Self {
    Self(normalize(symbols))
  }
}

impl From<Vec<String>> for SymbolList {
  #[inline]
  fn from(symbols: Vec<String>) -> Self {
    Self(normalize(Cow::from(
      IntoIterator::into_iter(symbols)
        .map(Symbol::from)
        .collect::<Vec<_>>(),
    )))
  }
}

impl<const N: usize> From<[&'static str; N]> for SymbolList {
  #[inline]
  fn from(symbols: [&'static str; N]) -> Self {
    Self(normalize(Cow::from(
      IntoIterator::into_iter(symbols)
        .map(Symbol::from)
        .collect::<Vec<_>>(),
    )))
  }
}


mod symbols_all {
  use super::*;

  use serde::de::Error;
  use serde::de::Unexpected;

  pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<(), D::Error>
  where
    D: Deserializer<'de>,
  {
    let string = <[&str; 1]>::deserialize(deserializer)?;
    if string == ["*"] {
      Ok(())
    } else {
      Err(Error::invalid_value(
        Unexpected::Str(string[0]),
        &"the string \"*\"",
      ))
    }
  }

  pub(crate) fn serialize<S>(serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut seq = serializer.serialize_seq(Some(1))?;
    seq.serialize_element("*")?;
    seq.end()
  }
}


/// Symbols to subscribe to.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(untagged)]
pub enum Symbols {
  /// All symbols.
  #[serde(with = "symbols_all")]
  All,
  /// A specific list of symbols.
  List(SymbolList),
}

impl Symbols {
  pub fn is_empty(&self) -> bool {
    match self {
      Self::List(list) => list.is_empty(),
      Self::All => false,
    }
  }
}

impl Default for Symbols {
  fn default() -> Self {
    Self::List(SymbolList::from([]))
  }
}


/// Market data subscription specification.
///
/// For stock sources (IEX/SIP), only `bars`, `quotes`, and `trades` are relevant.
/// For crypto sources, all fields including `daily_bars`, `updated_bars`, and
/// `orderbooks` are supported.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct MarketData {
  #[serde(default)]
  pub bars: Symbols,
  #[serde(default)]
  pub quotes: Symbols,
  #[serde(default)]
  pub trades: Symbols,
  /// Daily bars (crypto only).
  #[serde(
    rename = "dailyBars",
    default,
    skip_serializing_if = "Symbols::is_empty"
  )]
  pub daily_bars: Symbols,
  /// Updated/corrected bars (crypto only).
  #[serde(
    rename = "updatedBars",
    default,
    skip_serializing_if = "Symbols::is_empty"
  )]
  pub updated_bars: Symbols,
  /// Orderbook updates (crypto only).
  #[serde(default, skip_serializing_if = "Symbols::is_empty")]
  pub orderbooks: Symbols,
}

impl MarketData {
  pub fn set_bars<S>(&mut self, symbols: S)
  where
    S: Into<SymbolList>,
  {
    self.bars = Symbols::List(symbols.into());
  }

  pub fn set_quotes<S>(&mut self, symbols: S)
  where
    S: Into<SymbolList>,
  {
    self.quotes = Symbols::List(symbols.into());
  }

  pub fn set_trades<S>(&mut self, symbols: S)
  where
    S: Into<SymbolList>,
  {
    self.trades = Symbols::List(symbols.into());
  }

  /// Set daily bar subscriptions (crypto only).
  pub fn set_daily_bars<S>(&mut self, symbols: S)
  where
    S: Into<SymbolList>,
  {
    self.daily_bars = Symbols::List(symbols.into());
  }

  /// Set updated bar subscriptions (crypto only).
  pub fn set_updated_bars<S>(&mut self, symbols: S)
  where
    S: Into<SymbolList>,
  {
    self.updated_bars = Symbols::List(symbols.into());
  }

  /// Set orderbook subscriptions (crypto only).
  pub fn set_orderbooks<S>(&mut self, symbols: S)
  where
    S: Into<SymbolList>,
  {
    self.orderbooks = Symbols::List(symbols.into());
  }
}


/// Request messages sent over the WebSocket.
#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[doc(hidden)]
#[serde(tag = "action")]
pub enum Request<'d> {
  #[serde(rename = "auth")]
  Authenticate {
    #[serde(rename = "key")]
    key_id: Cow<'d, str>,
    #[serde(rename = "secret")]
    secret: Cow<'d, str>,
  },
  #[serde(rename = "subscribe")]
  Subscribe(Cow<'d, MarketData>),
  #[serde(rename = "unsubscribe")]
  Unsubscribe(Cow<'d, MarketData>),
}


/// A subscription handle for controlling a real-time data stream.
#[derive(Debug)]
pub struct Subscription<S, B, Q, T> {
  subscription: subscribe::Subscription<S, ParsedMessage<B, Q, T>, wrap::Message>,
  subscriptions: MarketData,
}

impl<S, B, Q, T> Subscription<S, B, Q, T> {
  #[inline]
  fn new(subscription: subscribe::Subscription<S, ParsedMessage<B, Q, T>, wrap::Message>) -> Self {
    Self {
      subscription,
      subscriptions: MarketData::default(),
    }
  }
}

impl<S, B, Q, T> Subscription<S, B, Q, T>
where
  S: Sink<wrap::Message> + Unpin,
{
  async fn authenticate(
    &mut self,
    key_id: &str,
    secret: &str,
  ) -> Result<Result<(), Error>, S::Error> {
    let request = Request::Authenticate {
      key_id: key_id.into(),
      secret: secret.into(),
    };
    let json = match to_json(&request) {
      Ok(json) => json,
      Err(err) => return Ok(Err(Error::Json(err))),
    };
    let message = wrap::Message::Text(json);
    let response = self.subscription.send(message).await?;

    match response {
      Some(response) => match response {
        Ok(ControlMessage::Success) => Ok(Ok(())),
        Ok(ControlMessage::Subscription(..)) => Ok(Err(Error::Str(
          "server responded with unexpected subscription message".into(),
        ))),
        Ok(ControlMessage::Error(error)) => Ok(Err(Error::Str(
          format!(
            "failed to authenticate with server: {} ({})",
            error.message, error.code
          )
          .into(),
        ))),
        Err(()) => Ok(Err(Error::Str("failed to authenticate with server".into()))),
      },
      None => Ok(Err(Error::Str(
        "stream was closed before authorization message was received".into(),
      ))),
    }
  }

  async fn subscribe_unsubscribe(
    &mut self,
    request: &Request<'_>,
  ) -> Result<Result<(), Error>, S::Error> {
    let json = match to_json(request) {
      Ok(json) => json,
      Err(err) => return Ok(Err(Error::Json(err))),
    };
    let message = wrap::Message::Text(json);
    let response = self.subscription.send(message).await?;

    match response {
      Some(response) => match response {
        Ok(ControlMessage::Subscription(data)) => {
          self.subscriptions = data;
          Ok(Ok(()))
        },
        Ok(ControlMessage::Error(error)) => Ok(Err(Error::Str(
          format!("failed to subscribe: {error}").into(),
        ))),
        Ok(_) => Ok(Err(Error::Str(
          "server responded with unexpected message".into(),
        ))),
        Err(()) => Ok(Err(Error::Str("failed to adjust subscription".into()))),
      },
      None => Ok(Err(Error::Str(
        "stream was closed before subscription confirmation message was received".into(),
      ))),
    }
  }

  /// Subscribe to additional market data.
  #[inline]
  pub async fn subscribe(&mut self, subscribe: &MarketData) -> Result<Result<(), Error>, S::Error> {
    let request = Request::Subscribe(Cow::Borrowed(subscribe));
    self.subscribe_unsubscribe(&request).await
  }

  /// Unsubscribe from market data for the provided symbols.
  #[inline]
  pub async fn unsubscribe(
    &mut self,
    unsubscribe: &MarketData,
  ) -> Result<Result<(), Error>, S::Error> {
    let request = Request::Unsubscribe(Cow::Borrowed(unsubscribe));
    self.subscribe_unsubscribe(&request).await
  }

  /// Get the currently active subscriptions.
  #[inline]
  pub fn subscriptions(&self) -> &MarketData {
    &self.subscriptions
  }
}


type ParseFn<B, Q, T> = fn(
  Result<wrap::Message, WebSocketError>,
) -> Result<Result<Vec<DataMessage<B, Q, T>>, JsonError>, WebSocketError>;
type MapFn<B, Q, T> =
  fn(Result<Result<DataMessage<B, Q, T>, JsonError>, WebSocketError>) -> ParsedMessage<B, Q, T>;
type Stream<B, Q, T> = Map<
  Unfold<
    Map<Wrapper<WebSocketStream<MaybeTlsStream<TcpStream>>>, ParseFn<B, Q, T>>,
    DataMessage<B, Q, T>,
    JsonError,
  >,
  MapFn<B, Q, T>,
>;


/// Subscribe to real-time market data.
///
/// The type parameter `S` selects the data source:
/// - [`IEX`] or [`SIP`] for stocks
/// - [`CryptoUs`], [`CryptoUs1`], [`CryptoEu1`] for crypto (24/7)
/// - [`CustomUrl`] for any custom endpoint
///
/// The `B`, `Q`, `T` parameters allow customizing the bar/quote/trade types.
#[derive(Debug)]
pub struct RealtimeData<S, B = Bar, Q = Quote, T = Trade> {
  _phantom: PhantomData<(S, B, Q, T)>,
}

#[async_trait]
impl<S, B, Q, T> Subscribable for RealtimeData<S, B, Q, T>
where
  S: Source,
  B: Send + Unpin + Debug + DeserializeOwned,
  Q: Send + Unpin + Debug + DeserializeOwned,
  T: Send + Unpin + Debug + DeserializeOwned,
{
  type Input = ApiInfo;
  type Subscription = Subscription<SplitSink<Stream<B, Q, T>, wrap::Message>, B, Q, T>;
  type Stream = Fuse<MessageStream<SplitStream<Stream<B, Q, T>>, ParsedMessage<B, Q, T>>>;

  async fn connect(api_info: &Self::Input) -> Result<(Self::Stream, Self::Subscription), Error> {
    fn parse<B, Q, T>(
      result: Result<wrap::Message, WebSocketError>,
    ) -> Result<Result<Vec<DataMessage<B, Q, T>>, JsonError>, WebSocketError>
    where
      B: DeserializeOwned,
      Q: DeserializeOwned,
      T: DeserializeOwned,
    {
      result.map(|message| match message {
        wrap::Message::Text(string) => json_from_str::<Vec<DataMessage<B, Q, T>>>(&string),
        wrap::Message::Binary(data) => json_from_slice::<Vec<DataMessage<B, Q, T>>>(&data),
      })
    }

    let ApiInfo {
      data_stream_base_url: url,
      key_id,
      secret,
      ..
    } = api_info;

    let url = match S::source() {
      SourceVariant::PathComponent(component) => {
        let mut url = url.clone();
        // For stock sources (iex, sip), prepend v2/.
        // For crypto sources (v1beta3/crypto/...), use as-is.
        if component.starts_with("v1beta") {
          url.set_path(component);
        } else {
          url.set_path(&format!("v2/{}", component));
        }
        url
      },
      SourceVariant::Url(url) => Url::parse(&url)?,
    };

    let stream = Unfold::new(
      connect(&url)
        .await?
        .map(parse::<B, Q, T> as ParseFn<_, _, _>),
    )
    .map(MessageResult::from as MapFn<B, Q, T>);
    let (send, recv) = stream.split();
    let (stream, subscription) = subscribe::subscribe(recv, send);
    let mut stream = stream.fuse();
    let mut subscription = Subscription::new(subscription);

    let connect = subscription.subscription.read().boxed();
    let message = drive(connect, &mut stream).await.map_err(|result| {
      result
        .map(|result| Error::Json(result.unwrap_err()))
        .map_err(Error::WebSocket)
        .unwrap_or_else(|err| err)
    })?;

    match message {
      Some(Ok(ControlMessage::Success)) => (),
      Some(Ok(_)) => {
        return Err(Error::Str(
          "server responded with unexpected initial message".into(),
        ))
      },
      Some(Err(())) => return Err(Error::Str("failed to read connected message".into())),
      None => {
        return Err(Error::Str(
          "stream was closed before connected message was received".into(),
        ))
      },
    }

    let authenticate = subscription.authenticate(key_id, secret).boxed();
    let () = drive(authenticate, &mut stream).await.map_err(|result| {
      result
        .map(|result| Error::Json(result.unwrap_err()))
        .map_err(Error::WebSocket)
        .unwrap_or_else(|err| err)
    })???;

    Ok((stream, subscription))
  }
}


#[allow(clippy::to_string_trait_impl)]
#[cfg(test)]
mod tests {
  use super::*;

  use std::time::Duration;

  use futures::SinkExt as _;


  use serial_test::serial;

  use serde_json::from_str as json_from_str;

  use test_log::test;

  use tokio::time::timeout;

  use websocket_util::test::WebSocketStream;
  use websocket_util::tungstenite::Message;

  use crate::api_info::API_BASE_URL;
  use crate::websocket::test::mock_stream;
  use crate::Client;


  const CONN_RESP: &str = r#"[{"T":"success","msg":"connected"}]"#;
  const AUTH_RESP: &str = r#"[{"T":"success","msg":"authenticated"}]"#;
  const SUB_RESP: &str = r#"[{"T":"subscription","bars":["AAPL","VOO"]}]"#;


  // --- Serialization/deserialization tests ---

  #[test]
  fn sip_source() {
    assert_ne!(format!("{:?}", SIP::source()), "");
  }

  #[test]
  fn crypto_us_source() {
    match CryptoUs::source() {
      SourceVariant::PathComponent(p) => assert_eq!(p, "v1beta3/crypto/us"),
      _ => panic!("expected PathComponent"),
    }
  }

  #[test]
  fn data_classification() {
    assert!(Data::<(), Quote, Trade>::Bar(()).is_bar());
    assert!(Data::<Bar, (), Trade>::Quote(()).is_quote());
    assert!(Data::<Bar, Quote, ()>::Trade(()).is_trade());
    assert!(Data::<(), Quote, Trade>::DailyBar(()).is_daily_bar());
    assert!(Data::<(), Quote, Trade>::UpdatedBar(()).is_updated_bar());
  }

  #[test]
  fn symbols_is_empty() {
    assert!(!Symbols::All.is_empty());
    assert!(!Symbols::List(SymbolList::from(["SPY"])).is_empty());
    assert!(Symbols::List(SymbolList::from([])).is_empty());
  }

  #[test]
  fn deserialize_bar() {
    let json = r#"{"T":"b","S":"AAPL","o":100.0,"h":105.0,"l":99.0,"c":104.0,"v":1000,"t":"2021-01-01T00:00:00Z"}"#;
    let msg = json_from_str::<DataMessage>(json).unwrap();
    match msg {
      DataMessage::Bar(bar) => {
        assert_eq!(bar.symbol, "AAPL");
        assert!(bar.trade_count.is_none());
      },
      _ => panic!("expected Bar"),
    }
  }

  #[test]
  fn deserialize_crypto_bar() {
    let json = r#"{"T":"b","S":"BTC/USD","o":71856.14,"h":71856.14,"l":71856.14,"c":71856.14,"v":0,"t":"2024-03-12T10:37:00Z","n":0,"vw":0}"#;
    let msg = json_from_str::<DataMessage>(json).unwrap();
    match msg {
      DataMessage::Bar(bar) => {
        assert_eq!(bar.symbol, "BTC/USD");
        assert_eq!(bar.trade_count, Some(0));
        assert!(bar.vwap.is_some());
      },
      _ => panic!("expected Bar"),
    }
  }

  #[test]
  fn deserialize_crypto_trade() {
    let json = r#"{"T":"t","S":"AVAX/USD","p":47.299,"s":29.205707815,"t":"2024-03-12T10:27:48.858228144Z","i":3447222699101865076,"tks":"S"}"#;
    let msg = json_from_str::<DataMessage>(json).unwrap();
    match msg {
      DataMessage::Trade(trade) => {
        assert_eq!(trade.symbol, "AVAX/USD");
        assert_eq!(trade.taker_side.as_deref(), Some("S"));
      },
      _ => panic!("expected Trade"),
    }
  }

  #[test]
  fn deserialize_crypto_quote() {
    let json = r#"{"T":"q","S":"BAT/USD","bp":0.35718,"bs":13445.46,"ap":0.3581,"as":13561.902,"t":"2024-03-12T10:29:43.111588173Z"}"#;
    let msg = json_from_str::<DataMessage>(json).unwrap();
    match msg {
      DataMessage::Quote(quote) => {
        assert_eq!(quote.symbol, "BAT/USD");
      },
      _ => panic!("expected Quote"),
    }
  }

  #[test]
  fn deserialize_orderbook() {
    let json = r#"{"T":"o","S":"BTC/USD","t":"2024-03-12T10:38:50.79613221Z","b":[{"p":71859.53,"s":0.27994}],"a":[{"p":71939.7,"s":0.83953}],"r":true}"#;
    let msg = json_from_str::<DataMessage>(json).unwrap();
    match msg {
      DataMessage::Orderbook(ob) => {
        assert_eq!(ob.symbol, "BTC/USD");
        assert!(ob.reset);
        assert_eq!(ob.bids.len(), 1);
        assert_eq!(ob.asks.len(), 1);
      },
      _ => panic!("expected Orderbook"),
    }
  }

  #[test]
  fn deserialize_daily_bar() {
    let json = r#"{"T":"d","S":"BTC/USD","o":71000.0,"h":72000.0,"l":70000.0,"c":71500.0,"v":100,"t":"2024-03-12T00:00:00Z","n":50,"vw":71200.0}"#;
    let msg = json_from_str::<DataMessage>(json).unwrap();
    match msg {
      DataMessage::DailyBar(bar) => {
        assert_eq!(bar.symbol, "BTC/USD");
        assert_eq!(bar.trade_count, Some(50));
      },
      _ => panic!("expected DailyBar"),
    }
  }

  #[test]
  fn deserialize_updated_bar() {
    let json = r#"{"T":"u","S":"BTC/USD","o":71000.0,"h":72000.0,"l":70000.0,"c":71500.0,"v":100,"t":"2024-03-12T00:00:00Z"}"#;
    let msg = json_from_str::<DataMessage>(json).unwrap();
    assert!(matches!(msg, DataMessage::UpdatedBar(_)));
  }

  #[test]
  fn serialize_market_data_with_crypto_channels() {
    let mut data = MarketData::default();
    data.set_bars(["BTC/USD"]);
    data.set_orderbooks(["BTC/USD"]);
    data.set_daily_bars(["BTC/USD"]);

    let json = serde_json::to_string(&data).unwrap();
    assert!(json.contains("\"orderbooks\""));
    assert!(json.contains("\"dailyBars\""));
  }

  #[test]
  fn serialize_market_data_stock_omits_empty_crypto_channels() {
    let mut data = MarketData::default();
    data.set_bars(["AAPL"]);

    let json = serde_json::to_string(&data).unwrap();
    assert!(!json.contains("orderbooks"));
    assert!(!json.contains("dailyBars"));
    assert!(!json.contains("updatedBars"));
  }

  #[test]
  fn deserialize_crypto_subscription_response() {
    let json = r#"{"T":"subscription","trades":[],"quotes":[],"orderbooks":[],"bars":["BTC/USD"],"updatedBars":[],"dailyBars":[]}"#;
    let msg = json_from_str::<DataMessage>(json).unwrap();
    match msg {
      DataMessage::Subscription(data) => {
        assert_eq!(data.bars, Symbols::List(SymbolList::from(["BTC/USD"])));
      },
      _ => panic!("expected Subscription"),
    }
  }


  // --- Mock WebSocket tests ---

  #[test(tokio::test)]
  async fn mock_connect_and_subscribe() {
    async fn handle(mut stream: WebSocketStream) -> Result<(), WebSocketError> {
      stream.send(Message::Text(CONN_RESP.into())).await?;

      let msg = stream.next().await.unwrap()?;
      let _text = msg.into_text().unwrap();
      stream.send(Message::Text(AUTH_RESP.into())).await?;

      let msg = stream.next().await.unwrap()?;
      let _text = msg.into_text().unwrap();
      stream.send(Message::Text(SUB_RESP.into())).await?;

      // Keep the connection open until the client closes.
      while let Some(msg) = stream.next().await {
        if msg.is_err() {
          break;
        }
      }
      Ok(())
    }

    let (mut stream, mut subscription) = mock_stream::<RealtimeData<IEX>, _, _>(handle)
      .await
      .unwrap();

    let mut data = MarketData::default();
    data.set_bars(["AAPL", "VOO"]);

    let subscribe = subscription.subscribe(&data).boxed();
    let () = drive(subscribe, &mut stream)
      .await
      .unwrap()
      .unwrap()
      .unwrap();

    assert_eq!(subscription.subscriptions(), &data);
  }

  #[test(tokio::test)]
  async fn mock_receive_crypto_data() {
    async fn handle(mut stream: WebSocketStream) -> Result<(), WebSocketError> {
      stream.send(Message::Text(CONN_RESP.into())).await?;
      let _auth = stream.next().await.unwrap()?;
      stream.send(Message::Text(AUTH_RESP.into())).await?;

      let _sub = stream.next().await.unwrap()?;
      let sub_resp = r#"[{"T":"subscription","bars":["BTC/USD"],"quotes":[],"trades":[],"orderbooks":[],"dailyBars":[],"updatedBars":[]}]"#;
      stream.send(Message::Text(sub_resp.into())).await?;

      // Send a crypto bar
      let bar = r#"[{"T":"b","S":"BTC/USD","o":71856.14,"h":71856.14,"l":71856.14,"c":71856.14,"v":0,"t":"2024-03-12T10:37:00Z","n":0,"vw":0}]"#;
      stream.send(Message::Text(bar.into())).await?;

      // Send an orderbook
      let ob = r#"[{"T":"o","S":"BTC/USD","t":"2024-03-12T10:38:50Z","b":[{"p":71859.53,"s":0.27994}],"a":[{"p":71939.7,"s":0.83953}],"r":true}]"#;
      stream.send(Message::Text(ob.into())).await?;

      // Keep connection open
      while let Some(msg) = stream.next().await {
        if msg.is_err() {
          break;
        }
      }
      Ok(())
    }

    let (mut stream, mut subscription) = mock_stream::<RealtimeData<IEX>, _, _>(handle)
      .await
      .unwrap();

    let mut data = MarketData::default();
    data.set_bars(["BTC/USD"]);

    let subscribe = subscription.subscribe(&data).boxed();
    let () = drive(subscribe, &mut stream)
      .await
      .unwrap()
      .unwrap()
      .unwrap();

    // Receive the bar
    let item = stream.next().await.unwrap();
    let data_item = item.unwrap().unwrap();
    assert!(data_item.is_bar(), "expected bar, got: {data_item:?}");

    // Receive the orderbook
    let item = stream.next().await.unwrap();
    let data_item = item.unwrap().unwrap();
    assert!(
      data_item.is_orderbook(),
      "expected orderbook, got: {data_item:?}"
    );
  }


  // --- Integration tests (require APCA_API_KEY_ID + APCA_API_SECRET_KEY) ---

  /// Stream crypto bars from the live Alpaca endpoint (24/7).
  /// Waits until at least one bar is received and validates its fields.
  #[test(tokio::test)]
  #[serial(realtime_data)]
  #[ignore = "requires APCA_API_KEY_ID and APCA_API_SECRET_KEY env vars"]
  async fn stream_crypto_bars() {
    let api_info = ApiInfo::from_env().unwrap();
    let client = Client::new(api_info);
    let (mut stream, mut subscription) =
      client.subscribe::<RealtimeData<CryptoUs>>().await.unwrap();

    let mut data = MarketData::default();
    data.set_bars(["BTC/USD"]);

    let subscribe = subscription.subscribe(&data).boxed();
    let () = drive(subscribe, &mut stream)
      .await
      .unwrap()
      .unwrap()
      .unwrap();

    assert_eq!(subscription.subscriptions().bars, data.bars);

    // Wait for one real bar (crypto is 24/7, bars emit every minute).
    let item = timeout(Duration::from_secs(120), stream.next())
      .await
      .expect("timed out waiting for a crypto bar")
      .expect("stream ended without yielding a bar");

    let msg = item.unwrap().unwrap();
    assert!(msg.is_bar(), "expected a bar, got: {msg:?}");
    match msg {
      Data::Bar(bar) => {
        assert_eq!(bar.symbol, "BTC/USD");
        assert!(bar.open_price > Num::from(0));
        assert!(bar.volume >= Num::from(0));
      },
      _ => unreachable!(),
    }
  }

  /// Stream SIP stock bars from the live Alpaca endpoint (market hours).
  #[test(tokio::test)]
  #[serial(realtime_data)]
  #[ignore = "requires APCA_API_KEY_ID and APCA_API_SECRET_KEY env vars + market hours"]
  async fn stream_sip_bars() {
    let api_info = ApiInfo::from_env().unwrap();
    let client = Client::new(api_info);
    let (mut stream, mut subscription) = client.subscribe::<RealtimeData<SIP>>().await.unwrap();

    let mut data = MarketData::default();
    data.set_bars(["TSLA", "SPY"]);

    let subscribe = subscription.subscribe(&data).boxed();
    let () = drive(subscribe, &mut stream)
      .await
      .unwrap()
      .unwrap()
      .unwrap();

    // Wait for one real bar (SIP bars emit every minute during market hours).
    let item = timeout(Duration::from_secs(120), stream.next())
      .await
      .expect("timed out waiting for a SIP bar")
      .expect("stream ended without yielding a bar");

    let msg = item.unwrap().unwrap();
    assert!(msg.is_bar(), "expected a bar, got: {msg:?}");
  }

  /// Stream crypto trades from the live Alpaca endpoint (24/7).
  /// Subscribes to all crypto trades via wildcard to maximize the chance
  /// of receiving data quickly, then validates the first trade.
  #[test(tokio::test)]
  #[serial(realtime_data)]
  #[ignore = "requires APCA_API_KEY_ID and APCA_API_SECRET_KEY env vars"]
  async fn stream_crypto_trades() {
    let api_info = ApiInfo::from_env().unwrap();
    let client = Client::new(api_info);
    let (mut stream, mut subscription) =
      client.subscribe::<RealtimeData<CryptoUs>>().await.unwrap();

    let mut data = MarketData::default();
    data.trades = Symbols::All;

    let subscribe = subscription.subscribe(&data).boxed();
    let () = drive(subscribe, &mut stream)
      .await
      .unwrap()
      .unwrap()
      .unwrap();

    let item = timeout(Duration::from_secs(120), stream.next())
      .await
      .expect("timed out waiting for a crypto trade")
      .expect("stream ended without yielding a trade");

    let msg = item.unwrap().unwrap();
    assert!(msg.is_trade(), "expected a trade, got: {msg:?}");
    match msg {
      Data::Trade(trade) => {
        assert!(!trade.symbol.is_empty());
        assert!(trade.trade_price > Num::from(0));
        assert!(trade.trade_size > Num::from(0));
        assert!(
          trade.taker_side.is_some(),
          "crypto trades should have taker_side"
        );
      },
      _ => unreachable!(),
    }
  }

  /// Stream crypto quotes from the live Alpaca endpoint (24/7).
  /// Waits until at least one quote is received and validates its fields.
  #[test(tokio::test)]
  #[serial(realtime_data)]
  #[ignore = "requires APCA_API_KEY_ID and APCA_API_SECRET_KEY env vars"]
  async fn stream_crypto_quotes() {
    let api_info = ApiInfo::from_env().unwrap();
    let client = Client::new(api_info);
    let (mut stream, mut subscription) =
      client.subscribe::<RealtimeData<CryptoUs>>().await.unwrap();

    let mut data = MarketData::default();
    data.set_quotes(["BTC/USD"]);

    let subscribe = subscription.subscribe(&data).boxed();
    let () = drive(subscribe, &mut stream)
      .await
      .unwrap()
      .unwrap()
      .unwrap();

    let item = timeout(Duration::from_secs(30), stream.next())
      .await
      .expect("timed out waiting for a crypto quote")
      .expect("stream ended without yielding a quote");

    let msg = item.unwrap().unwrap();
    assert!(msg.is_quote(), "expected a quote, got: {msg:?}");
    match msg {
      Data::Quote(quote) => {
        assert_eq!(quote.symbol, "BTC/USD");
        assert!(quote.bid_price > Num::from(0));
        assert!(quote.ask_price > Num::from(0));
        assert!(quote.ask_price >= quote.bid_price, "ask should be >= bid");
      },
      _ => unreachable!(),
    }
  }

  /// Stream crypto orderbook from the live Alpaca endpoint (24/7).
  /// Waits until at least one orderbook update and validates its structure.
  #[test(tokio::test)]
  #[serial(realtime_data)]
  #[ignore = "requires APCA_API_KEY_ID and APCA_API_SECRET_KEY env vars"]
  async fn stream_crypto_orderbook() {
    let api_info = ApiInfo::from_env().unwrap();
    let client = Client::new(api_info);
    let (mut stream, mut subscription) =
      client.subscribe::<RealtimeData<CryptoUs>>().await.unwrap();

    let mut data = MarketData::default();
    data.set_orderbooks(["BTC/USD"]);

    let subscribe = subscription.subscribe(&data).boxed();
    let () = drive(subscribe, &mut stream)
      .await
      .unwrap()
      .unwrap()
      .unwrap();

    let item = timeout(Duration::from_secs(30), stream.next())
      .await
      .expect("timed out waiting for a crypto orderbook update")
      .expect("stream ended without yielding an orderbook");

    let msg = item.unwrap().unwrap();
    assert!(msg.is_orderbook(), "expected an orderbook, got: {msg:?}");
    match msg {
      Data::Orderbook(ob) => {
        assert_eq!(ob.symbol, "BTC/USD");
        // First message should be a reset with the full book.
        if ob.reset {
          assert!(!ob.bids.is_empty(), "reset orderbook should have bids");
          assert!(!ob.asks.is_empty(), "reset orderbook should have asks");
        }
      },
      _ => unreachable!(),
    }
  }

  /// Test subscribe and unsubscribe with crypto.
  #[test(tokio::test)]
  #[serial(realtime_data)]
  #[ignore = "requires APCA_API_KEY_ID and APCA_API_SECRET_KEY env vars"]
  async fn crypto_subscribe_unsubscribe() {
    let api_info = ApiInfo::from_env().unwrap();
    let client = Client::new(api_info);
    let (mut stream, mut subscription) =
      client.subscribe::<RealtimeData<CryptoUs>>().await.unwrap();

    let mut data = MarketData::default();
    data.set_bars(["BTC/USD", "ETH/USD"]);

    let subscribe_fut = subscription.subscribe(&data).boxed();
    let () = drive(subscribe_fut, &mut stream)
      .await
      .unwrap()
      .unwrap()
      .unwrap();

    // Unsubscribe from ETH/USD
    let mut unsub = MarketData::default();
    unsub.set_bars(["ETH/USD"]);

    let unsubscribe_fut = subscription.unsubscribe(&unsub).boxed();
    let () = drive(unsubscribe_fut, &mut stream)
      .await
      .unwrap()
      .unwrap()
      .unwrap();

    let subs = subscription.subscriptions();
    assert_eq!(subs.bars, Symbols::List(SymbolList::from(["BTC/USD"])));
  }

  /// Test authentication failure with invalid credentials.
  #[test(tokio::test)]
  #[serial(realtime_data)]
  #[ignore = "requires network access"]
  async fn stream_crypto_with_invalid_credentials() {
    let api_info = ApiInfo::from_parts(API_BASE_URL, "invalid", "invalid-too").unwrap();
    let client = Client::new(api_info);
    let err = client
      .subscribe::<RealtimeData<CryptoUs>>()
      .await
      .unwrap_err();

    match err {
      Error::Str(ref e) if e.starts_with("failed to authenticate with server") => (),
      e => panic!("received unexpected error: {e}"),
    }
  }

  /// Test connection with invalid URL.
  #[test(tokio::test)]
  #[serial(realtime_data)]
  #[ignore = "requires APCA_API_KEY_ID and APCA_API_SECRET_KEY env vars"]
  async fn stream_with_invalid_url() {
    #[derive(Default)]
    struct Invalid;

    impl ToString for Invalid {
      fn to_string(&self) -> String {
        "<invalid-url-is-invalid>".into()
      }
    }

    let api_info = ApiInfo::from_env().unwrap();
    let client = Client::new(api_info);
    let err = client
      .subscribe::<RealtimeData<CustomUrl<Invalid>>>()
      .await
      .unwrap_err();

    match err {
      Error::Url(..) => (),
      _ => panic!("Received unexpected error: {err:?}"),
    };
  }
}
