// Copyright (C) 2019-2024 The apca Developers
// SPDX-License-Identifier: GPL-3.0-or-later

//! Real-time order update streaming over WebSocket.
//!
//! Subscribes to the `trade_updates` stream to receive order status changes.

use std::borrow::Cow;

use async_trait::async_trait;

use futures::stream::Fuse;
use futures::stream::Map;
use futures::stream::SplitSink;
use futures::stream::SplitStream;
use futures::FutureExt as _;
use futures::Sink;
use futures::StreamExt as _;

use serde::Deserialize;
use serde::Serialize;
use serde_json::from_slice as json_from_slice;
use serde_json::from_str as json_from_str;
use serde_json::to_string as to_json;
use serde_json::Error as JsonError;

use tokio::net::TcpStream;

use tungstenite::MaybeTlsStream;
use tungstenite::WebSocketStream;

use websocket_util::subscribe;
use websocket_util::subscribe::MessageStream;
use websocket_util::tungstenite::Error as WebSocketError;
use websocket_util::wrap;
use websocket_util::wrap::Wrapper;

use crate::api_info::ApiInfo;
use crate::subscribable::Subscribable;
use crate::websocket::connect;
use crate::websocket::MessageResult;
use crate::Error;


/// The status of an order update event.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum OrderStatus {
  #[serde(rename = "new")]
  New,
  #[serde(rename = "replaced")]
  Replaced,
  #[serde(rename = "order_replace_rejected")]
  ReplaceRejected,
  #[serde(rename = "partial_fill")]
  PartialFill,
  #[serde(rename = "fill")]
  Filled,
  #[serde(rename = "done_for_day")]
  DoneForDay,
  #[serde(rename = "canceled")]
  Canceled,
  #[serde(rename = "order_cancel_rejected")]
  CancelRejected,
  #[serde(rename = "expired")]
  Expired,
  #[serde(rename = "pending_cancel")]
  PendingCancel,
  #[serde(rename = "stopped")]
  Stopped,
  #[serde(rename = "rejected")]
  Rejected,
  #[serde(rename = "suspended")]
  Suspended,
  #[serde(rename = "pending_new")]
  PendingNew,
  #[serde(rename = "pending_replace")]
  PendingReplace,
  #[serde(rename = "calculated")]
  Calculated,
  #[doc(hidden)]
  #[serde(other, rename(serialize = "unknown"))]
  Unknown,
}


#[derive(Copy, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[doc(hidden)]
pub enum StreamType {
  #[serde(rename = "trade_updates")]
  OrderUpdates,
}


#[derive(Debug, Deserialize, Serialize)]
#[doc(hidden)]
pub struct Streams<'d> {
  pub streams: Cow<'d, [StreamType]>,
}

impl<'d> From<&'d [StreamType]> for Streams<'d> {
  #[inline]
  fn from(src: &'d [StreamType]) -> Self {
    Self {
      streams: Cow::from(src),
    }
  }
}


#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[doc(hidden)]
#[allow(missing_copy_implementations)]
pub enum AuthenticationStatus {
  #[serde(rename = "authorized")]
  Authorized,
  #[serde(rename = "unauthorized")]
  Unauthorized,
}


#[derive(Debug, Deserialize, Serialize)]
#[doc(hidden)]
pub struct Authentication {
  pub status: AuthenticationStatus,
}


#[derive(Debug, Deserialize, Serialize)]
#[doc(hidden)]
#[serde(tag = "action")]
pub enum Authenticate<'d> {
  #[serde(rename = "auth")]
  Request {
    #[serde(rename = "key")]
    key_id: Cow<'d, str>,
    #[serde(rename = "secret")]
    secret: Cow<'d, str>,
  },
}


#[derive(Debug, Deserialize, Serialize)]
#[doc(hidden)]
#[serde(tag = "action", content = "data")]
pub enum Listen<'d> {
  #[serde(rename = "listen")]
  Request(Streams<'d>),
}


#[derive(Debug)]
#[doc(hidden)]
pub enum ControlMessage {
  AuthenticationMessage(Authentication),
  ListeningMessage(Streams<'static>),
}


/// A raw order update message from the WebSocket.
#[derive(Debug, Deserialize, Serialize)]
#[doc(hidden)]
#[serde(tag = "stream", content = "data")]
#[allow(clippy::large_enum_variant)]
pub enum OrderMessage {
  #[serde(rename = "trade_updates")]
  OrderUpdate(OrderUpdate),
  #[serde(rename = "authorization")]
  AuthenticationMessage(Authentication),
  #[serde(rename = "listening")]
  ListeningMessage(Streams<'static>),
}


/// An order update received via the `trade_updates` stream.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct OrderUpdate {
  /// The event that occurred.
  #[serde(rename = "event")]
  pub event: OrderStatus,
  /// The full order object. Uses `serde_json::Value` for flexibility
  /// with API changes; convert to `trading::types::Order` if needed.
  #[serde(rename = "order")]
  pub order: serde_json::Value,
}


type ParsedMessage = MessageResult<Result<OrderMessage, JsonError>, WebSocketError>;

impl subscribe::Message for ParsedMessage {
  type UserMessage = Result<Result<OrderUpdate, JsonError>, WebSocketError>;
  type ControlMessage = ControlMessage;

  fn classify(self) -> subscribe::Classification<Self::UserMessage, Self::ControlMessage> {
    match self {
      MessageResult::Ok(Ok(message)) => match message {
        OrderMessage::OrderUpdate(update) => subscribe::Classification::UserMessage(Ok(Ok(update))),
        OrderMessage::AuthenticationMessage(authentication) => {
          subscribe::Classification::ControlMessage(ControlMessage::AuthenticationMessage(
            authentication,
          ))
        },
        OrderMessage::ListeningMessage(streams) => {
          subscribe::Classification::ControlMessage(ControlMessage::ListeningMessage(streams))
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


/// A subscription handle for order updates.
#[derive(Debug)]
pub struct Subscription<S>(subscribe::Subscription<S, ParsedMessage, wrap::Message>);

impl<S> Subscription<S>
where
  S: Sink<wrap::Message> + Unpin,
{
  async fn authenticate(
    &mut self,
    key_id: &str,
    secret: &str,
  ) -> Result<Result<(), Error>, S::Error> {
    let request = Authenticate::Request {
      key_id: key_id.into(),
      secret: secret.into(),
    };
    let json = match to_json(&request) {
      Ok(json) => json,
      Err(err) => return Ok(Err(Error::Json(err))),
    };
    let message = wrap::Message::Text(json);
    let response = self.0.send(message).await?;

    match response {
      Some(response) => match response {
        Ok(ControlMessage::AuthenticationMessage(authentication)) => {
          if authentication.status != AuthenticationStatus::Authorized {
            return Ok(Err(Error::Str("authentication not successful".into())))
          }
          Ok(Ok(()))
        },
        Ok(_) => Ok(Err(Error::Str(
          "server responded with an unexpected message".into(),
        ))),
        Err(()) => Ok(Err(Error::Str("failed to authenticate with server".into()))),
      },
      None => Ok(Err(Error::Str(
        "stream was closed before authorization message was received".into(),
      ))),
    }
  }

  async fn listen(&mut self) -> Result<Result<(), Error>, S::Error> {
    let streams = Streams::from([StreamType::OrderUpdates].as_ref());
    let request = Listen::Request(streams);
    let json = match to_json(&request) {
      Ok(json) => json,
      Err(err) => return Ok(Err(Error::Json(err))),
    };
    let message = wrap::Message::Text(json);
    let response = self.0.send(message).await?;

    match response {
      Some(response) => match response {
        Ok(ControlMessage::ListeningMessage(streams)) => {
          if !streams.streams.contains(&StreamType::OrderUpdates) {
            return Ok(Err(Error::Str(
              "server did not subscribe us to order update stream".into(),
            )))
          }
          Ok(Ok(()))
        },
        Ok(_) => Ok(Err(Error::Str(
          "server responded with an unexpected message".into(),
        ))),
        Err(()) => Ok(Err(Error::Str(
          "failed to listen to order update stream".into(),
        ))),
      },
      None => Ok(Err(Error::Str(
        "stream was closed before listen message was received".into(),
      ))),
    }
  }
}


type Stream = Map<Wrapper<WebSocketStream<MaybeTlsStream<TcpStream>>>, MapFn>;
type MapFn = fn(Result<wrap::Message, WebSocketError>) -> ParsedMessage;


/// Subscribe to real-time order updates via the `trade_updates` stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderUpdates {}

#[async_trait]
impl Subscribable for OrderUpdates {
  type Input = ApiInfo;
  type Subscription = Subscription<SplitSink<Stream, wrap::Message>>;
  type Stream = Fuse<MessageStream<SplitStream<Stream>, ParsedMessage>>;

  async fn connect(api_info: &Self::Input) -> Result<(Self::Stream, Self::Subscription), Error> {
    fn map(result: Result<wrap::Message, WebSocketError>) -> ParsedMessage {
      MessageResult::from(result.map(|message| match message {
        wrap::Message::Text(string) => json_from_str::<OrderMessage>(&string),
        wrap::Message::Binary(data) => json_from_slice::<OrderMessage>(&data),
      }))
    }

    let ApiInfo {
      api_stream_url: url,
      key_id,
      secret,
      ..
    } = api_info;

    let stream = connect(url).await?.map(map as MapFn);
    let (send, recv) = stream.split();
    let (stream, subscription) = subscribe::subscribe(recv, send);
    let mut stream = stream.fuse();

    let mut subscription = Subscription(subscription);
    let authenticate = subscription.authenticate(key_id, secret).boxed();
    let () = subscribe::drive::<ParsedMessage, _, _>(authenticate, &mut stream)
      .await
      .map_err(|result| {
        result
          .map(|result| Error::Json(result.unwrap_err()))
          .map_err(Error::WebSocket)
          .unwrap_or_else(|err| err)
      })???;

    let listen = subscription.listen().boxed();
    let () = subscribe::drive::<ParsedMessage, _, _>(listen, &mut stream)
      .await
      .map_err(|result| {
        result
          .map(|result| Error::Json(result.unwrap_err()))
          .map_err(Error::WebSocket)
          .unwrap_or_else(|err| err)
      })???;

    Ok((stream, subscription))
  }
}
