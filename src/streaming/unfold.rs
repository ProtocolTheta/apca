// Copyright (C) 2021 The apca Developers
// SPDX-License-Identifier: GPL-3.0-or-later

use std::marker::PhantomData;
use std::pin::Pin;

use futures::task::Context;
use futures::task::Poll;
use futures::Sink;
use futures::SinkExt as _;
use futures::Stream;
use futures::StreamExt as _;

use websocket_util::tungstenite::Error as WebSocketError;

#[derive(Debug)]
#[doc(hidden)]
#[must_use = "streams do nothing unless polled"]
pub struct Unfold<S, T, E> {
  inner: S,
  messages: Vec<T>,
  _phantom: PhantomData<E>,
}

impl<S, T, E> Unfold<S, T, E> {
  pub(crate) fn new(inner: S) -> Self {
    Self {
      inner,
      messages: Vec::new(),
      _phantom: PhantomData,
    }
  }
}

impl<S, T, E> Stream for Unfold<S, T, E>
where
  S: Stream<Item = Result<Result<Vec<T>, E>, WebSocketError>> + Unpin,
  T: Unpin,
  E: Unpin,
{
  type Item = Result<Result<T, E>, WebSocketError>;

  fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    loop {
      if !self.messages.is_empty() {
        let message = self.messages.remove(0);
        break Poll::Ready(Some(Ok(Ok(message))))
      } else {
        match self.inner.poll_next_unpin(ctx) {
          Poll::Pending => break Poll::Pending,
          Poll::Ready(None) => break Poll::Ready(None),
          Poll::Ready(Some(Err(err))) => break Poll::Ready(Some(Err(err))),
          Poll::Ready(Some(Ok(Err(err)))) => break Poll::Ready(Some(Ok(Err(err)))),
          Poll::Ready(Some(Ok(Ok(messages)))) => {
            self.messages = messages;
          },
        }
      }
    }
  }
}

impl<S, T, E, U> Sink<U> for Unfold<S, T, E>
where
  S: Sink<U, Error = WebSocketError> + Unpin,
  T: Unpin,
  E: Unpin,
{
  type Error = WebSocketError;

  fn poll_ready(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
    self.inner.poll_ready_unpin(ctx)
  }

  fn start_send(mut self: Pin<&mut Self>, message: U) -> Result<(), Self::Error> {
    self.inner.start_send_unpin(message)
  }

  fn poll_flush(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
    self.inner.poll_flush_unpin(ctx)
  }

  fn poll_close(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
    self.inner.poll_close_unpin(ctx)
  }
}
