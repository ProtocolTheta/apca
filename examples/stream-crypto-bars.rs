// Copyright (C) 2024 The apca Developers
// SPDX-License-Identifier: GPL-3.0-or-later

//! Streams real-time 1-minute crypto bars (candles) from Alpaca and
//! logs them to stdout.  Crypto markets run 24/7 so this works at any
//! time of day.
//!
//! Usage:
//!   APCA_API_KEY_ID=… APCA_API_SECRET_KEY=… cargo run --example stream-crypto-bars

use apca::streaming::drive;
use apca::streaming::Bar;
use apca::streaming::CryptoUs;
use apca::streaming::Data;
use apca::streaming::MarketData;
use apca::streaming::RealtimeData;
use apca::ApiInfo;
use apca::Client;

use futures::FutureExt as _;
use futures::StreamExt as _;

fn print_bar(bar: &Bar) {
  println!(
    "[{}] {} O:{} H:{} L:{} C:{} V:{} trades:{} vwap:{}",
    bar.timestamp,
    bar.symbol,
    bar.open_price,
    bar.high_price,
    bar.low_price,
    bar.close_price,
    bar.volume,
    bar.trade_count.map_or("-".into(), |n| n.to_string()),
    bar.vwap.as_ref().map_or("-".into(), |v| v.to_string()),
  );
}

#[tokio::main]
async fn main() {
  let api_info = ApiInfo::from_env().unwrap();
  let client = Client::new(api_info);

  println!("Connecting to Alpaca crypto stream...");
  let (mut stream, mut subscription) = client.subscribe::<RealtimeData<CryptoUs>>().await.unwrap();

  let mut data = MarketData::default();
  data.set_bars(["BTC/USD", "ETH/USD", "SOL/USD"]);

  let subscribe = subscription.subscribe(&data).boxed();
  let () = drive(subscribe, &mut stream)
    .await
    .unwrap()
    .unwrap()
    .unwrap();

  println!("Subscribed to BTC/USD, ETH/USD, SOL/USD bars. Waiting for candles...\n");

  while let Some(msg) = stream.next().await {
    match msg {
      Ok(Ok(Data::Bar(bar))) => print_bar(&bar),
      Ok(Ok(other)) => eprintln!("unexpected message: {other:?}"),
      Ok(Err(e)) => eprintln!("json error: {e}"),
      Err(e) => {
        eprintln!("websocket error: {e}");
        break;
      },
    }
  }
}
