// Copyright (C) 2020-2024 The apca Developers
// SPDX-License-Identifier: GPL-3.0-or-later

use apca::ApiInfo;
use apca::Client;


#[tokio::main]
async fn main() {
  let api_info = ApiInfo::from_env().unwrap();
  let client = Client::new(api_info);

  // Get account info
  let account = client.trading().get_account().await.unwrap();
  println!("Account: {:?}", account.into_inner());

  // List open positions
  let positions = client.trading().get_all_open_positions().await.unwrap();
  println!("Positions: {:?}", positions.into_inner());
}
