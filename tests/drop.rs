// Copyright 2020-2022 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io;

mod util;
pub use util::*;

#[tokio::test]
async fn drop_flushes() -> io::Result<()> {
    let s = util::run_basic_server();

    let nc1 = nats_aflowt::connect(&s.client_url()).await?;
    let nc2 = nats_aflowt::connect(&s.client_url()).await?;

    let inbox = nc1.new_inbox();
    let sub = nc2.subscribe(&inbox).await?;
    nc2.flush().await?;

    nc1.publish(&inbox, b"hello").await?;
    drop(nc1); // Dropping should flush the published message.

    assert_eq!(sub.next().await.unwrap().data, b"hello");

    Ok(())
}

#[tokio::test]
async fn two_connections() -> io::Result<()> {
    let s = util::run_basic_server();

    let nc1 = nats_aflowt::connect(&s.client_url()).await?;
    let nc2 = nc1.clone();

    nc1.publish("foo", b"bar").await?;
    nc2.publish("foo", b"bar").await?;

    drop(nc1);
    nc2.publish("foo", b"bar").await?;

    Ok(())
}

#[tokio::test]
async fn async_subscription_drop() -> io::Result<()> {
    use std::time::Duration;

    let s = util::run_basic_server();

    let nc = nats_aflowt::connect(s.client_url()).await?;
    let inbox = nc.new_inbox();

    // This makes sure the subscription is closed after being dropped. If it wasn't closed,
    // creating the 501st subscription would block forever due to the `blocking` crate's thread
    // pool being fully occupied.
    for i in 0..600 {
        let sub = match tokio::time::timeout(Duration::from_secs(2), nc.subscribe(&inbox)).await {
            Ok(sub) => sub,
            Err(_) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("unable to create subscription {}", i),
            )),
        }?;
        sub.next_timeout(Duration::from_millis(1)).await.ok();
    }
    Ok(())
}
