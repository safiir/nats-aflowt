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

mod util;
use nats_aflowt::{AsyncCall, AsyncErrorCallback, BoxFuture};
pub use util::*;

struct SendBoolCallback {
    tx: tokio::sync::mpsc::Sender<bool>,
}

impl AsyncCall for SendBoolCallback {
    fn call(&self) -> BoxFuture<()> {
        let tx = self.tx.clone();
        Box::pin(async move {
            tx.send(true).await.unwrap();
        })
    }
}

struct SendErrCallback {
    tx: tokio::sync::mpsc::Sender<std::io::Error>,
}

impl AsyncErrorCallback for SendErrCallback {
    fn call(&self, _si: nats_aflowt::ServerInfo, err: std::io::Error) -> BoxFuture<()> {
        let tx = self.tx.clone();
        Box::pin(async move {
            tx.send(err).await.unwrap();
        })
    }
}

#[tokio::test]
async fn pub_perms() {
    use std::time::Duration;

    let s = util::run_server("tests/configs/perms.conf");
    let (dtx, mut drx) = tokio::sync::mpsc::channel(1);
    let (etx, mut erx) = tokio::sync::mpsc::channel(1);

    let nc = nats_aflowt::Options::with_user_pass("derek", "s3cr3t!")
        .error_callback(SendErrCallback { tx: etx })
        .disconnect_callback(SendBoolCallback { tx: dtx })
        .connect(&s.client_url())
        .await
        .expect("could not connect");

    nc.publish("foo", "NOT ALLOWED").await.unwrap();
    let r = tokio::time::timeout(Duration::from_millis(100), erx.recv())
        .await
        .expect("erx timeout");
    assert!(
        r.is_some(),
        "expected an error callback, got closed connection"
    );
    assert_eq!(
        r.unwrap().to_string(),
        r#"Permissions Violation for Publish to "foo""#
    );

    let r = tokio::time::timeout(Duration::from_millis(100), drx.recv()).await;
    assert!(r.is_err(), "we got disconnected on perm violation");
}

#[tokio::test]
async fn sub_perms() {
    use std::time::Duration;

    let s = util::run_server("tests/configs/perms.conf");
    let (dtx, mut drx) = tokio::sync::mpsc::channel(1);
    let (etx, mut erx) = tokio::sync::mpsc::channel(1);

    let nc = nats_aflowt::Options::with_user_pass("derek", "s3cr3t!")
        .error_callback(SendErrCallback { tx: etx })
        .disconnect_callback(SendBoolCallback { tx: dtx })
        .connect(&s.client_url())
        .await
        .expect("could not connect");

    let _sub = nc.subscribe("foo").await.unwrap();

    let r = tokio::time::timeout(Duration::from_millis(100), erx.recv())
        .await
        .expect("erx timeout");
    assert!(r.is_some(), "expected an error callback, got none");
    assert_eq!(
        r.unwrap().to_string(),
        r#"Permissions Violation for Subscription to "foo""#
    );

    let r = tokio::time::timeout(Duration::from_millis(100), drx.recv()).await;
    assert!(r.is_err(), "we got disconnected on perm violation");
}
