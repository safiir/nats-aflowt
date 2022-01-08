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
use nats::AsyncCall;
use nats::BoxFuture;
pub use util::*;

#[derive(Clone)]
struct LDCallback {
    ltx: tokio::sync::mpsc::Sender<bool>,
}

impl<'a> AsyncCall for LDCallback {
    fn call(&self) -> BoxFuture<()> {
        let ltx = self.ltx.clone();
        Box::pin(async move {
            ltx.send(true).await.unwrap();
        })
    }
}

#[tokio::test]
#[cfg_attr(target_os = "windows", ignore)]
async fn lame_duck_mode() {
    use std::time::Duration;

    let (ltx, mut lrx) = tokio::sync::mpsc::channel(1);

    let s = util::run_basic_server();
    let nc = nats::Options::new()
        .lame_duck_callback(LDCallback { ltx: ltx.clone() })
        .connect(s.client_url().as_str())
        .await
        .expect("could not connect to the server");
    let _sub = nc.subscribe("foo").await.unwrap();
    set_lame_duck_mode(&s);
    let r = tokio::time::timeout(Duration::from_millis(500), lrx.recv()).await;
    assert!(r.is_ok(), "expected lame duck response, got timeout");
    let r = r.unwrap();
    assert!(
        r.is_some(),
        "expected lame duck response, got stream closed"
    );
}
