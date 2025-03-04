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
pub use util::*;

#[tokio::test]
async fn basic_user_pass_auth() {
    let s = util::run_server("tests/configs/user_pass.conf");

    assert!(nats_aflowt::connect(&s.client_url()).await.is_err());

    assert!(nats_aflowt::Options::with_user_pass("derek", "s3cr3t")
        .connect(&s.client_url())
        .await
        .is_ok());

    assert!(nats_aflowt::connect(&s.client_url_with("derek", "s3cr3t"))
        .await
        .is_ok());

    // Check override.
    assert!(
        nats_aflowt::Options::with_user_pass("derek", "bad-password")
            .connect(&s.client_url_with("derek", "s3cr3t"))
            .await
            .is_ok()
    );
}
