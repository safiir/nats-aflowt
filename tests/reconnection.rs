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

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use nats_test_server::NatsTestServer;

#[tokio::test]
async fn reconnect_test() {
    env_logger::init();

    let shutdown = Arc::new(AtomicBool::new(false));
    let success = Arc::new(AtomicBool::new(false));

    // kill process if we take longer than 15 minutes to run the test
    thread::spawn({
        let success = success.clone();
        move || {
            thread::sleep(Duration::from_secs(15 * 60));
            if !success.load(Ordering::Acquire) {
                log::error!("killing process after 15 minutes");
                std::process::exit(1);
            }
        }
    });

    let server = NatsTestServer::build().bugginess(200).spawn();

    let nc = loop {
        if let Ok(nc) = nats_aflowt::Options::new()
            .max_reconnects(None)
            .connect(&server.address().to_string())
            .await
        {
            break Arc::new(nc);
        }
    };

    let tx = tokio::spawn({
        let nc = nc.clone();
        let success = success.clone();
        let shutdown = shutdown.clone();
        async move {
            const EXPECTED_SUCCESSES: usize = 25;
            let mut received = 0;

            while received < EXPECTED_SUCCESSES && !shutdown.load(Ordering::Acquire) {
                if nc
                    .request_timeout(
                        "rust.tests.faulty_requests",
                        "Help me?",
                        std::time::Duration::from_millis(200),
                    )
                    .await
                    .is_ok()
                {
                    received += 1;
                } else {
                    log::debug!("timed out before we received a response :(");
                }
            }

            if received == EXPECTED_SUCCESSES {
                success.store(true, Ordering::Release);
            }
        }
    });

    let subscriber = loop {
        if let Ok(subscriber) = nc.subscribe("rust.tests.faulty_requests").await {
            break subscriber;
        }
    };

    while !success.load(Ordering::Acquire) {
        while let Ok(msg) = subscriber.next_timeout(Duration::from_millis(10)).await {
            let _unchecked = msg.respond("Anything for the story").await;
        }
    }

    shutdown.store(true, Ordering::Release);

    tx.await.unwrap();
}
