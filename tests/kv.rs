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

#![cfg(feature = "unstable")]
mod util;

use futures::stream::StreamExt;
use nats::jetstream::StreamConfig;

use nats::kv::*;

#[tokio::test]
async fn key_value_entry() {
    let server = util::run_server("tests/configs/jetstream.conf");
    let client = nats::connect(&server.client_url()).await.unwrap();
    let context = nats::jetstream::new(client);

    let kv = context
        .create_key_value(&nats::kv::Config {
            bucket: "ENTRY".to_string(),
            history: 5,
            max_age: std::time::Duration::from_secs(3600),
            ..Default::default()
        })
        .await
        .unwrap();

    // Initial state
    assert!(kv.entry("foo").await.unwrap().is_none());

    // Put
    let revision = kv.put("foo", b"bar").await.unwrap();
    assert_eq!(revision, 1);

    let entry = kv.entry("foo").await.unwrap().unwrap();
    assert_eq!(entry.value, b"bar");
    assert_eq!(entry.revision, 1);

    let value = kv.get("foo").await.unwrap();
    assert_eq!(value, Some(b"bar".to_vec()));

    // Delete
    kv.delete("foo").await.unwrap();

    let entry = kv.entry("foo").await.unwrap().unwrap();
    assert_eq!(entry.operation, Operation::Delete);

    let value = kv.get("foo").await.unwrap();
    assert_eq!(value, None);

    // Create
    let revision = kv.create("foo", b"bar").await.unwrap();
    assert_eq!(revision, 3);

    // Test conditional updates
    let revision = kv.update("foo", b"rip", revision).await.unwrap();
    kv.update("foo", b"rip", revision).await.unwrap();

    let revision = kv.create("bar", b"baz").await.unwrap();
    kv.update("bar", b"baz", revision).await.unwrap();

    // Status
    let status = kv.status().await.unwrap();
    assert_eq!(status.history(), 5);
    assert_eq!(status.bucket(), "ENTRY");
    assert_eq!(status.max_age(), std::time::Duration::from_secs(3600));
    assert_eq!(status.values(), 7);
}

#[tokio::test]
async fn key_value_short_history() {
    let server = util::run_server("tests/configs/jetstream.conf");
    let client = nats::connect(&server.client_url()).await.unwrap();
    let context = nats::jetstream::new(client);

    let kv = context
        .create_key_value(&nats::kv::Config {
            bucket: "HISTORY".to_string(),
            history: 5,
            ..Default::default()
        })
        .await
        .unwrap();

    for i in 0..10 {
        kv.put("value", [i as u8]).await.unwrap();
    }

    let mut history = kv.history("value").await.unwrap();
    for i in 5..10 {
        let entry = history.next().await.unwrap();

        assert_eq!(entry.key, "value");
        assert_eq!(entry.value, [i as u8]);
    }
}

#[tokio::test]
async fn key_value_long_history() {
    let server = util::run_server("tests/configs/jetstream.conf");
    let client = nats::connect(&server.client_url()).await.unwrap();
    let context = nats::jetstream::new(client);

    let kv = context
        .create_key_value(&nats::kv::Config {
            bucket: "HISTORY".to_string(),
            history: 25,
            ..Default::default()
        })
        .await
        .unwrap();

    for i in 0..50 {
        kv.put("value", [i as u8]).await.unwrap();
    }

    let mut history = kv.history("value").await.unwrap();
    for i in 25..50 {
        let entry = history.next().await.unwrap();

        assert_eq!(entry.key, "value");
        assert_eq!(entry.value, [i as u8]);
    }
}

#[tokio::test]
async fn key_value_watch() {
    let server = util::run_server("tests/configs/jetstream.conf");
    let client = nats::connect(&server.client_url()).await.unwrap();
    let context = nats::jetstream::new(client);

    let kv = context
        .create_key_value(&nats::kv::Config {
            bucket: "WATCH".to_string(),
            history: 10,
            ..Default::default()
        })
        .await
        .unwrap();

    let mut watch = kv.watch().await.unwrap();

    kv.create("foo", b"lorem").await.unwrap();
    let entry = watch.next().await.unwrap();
    assert_eq!(entry.key, "foo".to_string());
    assert_eq!(entry.value, b"lorem");
    assert_eq!(entry.revision, 1);

    kv.put("foo", b"ipsum").await.unwrap();
    let entry = watch.next().await.unwrap();
    assert_eq!(entry.key, "foo".to_string());
    assert_eq!(entry.value, b"ipsum");
    assert_eq!(entry.revision, 2);

    kv.delete("foo").await.unwrap();
    let entry = watch.next().await.unwrap();
    assert_eq!(entry.operation, Operation::Delete);

    drop(watch);
}

#[tokio::test]
async fn key_value_bind() {
    let server = util::run_server("tests/configs/jetstream.conf");
    let client = nats::connect(&server.client_url()).await.unwrap();
    let context = nats::jetstream::new(client);

    context
        .create_key_value(&Config {
            bucket: "WATCH".to_string(),
            ..Default::default()
        })
        .await
        .unwrap();

    // Now bind to it..
    context.key_value("WATCH").await.unwrap();

    // Make sure we can't bind to a non-kv style stream.
    // We have some protection with stream name prefix.
    context
        .add_stream(&StreamConfig {
            name: "KV_TEST".to_string(),
            subjects: vec!["foo".to_string()],
            ..Default::default()
        })
        .await
        .unwrap();

    context.key_value("TEST").await.unwrap_err();
}

#[tokio::test]
async fn key_value_delete() {
    let server = util::run_server("tests/configs/jetstream.conf");
    let client = nats::connect(&server.client_url()).await.unwrap();
    let context = nats::jetstream::new(client);

    context
        .create_key_value(&Config {
            bucket: "TEST".to_string(),
            ..Default::default()
        })
        .await
        .unwrap();

    context.key_value("TEST").await.unwrap();

    context.delete_key_value("TEST").await.unwrap();
    context.key_value("TEST").await.unwrap_err();
}

#[tokio::test]
async fn key_value_purge() {
    let server = util::run_server("tests/configs/jetstream.conf");
    let client = nats::connect(&server.client_url()).await.unwrap();
    let context = nats::jetstream::new(client);

    let bucket = context
        .create_key_value(&Config {
            bucket: "FOO".to_string(),
            history: 10,
            ..Default::default()
        })
        .await
        .unwrap();

    bucket.put("foo", "1").await.unwrap();
    bucket.put("foo", "2").await.unwrap();
    bucket.put("foo", "3").await.unwrap();

    bucket.put("bar", "1").await.unwrap();
    bucket.put("baz", "2").await.unwrap();
    bucket.put("baz", "3").await.unwrap();

    let entries = bucket.history("foo").await.unwrap();
    assert_eq!(entries.count().await, 3);

    bucket.purge("foo").await.unwrap();

    let value = bucket.get("foo").await.unwrap();
    assert_eq!(value, None);

    let entries = bucket.history("foo").await.unwrap();
    assert_eq!(entries.count().await, 1);
}

#[tokio::test]
async fn key_value_keys() {
    use futures::stream::StreamExt as _;

    let server = util::run_server("tests/configs/jetstream.conf");
    let client = nats::connect(&server.client_url()).await.unwrap();
    let context = nats::jetstream::new(client);

    let kv = context
        .create_key_value(&Config {
            bucket: "KVS".to_string(),
            history: 2,
            ..Default::default()
        })
        .await
        .unwrap();

    kv.put("foo", b"a").await.unwrap();
    kv.put("bar", b"b").await.unwrap();
    kv.put("baz", b"c").await.unwrap();

    let keys = kv.keys().await.unwrap().collect::<Vec<String>>().await;
    assert!(keys.iter().any(|s| s == "foo"));
    assert!(keys.iter().any(|s| s == "bar"));
    assert!(keys.iter().any(|s| s == "baz"));
    assert_eq!(keys.len(), 3);

    kv.delete("foo").await.unwrap();
    kv.purge("bar").await.unwrap();

    // TODO(ss) BUG: after either delete or purge above, keys() stream hangs.
    // All items are returned, but the n+1th call to next().await hangs when it should return None.
    // Using either delete or purge causes the problem.
    // With neither, we can iterate through keys again.
    #[cfg(feature = "failing_tests")]
    {
        eprintln!("checking keys after delete/purge, expect 1 item");
        let mut stream = kv.keys().await.unwrap();
        let mut keys = Vec::new();
        while let Some(k) = stream.next().await {
            eprintln!("found {}", &k);
            keys.push(k);
        }

        assert!(keys.iter().any(|s| s == "baz"));
        assert_eq!(keys.len(), 1);
    }
}
