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

#![cfg(feature = "unstable")]
mod util;

use futures::stream::StreamExt;
use nats_aflowt::{jetstream::StreamConfig, kv::*};

#[tokio::test]
async fn key_value_entry() {
    let server = util::run_server("tests/configs/jetstream.conf");
    let client = nats_aflowt::connect(&server.client_url()).await.unwrap();
    let context = nats_aflowt::jetstream::new(client);

    let kv = context
        .create_key_value(&nats_aflowt::kv::Config {
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
    let client = nats_aflowt::connect(&server.client_url()).await.unwrap();
    let context = nats_aflowt::jetstream::new(client);

    let kv = context
        .create_key_value(&nats_aflowt::kv::Config {
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
    let client = nats_aflowt::connect(&server.client_url()).await.unwrap();
    let context = nats_aflowt::jetstream::new(client);

    let kv = context
        .create_key_value(&nats_aflowt::kv::Config {
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
    let client = nats_aflowt::connect(&server.client_url()).await.unwrap();
    let context = nats_aflowt::jetstream::new(client);

    let kv = context
        .create_key_value(&nats_aflowt::kv::Config {
            bucket: "WATCH".to_string(),
            history: 10,
            ..Default::default()
        })
        .await
        .unwrap();

    let mut watch = kv.watch("foo.>").await.unwrap();
    // check if we get only foo.bar watch event.
    kv.create("foo", b"ignored").await.unwrap();
    kv.put("foo.bar", b"lorem").await.unwrap();
    let entry = watch.next().await.unwrap();
    assert_eq!(entry.key, "foo.bar".to_string());
    assert_eq!(entry.value, b"lorem");
    // expect revision 2, instead of 1 as two values were put.
    assert_eq!(entry.revision, 2);

    // check if we only get foo.bar.z events
    kv.put("foo", b"ignored").await.unwrap();
    kv.put("foo.bar.z", b"ipsum").await.unwrap();
    let entry = watch.next().await.unwrap();
    assert_eq!(entry.key, "foo.bar.z".to_string());
    assert_eq!(entry.value, b"ipsum");
    // expect revision 4, as two values were inserted.
    assert_eq!(entry.revision, 4);

    kv.delete("foo").await.unwrap();
    kv.delete("foo.bar").await.unwrap();
    let entry = watch.next().await.unwrap();
    assert_eq!(entry.operation, Operation::Delete);
}

#[tokio::test]
async fn key_value_watch_all() {
    let server = util::run_server("tests/configs/jetstream.conf");
    let client = nats_aflowt::connect(&server.client_url()).await.unwrap();
    let context = nats_aflowt::jetstream::new(client);

    let kv = context
        .create_key_value(&nats_aflowt::kv::Config {
            bucket: "WATCH".to_string(),
            history: 10,
            ..Default::default()
        })
        .await
        .unwrap();

    // create second Store to see if
    // https://github.com/nats-io/nats.rs/issues/286 is still affecting our codebase.
    // It was causing one store being able to read data from another.
    let skv = context
        .create_key_value(&nats_aflowt::kv::Config {
            bucket: "TEST_CONFLICT".to_string(),
            history: 10,
            ..Default::default()
        })
        .await
        .unwrap();

    let mut watch = kv.watch_all().await.unwrap();

    // create some data in second Store to see if `watch` will catch this data and panic.
    skv.create("foo", b"loren").await.unwrap();

    // test watch
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
    let client = nats_aflowt::connect(&server.client_url()).await.unwrap();
    let context = nats_aflowt::jetstream::new(client);

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
    let client = nats_aflowt::connect(&server.client_url()).await.unwrap();
    let context = nats_aflowt::jetstream::new(client);

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
    let client = nats_aflowt::connect(&server.client_url()).await.unwrap();
    let context = nats_aflowt::jetstream::new(client);

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
    let client = nats_aflowt::connect(&server.client_url()).await.unwrap();
    let context = nats_aflowt::jetstream::new(client);

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
