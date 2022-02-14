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

use std::{collections::HashSet, io, iter::FromIterator, time::Duration};

mod util;
use nats_aflowt::jetstream::{self, *};
pub use util::*;

#[tokio::test]
async fn jetstream_not_enabled() {
    let s = util::run_basic_server();
    let nc = nats_aflowt::connect(&s.client_url()).await.unwrap();
    let js = jetstream::new(nc);

    let err = js.account_info().await.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::NotFound);

    /*
     * TODO(ss): Not getting this error from the server
    let err = err
        .into_inner()
        .expect("should be able to convert error into inner")
        .downcast::<jetstream::Error>()
        .expect("should be able to downcast into error");

    assert_eq!(err.error_code(), jetstream::ErrorCode::NotEnabled);
    */
}

#[tokio::test]
async fn jetstream_account_not_enabled() {
    let s = util::run_server("tests/configs/jetstream_account_not_enabled.conf");
    let nc = nats_aflowt::connect(&s.client_url()).await.unwrap();
    let js = nats_aflowt::jetstream::new(nc);

    let err = js.account_info().await.unwrap_err();
    println!("{:?}", err);
    assert_eq!(err.kind(), io::ErrorKind::Other);

    let err = err
        .into_inner()
        .expect("should be able to convert error into inner")
        .downcast::<jetstream::Error>()
        .expect("should be able to downcast into jetstream::Error");

    assert_eq!(err.error_code(), jetstream::ErrorCode::NotEnabledForAccount);
}

#[tokio::test]
async fn jetstream_publish() {
    let (_s, nc, js) = run_basic_jetstream().await;

    // Create the stream using our client API.
    js.add_stream(StreamConfig {
        name: "TEST".to_string(),
        subjects: vec!["test".to_string(), "foo".to_string(), "bar".to_string()],
        ..Default::default()
    })
    .await
    .unwrap();

    // Lookup the stream for testing.
    js.stream_info("TEST").await.unwrap();

    let msg = b"Hello JS";

    // Basic publish like NATS core.
    let ack = js.publish("foo", &msg).await.unwrap();
    assert_eq!(ack.stream, "TEST");
    assert_eq!(ack.sequence, 1, "seq 1");
    assert_eq!(js.stream_info("TEST").await.unwrap().state.messages, 1);

    // Test stream expectation.
    let err = js
        .publish_with_options(
            "foo",
            &msg,
            &PublishOptions {
                expected_stream: Some("ORDERS".to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();

    assert_eq!(err.kind(), io::ErrorKind::Other);

    let err = err
        .into_inner()
        .expect("should be able to convert error into inner")
        .downcast::<jetstream::Error>()
        .expect("should be able to downcast into error")
        .to_owned();

    assert_eq!(err.error_code(), jetstream::ErrorCode::StreamNotMatch);

    // Test last sequence expectation.
    let err = js
        .publish_with_options(
            "foo",
            &msg,
            &PublishOptions {
                expected_last_sequence: Some(10),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();

    assert_eq!(err.kind(), io::ErrorKind::Other);

    let err = err
        .into_inner()
        .expect("should be able to convert error into inner")
        .downcast::<jetstream::Error>()
        .expect("should be able to downcast into error")
        .to_owned();

    assert_eq!(
        err.error_code(),
        jetstream::ErrorCode::StreamWrongLastSequence
    );

    // Messages should have been rejected
    assert_eq!(js.stream_info("TEST").await.unwrap().state.messages, 1);

    // Send in a stream with a message id
    let ack = js
        .publish_with_options(
            "foo",
            &msg,
            &PublishOptions {
                id: Some("ZZZ".to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(ack.stream, "TEST");
    assert_eq!(ack.sequence, 2, "seq 2");
    assert_eq!(js.stream_info("TEST").await.unwrap().state.messages, 2);

    // Send in the same message with same message id.
    let ack = js
        .publish_with_options(
            "foo",
            &msg,
            &PublishOptions {
                id: Some("ZZZ".to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert!(ack.duplicate);
    assert_eq!(ack.stream, "TEST");
    assert_eq!(ack.sequence, 2);
    assert_eq!(js.stream_info("TEST").await.unwrap().state.messages, 2);

    // Now try to send one in with the wrong last msgId.
    let err = js
        .publish_with_options(
            "foo",
            msg,
            &PublishOptions {
                expected_last_msg_id: Some("AAA".to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Other);

    let err = err
        .into_inner()
        .expect("should be able to convert error into inner")
        .downcast::<jetstream::Error>()
        .expect("should be able to downcast into error")
        .to_owned();

    assert_eq!(err.error_code(), jetstream::ErrorCode::StreamWrongLastMsgId);
    assert_eq!(js.stream_info("TEST").await.unwrap().state.messages, 2);

    // Make sure expected sequence works.
    let err = js
        .publish_with_options(
            "foo",
            msg,
            &PublishOptions {
                expected_last_sequence: Some(22),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Other);

    let err = err
        .into_inner()
        .expect("should be able to convert error into inner")
        .downcast::<jetstream::Error>()
        .expect("should be able to downcast into error")
        .to_owned();

    assert_eq!(
        err.error_code(),
        jetstream::ErrorCode::StreamWrongLastSequence
    );
    assert_eq!(js.stream_info("TEST").await.unwrap().state.messages, 2);

    let ack = js
        .publish_with_options(
            "foo",
            msg,
            &PublishOptions {
                expected_last_sequence: Some(2),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(ack.stream, "TEST");
    assert_eq!(ack.sequence, 3);
    assert_eq!(js.stream_info("TEST").await.unwrap().state.messages, 3);

    // Test expected last subject sequence.
    // Just make sure that we set the header.
    let sub = nc.subscribe("test").await.unwrap();

    js.publish_with_options(
        "test",
        msg,
        &PublishOptions {
            expected_last_subject_sequence: Some(1),
            ..Default::default()
        },
    )
    .await
    .ok();

    let msg = sub.next_timeout(Duration::from_secs(1)).await.unwrap();
    assert_eq!(
        msg.headers
            .unwrap()
            .inner
            .get("Nats-Expected-Last-Subject-Sequence")
            .unwrap(),
        &HashSet::from_iter(vec!["1".to_string()])
    );
}

#[tokio::test]
async fn jetstream_subscribe() {
    let s = util::run_server("tests/configs/jetstream.conf");
    let nc = nats_aflowt::connect(&s.client_url()).await.unwrap();
    let js = nats_aflowt::jetstream::new(nc);

    js.add_stream(&StreamConfig {
        name: "TEST".to_string(),
        subjects: vec![
            "foo".to_string(),
            "bar".to_string(),
            "baz".to_string(),
            "foo.*".to_string(),
        ],
        ..Default::default()
    })
    .await
    .unwrap();

    js.stream_info("TEST").await.unwrap();

    let sub = js.subscribe("foo").await.unwrap();

    let payload = b"hello js";
    for _ in 0..10 {
        js.publish("foo", payload).await.unwrap();
    }

    for _ in 0..10 {
        // And receive it on our subscription
        let msg = sub.next().await.unwrap();
        msg.ack().await.unwrap();
        assert_eq!(msg.data, payload);
    }

    // Check the state of consumer matches up with our expectations
    let info = sub.consumer_info().await.unwrap();
    assert_eq!(info.config.ack_policy, AckPolicy::Explicit);
    assert_eq!(info.delivered.consumer_seq, 10);
    assert_eq!(info.ack_floor.consumer_seq, 10);
}

#[tokio::test]
async fn jetstream_subscribe_durable() {
    let s = util::run_server("tests/configs/jetstream.conf");
    let nc = nats_aflowt::connect(&s.client_url()).await.unwrap();
    let js = jetstream::new(nc);

    js.add_stream(&StreamConfig {
        name: "TEST".to_string(),
        subjects: vec![
            "foo".to_string(),
            "bar".to_string(),
            "baz".to_string(),
            "foo.*".to_string(),
        ],
        ..Default::default()
    })
    .await
    .unwrap();

    js.stream_info("TEST").await.unwrap();

    // Create a durable subscription.
    let mut sub = js
        .subscribe_with_options(
            "foo",
            &SubscribeOptions::new().durable_name("foobar".to_string()),
        )
        .await
        .unwrap();

    let info = sub.consumer_info().await.unwrap();
    assert_eq!(info.config.durable_name, Some("foobar".to_string()));

    // Drain to delete the consumer.
    sub.drain().await.unwrap();

    // Re-create the subscription
    let sub = js
        .subscribe_with_options(
            "foo",
            &SubscribeOptions::new().durable_name("foobar".to_string()),
        )
        .await
        .unwrap();

    // Check that it has a new delivery subject.
    let old_info = info;
    let new_info = sub.consumer_info().await.unwrap();

    assert_ne!(
        new_info.config.deliver_subject,
        old_info.config.deliver_subject
    );
    // Unsubscribe to delete the consumer.
    sub.unsubscribe().await.unwrap();

    // Create again and make sure that works.
    js.subscribe_with_options(
        "foo",
        &SubscribeOptions::new().durable_name("foobar".to_string()),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn jetstream_queue_subscribe() {
    let s = util::run_server("tests/configs/jetstream.conf");
    let nc = nats_aflowt::connect(&s.client_url()).await.unwrap();
    let js = nats_aflowt::jetstream::new(nc);

    js.add_stream(&StreamConfig {
        name: "TEST".to_string(),
        subjects: vec![
            "foo".to_string(),
            "bar".to_string(),
            "baz".to_string(),
            "foo.*".to_string(),
        ],
        ..Default::default()
    })
    .await
    .unwrap();
    js.stream_info("TEST").await.unwrap();

    for _ in 0..10 {
        js.publish("bar", b"hello js").await.unwrap();
    }

    // Create a queue group on "bar" with no explicit durable name, which
    // means that the queue name will be used as the durable name.
    let sub1 = js.queue_subscribe("bar", "v0").await.unwrap();

    // Since the above JS consumer is created on subject "bar", trying to
    // add a member to the same group but on subject "baz" should fail.
    js.queue_subscribe("baz", "v0").await.unwrap_err();

    // If the queue group is different, but we try to attach to the existing
    // JS consumer that is created for group "v0", then this should fail.
    js.queue_subscribe_with_options(
        "bar",
        "v1",
        &SubscribeOptions::new().durable_name("v0".to_string()),
    )
    .await
    .unwrap_err();

    // However, if a durable name is specified, creating a queue sub with
    // the same queue name is ok, but will feed from a different JS consumer.
    let sub2 = js
        .queue_subscribe_with_options(
            "bar",
            "v0",
            &SubscribeOptions::new().durable_name("other_queue_durable".to_string()),
        )
        .await
        .unwrap();

    let msg = sub1.next().await.unwrap();
    msg.ack().await.unwrap();
    assert_eq!(msg.data, b"hello js");

    let msg = sub2.next_timeout(Duration::from_secs(1)).await.unwrap();
    msg.ack().await.unwrap();
    assert_eq!(msg.data, b"hello js");

    sub1.unsubscribe().await.unwrap();
    sub2.unsubscribe().await.unwrap();
}

#[tokio::test]
async fn jetstream_flow_control() {
    let s = util::run_server("tests/configs/jetstream.conf");
    let nc = nats_aflowt::connect(&s.client_url()).await.unwrap();
    let js = nats_aflowt::jetstream::new(nc);

    js.add_stream(&StreamConfig {
        name: "TEST".to_string(),
        subjects: vec![
            "foo".to_string(),
            "bar".to_string(),
            "baz".to_string(),
            "foo.*".to_string(),
        ],
        ..Default::default()
    })
    .await
    .unwrap();

    js.stream_info("TEST").await.unwrap();

    let sub = js
        .subscribe_with_options(
            "foo",
            &SubscribeOptions::new()
                .durable_name("foo".to_string())
                .deliver_subject("fs".to_string())
                .idle_heartbeat(Duration::from_millis(300))
                .enable_flow_control(),
        )
        .await
        .unwrap();

    let info = sub.consumer_info().await.unwrap();
    assert!(info.config.flow_control);

    // Publish a some messages
    let data = b"hello";
    for _ in 0..250 {
        js.publish("foo", data).await.unwrap();
    }

    // Wait for a second to force a build up of idle heartbeats and a flow control to be
    std::thread::sleep(Duration::from_secs(1));

    // Make sure no control messages make it through `next`
    for _ in 0..250 {
        let message = sub.next().await.unwrap();
        assert_eq!(message.data, data);
    }
}

#[tokio::test]
async fn jetstream_ordered() {
    let s = util::run_server("tests/configs/jetstream.conf");
    let nc = nats_aflowt::connect(&s.client_url()).await.unwrap();
    let js = nats_aflowt::jetstream::new(nc);

    js.add_stream(&StreamConfig {
        name: "TEST".to_string(),
        subjects: vec![
            "foo".to_string(),
            "bar".to_string(),
            "baz".to_string(),
            "foo.*".to_string(),
        ],
        ..Default::default()
    })
    .await
    .unwrap();

    js.stream_info("TEST").await.unwrap();

    let sub = js
        .subscribe_with_options("foo", &SubscribeOptions::ordered().deliver_all())
        .await
        .unwrap();

    let info = sub.consumer_info().await.unwrap();
    assert!(info.config.flow_control);

    for i in 0..250 {
        js.publish("foo", (i as i64).to_be_bytes()).await.unwrap();
    }

    for i in 0..250 {
        let message = sub.next().await.unwrap();
        assert_eq!(message.data, (i as i64).to_be_bytes());
    }
}
