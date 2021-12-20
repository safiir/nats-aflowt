use std::time::Duration;

use crossbeam_channel::bounded;

mod util;
pub use util::*;

#[tokio::test]
async fn pub_perms() {
    let s = util::run_server("tests/configs/perms.conf");

    let (dtx, drx) = bounded(1);
    let (etx, erx) = bounded(1);

    let nc = nats::Options::with_user_pass("derek", "s3cr3t!")
        .error_callback(move |err| etx.send(err).unwrap())
        .disconnect_callback(move || {
            let _ = dtx.send(true);
        })
        .connect(&s.client_url())
        .await
        .expect("could not connect");

    nc.publish("foo", "NOT ALLOWED").await.unwrap();

    let r = erx.recv_timeout(Duration::from_millis(100));
    assert!(r.is_ok(), "expected an error callback, got none");
    assert_eq!(
        r.unwrap().to_string(),
        r#"Permissions Violation for Publish to "foo""#
    );

    let r = drx.recv_timeout(Duration::from_millis(100));
    assert!(r.is_err(), "we got disconnected on perm violation");
}

#[tokio::test]
async fn sub_perms() {
    let s = util::run_server("tests/configs/perms.conf");

    let (dtx, drx) = bounded(1);
    let (etx, erx) = bounded(1);

    let nc = nats::Options::with_user_pass("derek", "s3cr3t!")
        .error_callback(move |err| etx.send(err).unwrap())
        .disconnect_callback(move || {
            let _ = dtx.send(true);
        })
        .connect(&s.client_url())
        .await
        .expect("could not connect");

    let _sub = nc.subscribe("foo").await.unwrap();

    let r = erx.recv_timeout(Duration::from_millis(100));
    assert!(r.is_ok(), "expected an error callback, got none");
    assert_eq!(
        r.unwrap().to_string(),
        r#"Permissions Violation for Subscription to "foo""#
    );

    let r = drx.recv_timeout(Duration::from_millis(100));
    assert!(r.is_err(), "we got disconnected on perm violation");
}
