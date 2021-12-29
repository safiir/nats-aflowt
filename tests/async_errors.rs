mod util;
pub use util::*;

/*
// TODO: FIXME: fix compilation
#[tokio::test]
async fn pub_perms() {
    use std::time::Duration;

    let s = util::run_server("tests/configs/perms.conf");
    let (dtx, drx) = tokio::sync::oneshot::channel();
    let (etx, erx) = tokio::sync::oneshot::channel();

    let nc = nats::Options::with_user_pass("derek", "s3cr3t!")
        .error_callback(move |err| etx.send(err).unwrap())
        .disconnect_callback(move || {
            let _ = dtx.send(true);
        })
        .connect(&s.client_url())
        .await
        .expect("could not connect");

    nc.publish("foo", "NOT ALLOWED").await.unwrap();
    let r = tokio::time::timeout(Duration::from_millis(100), erx.recv())
        .await
        .expect("no timeout");
    assert!(r.is_ok(), "expected an error callback, got none");
    assert_eq!(
        r.unwrap().to_string(),
        r#"Permissions Violation for Publish to "foo""#
    );

    let r = tokio::time::timeout(Duration::from_millis(100), drx.recv())
        .await
        .expect("no timeout");
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
 */
