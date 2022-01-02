mod util;
use nats::{AsyncCall, AsyncErrorCallback, BoxFuture};
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
    fn call(&self, _si: nats::ServerInfo, err: std::io::Error) -> BoxFuture<()> {
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

    let nc = nats::Options::with_user_pass("derek", "s3cr3t!")
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

    let nc = nats::Options::with_user_pass("derek", "s3cr3t!")
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
