mod util;
pub use util::*;

#[tokio::test]
async fn pub_perms() {
    use std::time::Duration;

    let s = util::run_server("tests/configs/perms.conf");
    let (dtx, mut drx) = tokio::sync::mpsc::channel(1);
    let (etx, mut erx) = tokio::sync::mpsc::channel(1);

    let nc = nats::Options::with_user_pass("derek", "s3cr3t!")
        .error_callback(move |err| {
            let etx = etx.clone();
            let _ = tokio::spawn(async move {
                etx.send(err).await.unwrap();
            });
        })
        .disconnect_callback(move || {
            let dtx = dtx.clone();
            let _ = tokio::spawn(async move {
                dtx.send(true).await.unwrap();
            });
        })
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
        .error_callback(move |err| {
            let etx = etx.clone();
            let _ = tokio::spawn(async move {
                etx.send(err).await.unwrap();
            });
        })
        .disconnect_callback(move || {
            let dtx = dtx.clone();
            let _ = tokio::spawn(async move {
                dtx.send(true).await.unwrap();
            });
        })
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
