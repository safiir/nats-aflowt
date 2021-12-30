mod util;
use std::time::Duration;
pub use util::*;

#[tokio::test]
#[cfg_attr(target_os = "windows", ignore)]
async fn lame_duck_mode() {
    let (ltx, mut lrx) = tokio::sync::mpsc::channel(1);

    let s = util::run_basic_server();
    let nc = nats::Options::new()
        .lame_duck_callback(move || {
            let ltx = ltx.clone();
            let _ = tokio::spawn(async move {
                ltx.send(true).await.unwrap();
            });
        })
        .connect(s.client_url().as_str())
        .await
        .expect("could not connect to the server");
    let _sub = nc.subscribe("foo").await.unwrap();
    set_lame_duck_mode();
    let r = tokio::time::timeout(Duration::from_millis(500), lrx.recv()).await;
    assert!(r.is_ok(), "expected lame duck response, got timeout");
    let r = r.unwrap();
    assert!(
        r.is_some(),
        "expected lame duck response, got stream closed"
    );
}
