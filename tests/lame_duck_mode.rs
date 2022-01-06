mod util;
use nats::AsyncCall;
use nats::BoxFuture;
pub use util::*;

#[derive(Clone)]
struct LDCallback {
    ltx: tokio::sync::mpsc::Sender<bool>,
}

impl<'a> AsyncCall for LDCallback {
    fn call(&self) -> BoxFuture<()> {
        let ltx = self.ltx.clone();
        Box::pin(async move {
            ltx.send(true).await.unwrap();
        })
    }
}

#[tokio::test]
#[cfg_attr(target_os = "windows", ignore)]
async fn lame_duck_mode() {
    use std::time::Duration;

    let (ltx, mut lrx) = tokio::sync::mpsc::channel(1);

    let s = util::run_basic_server();
    let nc = nats::Options::new()
        .lame_duck_callback(LDCallback { ltx: ltx.clone() })
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
