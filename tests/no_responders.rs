mod util;
pub use util::*;

#[tokio::test]
async fn no_responders() {
    let s = util::run_basic_server();
    let nc = nats::connect(&s.client_url())
        .await
        .expect("could not connect");
    let res = nc.request("nobody-home", "hello").await;
    assert!(res.is_err());
    let err = res.err().unwrap();
    assert!(err.to_string().contains("no responders"), "{}", err);
}
