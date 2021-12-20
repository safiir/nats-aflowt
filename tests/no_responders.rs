mod util;
pub use util::*;

#[tokio::test]
#[should_panic(expected = "no responders")]
async fn no_responders() {
    let s = util::run_basic_server();
    let nc = nats::connect(&s.client_url())
        .await
        .expect("could not connect");
    nc.request("nobody-home", "hello").await.unwrap();
}
