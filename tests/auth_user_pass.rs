mod util;
pub use util::*;

#[tokio::test]
async fn basic_user_pass_auth() {
    let s = util::run_server("tests/configs/user_pass.conf");

    assert!(nats::connect(&s.client_url()).await.is_err());

    assert!(nats::Options::with_user_pass("derek", "s3cr3t")
        .connect(&s.client_url())
        .await
        .is_ok());

    assert!(nats::connect(&s.client_url_with("derek", "s3cr3t"))
        .await
        .is_ok());

    // Check override.
    assert!(nats::Options::with_user_pass("derek", "bad-password")
        .connect(&s.client_url_with("derek", "s3cr3t"))
        .await
        .is_ok());
}
