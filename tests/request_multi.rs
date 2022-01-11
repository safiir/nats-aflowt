use std::io;
use std::time::Duration;

const RESPONSE_TIMEOUT: Duration = Duration::from_secs(1);

#[tokio::test]
async fn request_multi() -> io::Result<()> {
    let nc = nats_aflowt::connect("127.0.0.1:14222").await?;
    nc.subscribe("foo")
        .await?
        //.with_async_handler(async move |m| {
        .with_async_handler(move |m| async move {
            m.respond(b"ans=42").await?;
            Ok(())
        });
    let sub = nc.request_multi("foo", "What is the answer?").await?;

    loop {
        match sub.next_timeout(RESPONSE_TIMEOUT).await {
            Err(e) => {
                panic!("timed out with err {}", e);
            }
            Ok(msg) => {
                println!("Received: {}", msg);
                if msg.data == b"ans=42" {
                    break;
                }
            }
        }
    }

    Ok(())
}
