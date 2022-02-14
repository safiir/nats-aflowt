// example taken from doc for kv::Store::history
//
use futures::stream::StreamExt;
#[cfg(not(feature = "otel"))]
use log::info;
use nats_aflowt::kv::Config;
#[cfg(feature = "otel")]
use tracing::info;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    nats_aflowt::init_tracing();
    info!("hello from kv_history");
    let client = nats_aflowt::connect("127.0.0.1:4222").await?;
    let context = nats_aflowt::jetstream::new(client);

    let bucket = context
        .create_key_value(&Config {
            bucket: "history_iter".to_string(),
            history: 2,
            ..Default::default()
        })
        .await?;

    bucket.put("foo", b"fizz").await?;
    bucket.put("foo", b"buzz").await?;

    let mut history = bucket.history("foo").await?;

    let next = history.next().await.unwrap();
    assert_eq!(next.key, "foo".to_string());
    assert_eq!(next.value, b"fizz");

    let next = history.next().await.unwrap();
    assert_eq!(next.key, "foo".to_string());
    assert_eq!(next.value, b"buzz");

    context.delete_key_value("history_iter").await?;

    println!("done");
    Ok(())
}
