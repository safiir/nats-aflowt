// example taken from doc for PushSubscription::drain
//
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sub_name = format!("drain_{}", rand::random::<u32>());
    let client = nats_aflowt::connect("127.0.0.1:14222").await?;
    let context = nats_aflowt::jetstream::new(client);
    context.add_stream(sub_name.as_str()).await?;
    let mut subscription = context.subscribe("drain199").await?;
    context.publish(&sub_name, "foo").await?;
    context.publish(&sub_name, "bar").await?;
    context.publish(&sub_name, "baz").await?;

    subscription.drain().await?;

    assert!(subscription.next().await.is_none(), "subscription ended");

    println!("done");
    Ok(())
}
