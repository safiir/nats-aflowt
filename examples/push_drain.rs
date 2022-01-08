use nats_aflowt as nats;

// example taken from doc for PushSubscription::drain
//
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sub_name = format!("drain_{}", rand::random::<u32>());
    let client = nats::connect("demo.nats.io").await?;
    let context = nats::jetstream::new(client);
    context.add_stream(sub_name.as_str()).await?;
    let mut subscription = context.subscribe("drain199").await?;
    context.publish(&sub_name, "foo").await?;
    context.publish(&sub_name, "bar").await?;
    context.publish(&sub_name, "baz").await?;

    subscription.drain().await?;

    assert!(subscription.next().await.is_none());

    println!("done");
    Ok(())
}
