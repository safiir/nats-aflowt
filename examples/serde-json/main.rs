use pin_utils::pin_mut;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct Person {
    first_name: String,
    last_name: String,
    age: u8,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    use futures::prelude::*;
    let nc = nats::connect("demo.nats.io").await.expect("demo.nats.io");
    let subj = nc.new_inbox();

    let p = Person {
        first_name: "derek".to_owned(),
        last_name: "collison".to_owned(),
        age: 22,
    };

    let sub = nc.subscribe(&subj).await?;
    nc.publish(&subj, serde_json::to_vec(&p)?).await?;

    let p2 = sub.stream().map(move |msg| {
        let p: Person = serde_json::from_slice(&msg.data).unwrap();
        p
    });
    pin_mut!(p2);
    println!("received {:?}", p2.next().await.unwrap());

    Ok(())
}
