// Copyright 2020-2022 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::Stream;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::jetstream::{AckPolicy, ConsumerInfo, ConsumerOwnership, JetStream};
use crate::message::Message;
use crate::DEFAULT_FLUSH_TIMEOUT;

#[derive(Debug)]
pub(crate) struct Inner {
    /// Subscription ID.
    pub(crate) sid: Arc<AtomicU64>,

    /// MSG operations received from the server.
    pub(crate) messages: crate::SubscriptionReceiver<Message>,

    /// Name of the stream associated with the subscription.
    pub(crate) stream: String,

    /// Name of the consumer associated with the subscription.
    pub(crate) consumer: String,

    /// Ack policy used in while processing messages.
    pub(crate) consumer_ack_policy: AckPolicy,

    /// Indicates if we own the consumer and are responsible for deleting it or not.
    pub(crate) consumer_ownership: ConsumerOwnership,

    /// Client associated with subscription.
    pub(crate) context: JetStream,
}

impl Drop for Inner {
    fn drop(&mut self) {
        let client = self.context.connection.0.client.clone();
        let sid = self.sid.clone();
        let context = if self.consumer_ownership == ConsumerOwnership::Yes {
            Some((
                self.context.clone(),
                self.stream.clone(),
                self.consumer.clone(),
            ))
        } else {
            None
        };
        let _ = tokio::spawn(async move {
            client.unsubscribe(sid.load(Ordering::Relaxed)).await.ok();
            // Delete the consumer, if we own it.
            if let Some((context, stream, consumer)) = context {
                context.delete_consumer(&stream, &consumer).await.ok();
            }
        });
    }
}

/// A `PushSubscription` receives `Message`s published
/// to specific NATS `Subject`s.
#[derive(Clone, Debug)]
pub struct PushSubscription(pub(crate) Arc<Inner>);

impl PushSubscription {
    /// Creates a subscription.
    pub(crate) fn new(
        sid: Arc<AtomicU64>,
        consumer_info: ConsumerInfo,
        consumer_ownership: ConsumerOwnership,
        messages: crate::SubscriptionReceiver<Message>,
        context: JetStream,
    ) -> PushSubscription {
        PushSubscription(Arc::new(Inner {
            sid,
            stream: consumer_info.stream_name,
            consumer: consumer_info.name,
            consumer_ack_policy: consumer_info.config.ack_policy,
            consumer_ownership,
            messages,
            context,
        }))
    }

    /// Preprocesses the given message.
    /// Returns true if the message was processed and should be filtered out from the user's view.
    async fn should_skip(&self, message: &Message) -> bool {
        if message.is_flow_control() {
            message.respond(b"").await.ok();
            return true;
        }

        if message.is_idle_heartbeat() {
            return true;
        }

        false
    }

    /// Get the next message non-protocol message, or None if the subscription has been
    /// unsubscribed or the connection closed.
    ///
    /// # Example
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// # let client = nats_aflowt::connect("127.0.0.1:14222").await?;
    /// # let context = nats_aflowt::jetstream::new(client);
    /// # let name=format!("next_{}", rand::random::<u64>());
    /// # context.add_stream(name.as_str()).await?;
    /// # context.publish(&name, "hello").await?;
    /// # let subscription = context.subscribe(&name).await?;
    /// if let Some(message) = subscription.next().await {
    ///     println!("Received: '{}'", message);
    /// } else {
    ///     println!("No more messages");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn next(&self) -> Option<Message> {
        loop {
            match self.0.messages.recv().await {
                Some(message) => {
                    if self.should_skip(&message).await {
                        continue;
                    }
                    return Some(message);
                }
                None => return None,
            }
        }
    }

    /// Try to get the next non-protocol message, or None if no messages
    /// are present or if the subscription has been unsubscribed
    /// or the connection closed.
    ///
    /// # Example
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// # let client = nats_aflowt::connect("127.0.0.1:14222").await?;
    /// # let context = nats_aflowt::jetstream::new(client);
    /// #
    /// # context.add_stream("try_next").await?;
    /// # let subscription = context.subscribe("try_next").await?;
    /// if let Some(message) = subscription.try_next().await {
    ///     println!("Received {}", message);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn try_next(&self) -> Option<Message> {
        loop {
            match self.0.messages.try_recv().await {
                Some(message) => {
                    if self.should_skip(&message).await {
                        continue;
                    }
                    return Some(message);
                }
                None => return None,
            }
        }
    }

    /// Get the next message, or a timeout error
    /// if no messages are available for timout.
    ///
    /// # Example
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// # let client = nats_aflowt::connect("127.0.0.1:14222").await?;
    /// # let context = nats_aflowt::jetstream::new(client);
    /// # let name = format!("sub_{}", rand::random::<u64>());
    /// # context.add_stream(name.as_str()).await?;
    /// # let subscription = context.subscribe(name.as_str()).await?;
    /// if let Ok(message) = subscription.next_timeout(std::time::Duration::from_secs(1)).await {
    ///     println!("Received {}", message);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn next_timeout(&self, timeout: Duration) -> io::Result<Message> {
        loop {
            match tokio::time::timeout(timeout, self.0.messages.recv()).await {
                Ok(Some(message)) => {
                    if self.should_skip(&message).await {
                        continue;
                    }
                    return Ok(message);
                }
                Ok(None) => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "next_timeout: timed out",
                    ))
                }
                Err(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "next_timeout: unsubscribed",
                    ))
                }
            }
        }
    }

    /// Returns a pinned message stream.
    /// same as `stream()`
    ///
    /// # Example
    ///
    /// ```no_run
    /// use futures::stream::StreamExt;
    /// #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// # let client = nats_aflowt::connect("127.0.0.1:14222").await?;
    /// # let context = nats_aflowt::jetstream::new(client);
    /// # context.add_stream("messages-100").await?;
    /// let mut subscription = context.subscribe("messages-100").await?.messages();
    /// while let Some(message) = subscription.next().await {
    ///     println!("Received message {:?}", message);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn messages(self) -> Pin<Box<dyn Stream<Item = Message>>> {
        Box::pin(self.into_stream())
    }

    // convert the to unpinned stream
    #[doc(hidden)]
    fn into_stream(self) -> impl Stream<Item = Message> {
        async_stream::stream! {
            while let Some(message) = self.next().await {
                yield message;
            }
        }
    }

    /// Returns a pinned message stream.
    /// Same as `messages`
    ///
    /// # Example
    ///
    /// ```no_run
    /// use futures::stream::StreamExt;
    /// #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// # let client = nats_aflowt::connect("127.0.0.1:14222").await?;
    /// # let context = nats_aflowt::jetstream::new(client);
    /// # context.add_stream("stream").await?;
    /// let mut sub = context.subscribe("stream").await?.stream();
    /// # context.publish("stream", "hello").await?;
    /// while let Some(message) = sub.next().await {
    ///     println!("Received message {:?}", message);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn stream(self) -> Pin<Box<dyn Stream<Item = Message>>> {
        Box::pin(self.into_stream())
    }

    /// Attach a closure to handle messages. This closure will execute in a
    /// separate thread. The result of this call is a `Handler` which can
    /// not be iterated and must be unsubscribed or closed directly to
    /// unregister interest. A `Handler` will not unregister interest with
    /// the server when `drop(&mut self)` is called.
    ///
    /// # Example
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// # let client = nats_aflowt::connect("127.0.0.1:14222").await?;
    /// # let context = nats_aflowt::jetstream::new(client);
    /// # context.add_stream("with_handler").await?;
    /// context.subscribe("with_handler").await?.with_handler(move |message| {
    ///     println!("received {}", &message);
    ///     Ok(())
    /// });
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_handler<F>(self, handler: F) -> Handler
    where
        F: Fn(Message) -> io::Result<()> + Send + 'static,
    {
        // This will allow us to not have to capture the return. When it is
        // dropped it will not unsubscribe from the server.
        let sub = self.clone();
        thread::Builder::new()
            .name(format!(
                "nats_jetstream_push_subscriber_{}_{}",
                self.0.stream, self.0.consumer,
            ))
            .spawn(move || {
                futures::executor::block_on(async {
                    while let Some(m) = sub.next().await {
                        if let Err(e) = handler(m) {
                            // TODO(dlc) - Capture for last error?
                            log::error!("Error in callback! {:?}", e);
                        }
                    }
                })
            })
            .expect("threads should be spawn-able");
        Handler { subscription: self }
    }

    /// Attach an async closure to handle messages. The closure will run as a task
    /// within the current thread and must not be blocking.
    /// Any errors returned by the closure will be logged.
    /// A `Handler` will not unregister interest with
    /// the server when `drop(&mut self)` is called.
    ///
    /// # Example
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// # let nc = nats_aflowt::connect("127.0.0.1:14222").await?;
    /// let sub = nc.subscribe("foo").await?
    ///      .with_async_handler( move |m| async move { m.respond("ans=42").await?; Ok(()) });
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn with_async_handler<F, T>(self, handler: F) -> Self
    where
        F: Fn(Message) -> T + 'static + Send + Sync,
        T: futures::Future<Output = io::Result<()>> + Send,
    {
        let sub = self.clone();
        tokio::spawn(async move {
            while let Some(m) = sub.next().await {
                if let Err(e) = handler(m).await {
                    // TODO(dlc) - Capture for last error?
                    log::error!("Error in callback! {:?}", e);
                }
            }
        });
        self
    }

    /// Attach a closure to process and acknowledge messages. This closure will execute in a separate thread.
    ///
    /// The result of this call is a `Handler`
    /// which can not be iterated and must be unsubscribed or closed directly to
    /// unregister interest. A `Handler` will not unregister interest with
    /// the server when `drop(&mut self)` is called.
    ///
    /// # Example
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// # let client = nats_aflowt::connect("127.0.0.1:14222").await?;
    /// # let context = nats_aflowt::jetstream::new(client);
    /// # context.add_stream("with_process_handler").await;
    /// context.subscribe("with_process_handler").await?.with_process_handler(|message| {
    ///     println!("Received {}", &message);
    ///     Ok(())
    /// });
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_process_handler<F>(self, handler: F) -> Handler
    where
        F: Fn(&Message) -> io::Result<()> + Send + 'static,
    {
        let consumer_ack_policy = self.0.consumer_ack_policy;

        // This will allow us to not have to capture the return. When it is
        // dropped it will not unsubscribe from the server.
        let sub = self.clone();
        thread::Builder::new()
            .name(format!(
                "nats_push_subscriber_{}_{}",
                self.0.consumer, self.0.stream
            ))
            .spawn(move || {
                futures::executor::block_on(async {
                    while let Some(message) = sub.next().await {
                        if let Err(err) = handler(&message) {
                            log::error!("Error in callback! {:?}", err);
                        }

                        if consumer_ack_policy != AckPolicy::None {
                            if let Err(err) = message.ack().await {
                                log::error!("Error in callback! {:?}", err);
                            }
                        }
                    }
                })
            })
            .expect("threads should be spawnable");
        Handler { subscription: self }
    }

    /// Process and acknowledge a single message, waiting indefinitely for
    /// one to arrive.
    ///
    /// Does not acknowledge the processed message if the closure returns an `Err`.
    ///
    /// Example
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// # let client = nats_aflowt::connect("127.0.0.1:14222").await?;
    /// # let context = nats_aflowt::jetstream::new(client);
    /// # let sub_name = format!("process_{}", rand::random::<u64>());
    /// # context.add_stream(sub_name.as_str()).await?;
    /// # let mut subscription = context.subscribe(&sub_name).await?;
    /// # context.publish(&sub_name, "hello").await?;
    /// #
    /// subscription.process(|message| {
    ///     println!("Received message {:?}", message);
    ///     Ok(())
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn process<R, F: Fn(&Message) -> io::Result<R>>(&mut self, f: F) -> io::Result<R> {
        if let Some(next) = self.next().await {
            let result = f(&next)?;
            if self.0.consumer_ack_policy != AckPolicy::None {
                next.ack().await?;
            }
            Ok(result)
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "process: unsubscribed",
            ))
        }
    }

    /// Process and acknowledge a single message, waiting up to timeout configured `timeout` before
    /// returning a timeout error.
    ///
    /// Does not ack the processed message if the internal closure returns an `Err`.
    ///
    /// Example
    ///
    /// ```
    /// # use std::time::Duration;
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// # let client = nats_aflowt::connect("127.0.0.1:14222").await?;
    /// # let context = nats_aflowt::jetstream::new(client);
    /// # context.add_stream("process_timeout").await?;
    /// # context.publish("process_timeout", "hello").await?;
    /// # context.publish("process_timeout", "hello2").await?;
    /// #
    /// let mut subscription = context.subscribe("process_timeout").await?;
    /// subscription.process_timeout(Duration::from_secs(1), |message| {
    ///     println!("Received message {:?}", message);
    ///     Ok(())
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn process_timeout<R, F: Fn(&Message) -> io::Result<R>>(
        &mut self,
        timeout: Duration,
        f: F,
    ) -> io::Result<R> {
        let next = self.next_timeout(timeout).await?;

        let ret = f(&next)?;
        if self.0.consumer_ack_policy != AckPolicy::None {
            next.ack().await?;
        }

        Ok(ret)
    }

    /// Sends a request to fetch current information about the target consumer.
    ///
    /// # Example
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// # let client = nats_aflowt::connect("127.0.0.1:14222").await?;
    /// # let context = nats_aflowt::jetstream::new(client);
    /// #
    /// # context.add_stream("foo").await?;
    /// let subscription = context.subscribe("foo").await?;
    /// //let info = subscription.consumer_info().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn consumer_info(&self) -> io::Result<ConsumerInfo> {
        self.0
            .context
            .consumer_info(&self.0.stream, &self.0.consumer)
            .await
    }

    /// Unsubscribe a subscription immediately without draining.
    /// Use `drain` instead if you want any pending messages
    /// to be processed by a handler, if one is configured.
    ///
    /// # Example
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// # let client = nats_aflowt::connect("127.0.0.1:14222").await?;
    /// # let context = nats_aflowt::jetstream::new(client);
    /// # context.add_stream("unsubscribe").await?;
    /// #
    /// let subscription = context.subscribe("unsubscribe").await?;
    /// subscription.unsubscribe().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn unsubscribe(self) -> io::Result<()> {
        // Drain
        self.0
            .context
            .connection
            .0
            .client
            .flush(DEFAULT_FLUSH_TIMEOUT)
            .await?;

        self.0
            .context
            .connection
            .0
            .client
            .unsubscribe(self.0.sid.load(Ordering::Relaxed))
            .await?;

        // Discard all queued messages.
        while self.0.messages.try_recv().await.is_some() {}

        // Delete the consumer, if we own it.
        if self.0.consumer_ownership == ConsumerOwnership::Yes {
            self.0
                .context
                .delete_consumer(&self.0.stream, &self.0.consumer)
                .await
                .ok();
        }

        Ok(())
    }

    /// Close a subscription. Same as `unsubscribe`
    ///
    /// Use `drain` instead if you want any pending messages
    /// to be processed by a handler, if one is configured.
    ///
    /// # Example
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = nats_aflowt::connect("127.0.0.1:14222").await?;
    /// # let context = nats_aflowt::jetstream::new(client);
    /// # context.add_stream("close").await?;
    /// let subscription = context.subscribe("close").await?;
    /// subscription.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn close(self) -> io::Result<()> {
        self.unsubscribe().await
    }

    /// Send an unsubscription then flush the connection,
    /// allowing any unprocessed messages to be handled
    /// by a handler function if one is configured.
    ///
    /// After the flush returns, we know that a round-trip
    /// to the server has happened after it received our
    /// unsubscription, so we shut down the subscriber
    /// afterwards.
    ///
    /// A similar method exists on the `Connection` struct
    /// which will drain all subscriptions for the NATS
    /// client, and transition the entire system into
    /// the closed state afterward.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::time::Duration;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = nats_aflowt::connect("127.0.0.1:14222").await?;
    /// # let context = nats_aflowt::jetstream::new(client);
    /// # let sub_name = format!("drain_{}", rand::random::<u32>());
    /// # context.add_stream(sub_name.as_str()).await?;
    /// # let mut subscription = context.subscribe(&sub_name).await?;
    /// # context.publish(&sub_name, "foo").await?;
    /// # context.publish(&sub_name, "bar").await?;
    /// # context.publish(&sub_name, "baz").await?;
    ///
    /// subscription.drain().await?;
    ///
    /// # // there are no more messages in subscription
    /// # assert!(subscription.next_timeout(Duration::from_secs(2)).await.is_err());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn drain(&mut self) -> io::Result<()> {
        // Unsubscribe
        self.0
            .context
            .connection
            .0
            .client
            .flush(DEFAULT_FLUSH_TIMEOUT)
            .await?;

        self.0
            .context
            .connection
            .0
            .client
            .unsubscribe(self.0.sid.load(Ordering::Relaxed))
            .await?;

        // Discard all queued messages.
        while self.0.messages.try_recv().await.is_some() {}

        // Delete the consumer, if we own it.
        if self.0.consumer_ownership == ConsumerOwnership::Yes {
            self.0
                .context
                .delete_consumer(&self.0.stream, &self.0.consumer)
                .await
                .ok();
        }

        Ok(())
    }
}

/// A `Handler` may be used to unsubscribe a handler thread.
pub struct Handler {
    subscription: PushSubscription,
}

impl Handler {
    /// Unsubscribe a subscription.
    ///
    /// # Example
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// # let nc = nats_aflowt::connect("127.0.0.1:14222").await?;
    /// let sub = nc.subscribe("foo").await?.with_handler(move |msg| {
    ///     println!("Received {}", &msg);
    ///     Ok(())
    /// });
    /// sub.unsubscribe().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn unsubscribe(&mut self) -> io::Result<()> {
        self.subscription.drain().await
    }
}
