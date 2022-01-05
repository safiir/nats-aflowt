// Copyright 2020-2021 The NATS Authors
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

//use async_trait::async_trait;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::io::{self, Error, ErrorKind};
use std::mem;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

//use crossbeam_channel as channel;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::{io::BufReader, io::BufWriter, sync::Mutex};

use crate::connector::{Connector, NatsStream};
use crate::message::Message;
use crate::proto::{self, ClientOp, ServerOp};
use crate::BoxFuture;
use crate::{header::HeaderMap, inject_delay, inject_io_failure, Options, ServerInfo};

const BUF_CAPACITY: usize = 32 * 1024;
/// maximum messages to queue before applying backpressure
const MAX_SUBSCRIPTION_QUEUE: usize = 650;

/// Client state.
///
/// NB: locking protocol - writes must ALWAYS be locked
///     first and released after when both are used.
///     Failure to follow this strict rule WILL create
///     a deadlock!
struct State {
    write: Mutex<WriteState>,
    read: Mutex<ReadState>,
    meta: Mutex<MetaState>,
}

struct MetaState {
    /// Set of subjects that are currently muted.
    mutes: HashSet<u64>,
}

struct WriteState {
    /// Buffered writer with an active connection.
    ///
    /// When `None`, the client is either reconnecting or closed.
    writer: Option<BufWriter<NatsStream>>,

    /// Signals to the client thread that the writer needs a flush.
    flush_kicker: tokio::sync::mpsc::Sender<()>,

    /// The reconnect buffer.
    ///
    /// When the client is reconnecting, PUB messages get buffered here. When
    /// the connection is re-established, contents of the buffer are
    /// flushed to the server.
    buffer: Buffer,

    /// Next subscription ID.
    next_sid: u64,
}

struct ReadState {
    /// Current subscriptions.
    subscriptions: HashMap<u64, Subscription>,

    /// Expected pongs and their notification channels.
    pongs: VecDeque<tokio::sync::mpsc::Sender<()>>,

    /// Tracks the last activity from the server.
    last_active: Instant,

    /// Used for client side monitoring of connection health.
    pings_out: u8,
}

/// A handler for preprocess messages for a subscription as they arrive over the wire.
//#[async_trait]
//pub(crate) trait Preprocessor: Send + Sync {
//    async fn process(&self, sid: u64, msg: &Message) -> bool;
//}
//#[async_trait]
pub(crate) trait Preprocessor: Send + Sync {
    fn process<'proc>(&'proc self, sid: u64, msg: &'proc Message) -> BoxFuture<'proc, bool>;
}

#[derive(Debug, Default, Clone)]
struct NoProcessing {}
//#[async_trait]
//impl<'p> Preprocessor for NoProcessing {
//    async fn process(&self, _sid: u64, _msg: &Message) -> bool {
//        false
//    }
//}
//#[async_trait]
impl<'p> Preprocessor for NoProcessing {
    fn process(&self, _sid: u64, _msg: &Message) -> BoxFuture<'static, bool> {
        Box::pin(async { false })
    }
}

/// A registered subscription.
struct Subscription {
    subject: String,
    queue_group: Option<String>,
    messages: tokio::sync::mpsc::Sender<Message>,
    preprocess: Pin<Box<dyn Preprocessor>>,
}

/// A NATS client.
#[derive(Clone)]
pub struct Client {
    /// Shared client state.
    state: Arc<State>,

    /// Server info provided by the last INFO message.
    pub(crate) server_info: Arc<Mutex<ServerInfo>>,

    /// Set to `true` if shutdown has been requested.
    shutdown: Arc<AtomicBool>,

    /// The options that this `Client` was created using.
    pub(crate) options: Arc<Options>,
}

impl Client {
    /// Creates a new client that will begin connecting in the background.
    pub(crate) async fn connect(url: &str, options: Options) -> io::Result<Client> {
        // A channel for coordinating flushes.
        //let (flush_kicker, flush_wanted) = channel::bounded(1);
        let (flush_kicker, mut flush_wanted) = tokio::sync::mpsc::channel(1);

        // Channels for coordinating initial connect.
        let (run_sender, run_receiver) = tokio::sync::oneshot::channel();
        //let (pong_sender, pong_receiver) = channel::bounded::<()>(1);
        let (pong_sender, mut pong_receiver) = tokio::sync::mpsc::channel(1);

        // The client state.
        let _client = Client {
            state: Arc::new(State {
                meta: Mutex::new(MetaState {
                    mutes: HashSet::new(),
                }),
                write: Mutex::new(WriteState {
                    writer: None,
                    flush_kicker,
                    buffer: Buffer::new(options.reconnect_buffer_size),
                    next_sid: 1,
                }),
                read: Mutex::new(ReadState {
                    subscriptions: HashMap::new(),
                    pongs: VecDeque::from(vec![pong_sender]),
                    last_active: Instant::now(),
                    pings_out: 0,
                }),
            }),
            server_info: Arc::new(Mutex::new(ServerInfo::default())),
            shutdown: Arc::new(AtomicBool::new(false)),
            options: Arc::new(options),
        };

        let options = _client.options.clone();

        // Connector for creating the initial connection and reconnecting when
        // it is broken.
        let connector = Connector::new(url, options.clone())?;

        // Spawn the async task responsible for:
        // - Maintaining a connection to the server and reconnecting when it is
        //   broken.
        // - Reading messages from the server and processing them.
        // - Forwarding MSG operations to subscribers.
        let client = _client.clone();
        //let close_callback = &options.close_callback;
        let opt = options.clone();
        tokio::spawn(async move {
            {
                let res = client.run(connector).await;
                run_sender.send(res).ok();

                // One final flush before shutting down.
                // This way we make sure buffered published messages reach the
                // server.
                {
                    let mut write = client.state.write.lock().await;
                    if let Some(writer) = write.writer.as_mut() {
                        writer.shutdown().await.ok();
                    }
                }
                opt.close_callback.call().await;
            }
        });

        tokio::select! {
            res = run_receiver => {
                res.expect("client thread has panicked")?;
                unreachable!()
            }
            _ = pong_receiver.recv()  => { }
        };

        // Spawn a thread that periodically flushes buffered messages.
        let client = _client.clone();
        tokio::spawn(async move {
            // Track last flush/write time.
            const MIN_FLUSH_BETWEEN: Duration = Duration::from_millis(5);

            // Handle recv timeouts and check if we should send a PING.
            // TODO(dlc) - Make configurable.
            const PING_INTERVAL: Duration = Duration::from_secs(2 * 60);
            const MAX_PINGS_OUT: u8 = 2;

            let mut last = Instant::now() - MIN_FLUSH_BETWEEN;

            // Wait until at least one message is buffered.
            loop {
                if tokio::time::timeout(PING_INTERVAL, flush_wanted.recv())
                    .await
                    .is_ok()
                {
                    let since = last.elapsed();
                    if since < MIN_FLUSH_BETWEEN {
                        tokio::time::sleep(MIN_FLUSH_BETWEEN - since).await;
                    }

                    // Flush the writer.
                    let mut write = client.state.write.lock().await;
                    let mut read = client.state.read.lock().await;
                    if let Some(writer) = write.writer.as_mut() {
                        // If flushing fails, disconnect.
                        if writer.flush().await.is_err() {
                            last = Instant::now();
                            let _ = writer.shutdown().await;
                            write.writer = None;
                            read.pongs.clear();
                        }
                    }

                    // NB see locking protocol for state.write and state.read
                    drop(read);
                    drop(write);
                } else {
                    // timeout
                    let mut write = client.state.write.lock().await;
                    let mut read = client.state.read.lock().await;

                    if read.pings_out >= MAX_PINGS_OUT {
                        if let Some(writer) = write.writer.as_mut() {
                            writer.get_mut().shutdown().await;
                        }
                        write.writer = None;
                        read.pongs.clear();
                    } else if read.last_active.elapsed() > PING_INTERVAL {
                        read.pings_out += 1;
                        read.pongs.push_back(write.flush_kicker.clone());
                        // Send out a PING here.
                        if let Some(mut writer) = write.writer.as_mut() {
                            // Ok to ignore errors here.
                            let _ = proto::encode(&mut writer, ClientOp::Ping).await;
                            if writer.flush().await.is_err() {
                                // NB see locking protocol for state.write and state.read
                                writer.shutdown().await.ok();
                                write.writer = None;
                                read.pongs.clear();
                            }
                        }
                    }

                    // NB see locking protocol for state.write and state.read
                    drop(read);
                    drop(write);
                }
            }
        });

        Ok(_client)
    }

    /// Retrieves server info as received by the most recent connection.
    pub async fn server_info(&self) -> ServerInfo {
        self.server_info.lock().await.clone()
    }

    /// Makes a round trip to the server to ensure buffered messages reach it.
    pub(crate) async fn flush(&self, timeout: Duration) -> io::Result<()> {
        let mut pong = {
            // Inject random delays when testing.
            inject_delay().await;

            let mut write = self.state.write.lock().await;

            // Check if the client is closed.
            self.check_shutdown()?;

            let (sender, receiver) = tokio::sync::mpsc::channel(1);

            // If connected, send a PING.
            match write.writer.as_mut() {
                None => {}
                Some(mut writer) => {
                    // uses timeout for duration, not per-write
                    tokio::time::timeout(timeout, async {
                        if let Ok(()) = proto::encode(&mut writer, ClientOp::Ping).await {
                            let _ = writer.flush().await;
                        }
                    })
                    .await?;
                }
            }

            // Enqueue an expected PONG.
            let mut read = self.state.read.lock().await;
            read.pongs.push_back(sender);

            // NB see locking protocol for state.write and state.read
            drop(read);
            drop(write);

            receiver
        };

        // Wait until the PONG operation is received.
        match pong.recv().await {
            Some(()) => Ok(()),
            None => Err(Error::new(ErrorKind::ConnectionReset, "flush failed")),
        }
    }

    /// Closes the client.
    pub(crate) async fn close(&self) {
        // Inject random delays when testing.
        inject_delay().await;

        let mut write = self.state.write.lock().await;
        let mut read = self.state.read.lock().await;

        // Initiate shutdown process.
        if self.shutdown() {
            // Clear all subscriptions.
            let old_subscriptions = mem::take(&mut read.subscriptions);
            for (sid, _) in old_subscriptions {
                // Send an UNSUB message and ignore errors.
                if let Some(writer) = write.writer.as_mut() {
                    let max_msgs = None;
                    proto::encode(writer, ClientOp::Unsub { sid, max_msgs })
                        .await
                        .ok();
                    write.flush_kicker.try_send(()).ok();
                }
            }
            read.subscriptions.clear();

            // Flush the writer in case there are buffered messages.
            if let Some(writer) = write.writer.as_mut() {
                writer.flush().await.ok();
            }

            // Wake up all pending flushes.
            read.pongs.clear();

            // NB see locking protocol for state.write and state.read
            drop(read);
            drop(write);
        }
    }

    /// Kicks off the shutdown process, but doesn't wait for its completion.
    /// Returns true if this is the first attempt to shut down the system.
    pub(crate) fn shutdown(&self) -> bool {
        self.shutdown
            .compare_exchange(false, true, Ordering::Release, Ordering::Relaxed)
            .is_ok()
    }

    fn check_shutdown(&self) -> io::Result<()> {
        if self.shutdown.load(Ordering::Acquire) {
            Err(Error::new(ErrorKind::NotConnected, "the client is closed"))
        } else {
            Ok(())
        }
    }

    /// Subscribes to a subject.
    pub(crate) async fn subscribe(
        &self,
        subject: &str,
        queue_group: Option<&str>,
    ) -> io::Result<(u64, crate::subscription::SubscriptionReceiver<Message>)> {
        // Inject random delays when testing.
        self.subscribe_with_preprocessor(subject, queue_group, Box::pin(NoProcessing::default()))
            .await
    }

    /// Subscribe to a subject with a message preprocessor.
    pub(crate) async fn subscribe_with_preprocessor(
        &self,
        subject: &str,
        queue_group: Option<&str>,
        message_processor: Pin<Box<dyn Preprocessor>>,
    ) -> io::Result<(u64, crate::SubscriptionReceiver<Message>)> {
        inject_delay().await;

        let mut write = self.state.write.lock().await;
        let mut read = self.state.read.lock().await;

        // Check if the client is closed.
        self.check_shutdown()?;

        // Generate a subject ID.
        let sid = write.next_sid;
        write.next_sid += 1;

        // If connected, send a SUB operation.
        if let Some(writer) = write.writer.as_mut() {
            let op = ClientOp::Sub {
                subject,
                queue_group,
                sid,
            };
            proto::encode(writer, op).await?;
            write.flush_kicker.try_send(()).ok();
        }

        // Register the subscription in the hash map.
        let (sender, receiver) = tokio::sync::mpsc::channel(MAX_SUBSCRIPTION_QUEUE);
        read.subscriptions.insert(
            sid,
            Subscription {
                subject: subject.to_string(),
                queue_group: queue_group.map(ToString::to_string),
                messages: sender,
                preprocess: message_processor,
            },
        );

        // NB see locking protocol for state.write and state.read
        drop(read);
        drop(write);

        Ok((sid, receiver.into()))
    }

    /// Marks a subscription as muted.
    pub(crate) async fn mute(&self, sid: u64) -> io::Result<bool> {
        let mut meta = self.state.meta.lock().await;
        Ok(meta.mutes.insert(sid))
    }

    /// Resubscribes an existing subscription by unsubscribing from the old subject and subscribing
    /// to the new subject returning a new sid while retaining the existing channel receiver.
    pub(crate) async fn resubscribe(&self, old_sid: u64, new_subject: &str) -> io::Result<u64> {
        // Inject random delays when testing.
        inject_delay().await;

        let mut write = self.state.write.lock().await;
        let mut read = self.state.read.lock().await;

        // Check if the client is closed.
        self.check_shutdown()?;

        let subscription = read
            .subscriptions
            .remove(&old_sid)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "subscription not found"))?;

        // Generate a subject ID.
        let new_sid = write.next_sid;
        write.next_sid += 1;

        // Send an UNSUB and SUB messages.
        if let Some(writer) = write.writer.as_mut() {
            proto::encode(
                writer,
                ClientOp::Unsub {
                    sid: old_sid,
                    max_msgs: None,
                },
            )
            .await?;
        }

        let queue_group = subscription.queue_group.clone();
        read.subscriptions.insert(new_sid, subscription);

        if let Some(writer) = write.writer.as_mut() {
            proto::encode(
                writer,
                ClientOp::Sub {
                    sid: new_sid,
                    subject: new_subject,
                    queue_group: queue_group.as_deref(),
                },
            )
            .await?;
            write.flush_kicker.try_send(()).ok();
        }

        // NB see locking protocol for state.write and state.read
        drop(read);
        drop(write);

        Ok(new_sid)
    }

    /// Unsubscribes from a subject.
    pub(crate) async fn unsubscribe(&self, sid: u64) -> io::Result<()> {
        // Inject random delays when testing.
        inject_delay().await;

        let mut write = self.state.write.lock().await;
        let mut read = self.state.read.lock().await;

        // Remove the subscription from the map.
        if read.subscriptions.remove(&sid).is_none() {
            // already unsubscribed

            // NB see locking protocol for state.write and state.read
            drop(read);
            drop(write);

            return Ok(());
        }

        // Send an UNSUB message.
        if let Some(writer) = write.writer.as_mut() {
            let max_msgs = None;
            proto::encode(writer, ClientOp::Unsub { sid, max_msgs }).await?;
            write.flush_kicker.try_send(()).ok();
        }

        // NB see locking protocol for state.write and state.read
        drop(read);
        drop(write);

        Ok(())
    }

    /// Publishes a message with optional reply subject and headers.
    pub async fn publish(
        &self,
        subject: &str,
        reply_to: Option<&str>,
        headers: Option<&HeaderMap>,
        msg: &[u8],
    ) -> io::Result<()> {
        // Inject random delays when testing.
        inject_delay().await;

        let server_info = self.server_info.lock().await;
        if headers.is_some() && !server_info.headers {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "the server does not support headers",
            ));
        }
        drop(server_info);

        // Check if the client is closed.
        self.check_shutdown()?;

        let op = if let Some(headers) = headers {
            ClientOp::Hpub {
                subject,
                reply_to,
                payload: msg,
                headers,
            }
        } else {
            ClientOp::Pub {
                subject,
                reply_to,
                payload: msg,
            }
        };

        let mut write = self.state.write.lock().await;
        let written = write.buffer.written;
        match write.writer.as_mut() {
            None => {
                // If reconnecting, write into the buffer.
                proto::encode(&mut write.buffer, op).await?;
                write.buffer.flush().await?;
                Ok(())
            }
            Some(mut writer) => {
                assert_eq!(written, 0);

                // If connected, write into the writer.
                let res = proto::encode(&mut writer, op).await;

                // If writing fails, disconnect.
                if res.is_err() {
                    write.writer = None;

                    // NB see locking protocol for state.write and state.read
                    let mut read = self.state.read.lock().await;
                    read.pongs.clear();
                }

                write.flush_kicker.try_send(()).ok();

                res
            }
        }
    }

    /// Attempts to publish a message without blocking.
    ///
    /// This only works when the write buffer has enough space to encode the
    /// whole message.
    pub async fn try_publish(
        &self,
        subject: &str,
        reply_to: Option<&str>,
        headers: Option<&HeaderMap>,
        msg: &[u8],
    ) -> Option<io::Result<()>> {
        // Check if the client is closed.
        if let Err(e) = self.check_shutdown() {
            return Some(Err(e));
        }

        // Estimate how many bytes the message will consume when written into
        // the stream. We must make a conservative guess: it's okay to
        // overestimate but not to underestimate.
        let mut estimate = 1024 + subject.len() + reply_to.map_or(0, str::len) + msg.len();
        if let Some(headers) = headers {
            estimate += headers
                .iter()
                .map(|(k, v)| k.len() + v.len() + 3)
                .sum::<usize>();
        }

        let op = if let Some(headers) = headers {
            ClientOp::Hpub {
                subject,
                reply_to,
                payload: msg,
                headers,
            }
        } else {
            ClientOp::Pub {
                subject,
                reply_to,
                payload: msg,
            }
        };

        let mut write = match self.state.write.try_lock() {
            Ok(w) => w,
            Err(e) => {
                return Some(Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("mutex error: {}", e),
                )))
            }
        };

        match write.writer.as_mut() {
            None => {
                // If reconnecting, write into the buffer.
                Some(match proto::encode(&mut write.buffer, op).await {
                    Ok(()) => write.buffer.flush().await,
                    Err(e) => Err(e),
                })
            }
            Some(mut writer) => {
                // Check if there's enough space in the buffer to encode the
                // whole message.
                if BUF_CAPACITY - writer.buffer().len() < estimate {
                    return None;
                }

                // If connected, write into the writer. This is not going to
                // block because there's enough space in the buffer.
                let res = proto::encode(&mut writer, op).await;
                write.flush_kicker.try_send(()).ok();

                // If writing fails, disconnect.
                if res.is_err() {
                    write.writer = None;

                    // NB see locking protocol for state.write and state.read
                    let mut read = self.state.read.lock().await;
                    read.pongs.clear();
                }
                Some(res)
            }
        }
    }

    /// Runs the loop that connects and reconnects the client.
    async fn run(&self, mut connector: Connector) -> io::Result<()> {
        let mut first_connect = true;

        loop {
            //  Don't use backoff on first connect.
            let use_backoff = !first_connect;

            // Make a connection to the server.
            let (server_info, stream) = connector.connect(use_backoff).await?;
            self.process_info(&server_info, &connector).await;

            let reader = BufReader::with_capacity(BUF_CAPACITY, stream.clone());
            let writer = BufWriter::with_capacity(BUF_CAPACITY, stream);

            // Set up the new connection for this client.
            if self.reconnect(server_info, writer).await.is_ok() {
                // Connected! Now dispatch MSG operations.
                if !first_connect {
                    connector.get_options().reconnect_callback.call().await;
                }
                if self.dispatch(reader, &mut connector).await.is_ok() {
                    // If the client stopped gracefully, return.
                    return Ok(());
                } else {
                    connector.get_options().disconnect_callback.call().await;
                    self.state.write.lock().await.writer = None;
                }
            }

            // Clear our pings_out.
            let mut read = self.state.read.lock().await;
            read.pings_out = 0;
            drop(read);

            // Inject random delays when testing.
            inject_delay().await;

            // Quit if the client is closed.
            if self.check_shutdown().is_err() {
                return Ok(());
            }
            first_connect = false;
        }
    }

    /// Puts the client back into connected state with the given writer.
    async fn reconnect(
        &self,
        server_info: ServerInfo,
        mut writer: BufWriter<NatsStream>,
    ) -> io::Result<()> {
        // Inject random delays when testing.
        inject_delay().await;

        // Check if the client is closed.
        self.check_shutdown()?;

        let mut write = self.state.write.lock().await;
        let mut read = self.state.read.lock().await;

        // Drop the current writer, if there is one.
        write.writer = None;

        // Inject random I/O failures when testing.
        inject_io_failure()?;

        // Restart subscriptions that existed before the last reconnect.
        for (sid, subscription) in &read.subscriptions {
            // Send a SUB operation to the server.
            proto::encode(
                &mut writer,
                ClientOp::Sub {
                    subject: subscription.subject.as_str(),
                    queue_group: subscription.queue_group.as_deref(),
                    sid: *sid,
                },
            )
            .await?;
        }

        // Take out expected PONGs.
        let pongs = mem::take(&mut read.pongs);

        // Take out buffered operations.
        let buffered = write.buffer.clear();

        // Write buffered PUB operations into the new writer.
        writer.write_all(buffered).await?;
        writer.flush().await?;

        // All good, continue with this connection.
        *self.server_info.lock().await = server_info;
        write.writer = Some(writer);

        // Complete PONGs because the connection is healthy.
        for p in pongs {
            p.try_send(()).ok();
        }

        // NB see locking protocol for state.write and state.read
        drop(read);
        drop(write);

        Ok(())
    }

    // processes action need to be performed based on retrieved server info.
    async fn process_info(&self, server_info: &ServerInfo, connector: &Connector) {
        if server_info.lame_duck_mode {
            connector.get_options().lame_duck_callback.call().await;
        }
    }

    /// Updates our last activity from the server.
    async fn update_activity(&self) {
        let mut read = self.state.read.lock().await;
        read.last_active = Instant::now();
    }

    /// Reads messages from the server and dispatches them to subscribers.
    async fn dispatch(
        &self,
        mut reader: impl tokio::io::AsyncBufRead + std::marker::Unpin,
        connector: &mut Connector,
    ) -> io::Result<()> {
        // Handle operations received from the server.
        while let Some(op) = proto::decode(&mut reader).await? {
            // Inject random delays when testing.
            inject_delay().await;

            if self.check_shutdown().is_err() {
                break;
            }

            // Track activity.
            self.update_activity().await;

            match op {
                ServerOp::Info(server_info) => {
                    for url in &server_info.connect_urls {
                        connector.add_url(url).ok();
                    }
                    self.process_info(&server_info, connector).await;
                    *self.server_info.lock().await = server_info;
                }

                ServerOp::Ping => {
                    // Respond with a PONG if connected.
                    let mut write = self.state.write.lock().await;
                    let read = self.state.read.lock().await;

                    if let Some(w) = write.writer.as_mut() {
                        proto::encode(w, ClientOp::Pong).await?;
                        write.flush_kicker.try_send(()).ok();
                    }

                    // NB see locking protocol for state.write and state.read
                    drop(read);
                    drop(write);
                }

                ServerOp::Pong => {
                    // If a PONG is received while disconnected, it came from a
                    // connection that isn't alive anymore and therefore doesn't
                    // correspond to the next expected PONG.
                    let write = self.state.write.lock().await;
                    let mut read = self.state.read.lock().await;

                    // Clear any outstanding pings.
                    read.pings_out = 0;

                    if write.writer.is_some() {
                        // Take the next expected PONG and complete it by
                        // sending a message.
                        if let Some(pong) = read.pongs.pop_front() {
                            pong.try_send(()).ok();
                        }
                    }

                    // NB see locking protocol for state.write and state.read
                    drop(read);
                    drop(write);
                }

                ServerOp::Msg {
                    subject,
                    sid,
                    reply_to,
                    payload,
                } => {
                    // Ignore muted subscriptions
                    if self.state.meta.lock().await.mutes.get(&sid).is_some() {
                        continue;
                    }

                    let read = self.state.read.lock().await;

                    // Send the message to matching subscription.
                    if let Some(subscription) = read.subscriptions.get(&sid) {
                        let msg = Message {
                            subject,
                            reply: reply_to,
                            data: payload,
                            headers: None,
                            client: Some(self.clone()),
                            double_acked: Default::default(),
                        };

                        // Preprocess and drop the message from the buffer if it the predicate
                        // returns true
                        if (&subscription.preprocess).process(sid, &msg).await {
                            continue;
                        }

                        // Send a message or drop it if the channel is
                        // disconnected or full.
                        subscription.messages.send(msg).await.unwrap();
                    }
                }

                ServerOp::Hmsg {
                    subject,
                    headers,
                    sid,
                    reply_to,
                    payload,
                } => {
                    // Ignore muted subscriptions
                    if self.state.meta.lock().await.mutes.get(&sid).is_some() {
                        continue;
                    }

                    let read = self.state.read.lock().await;
                    // Send the message to matching subscription.
                    if let Some(subscription) = read.subscriptions.get(&sid) {
                        let msg = Message {
                            subject,
                            reply: reply_to,
                            data: payload,
                            headers: Some(headers),
                            client: Some(self.clone()),
                            double_acked: Default::default(),
                        };

                        // Preprocess and drop the message from the buffer if it the predicate
                        // returns true
                        if (subscription.preprocess).process(sid, &msg).await {
                            continue;
                        }

                        // Send a message or drop it if the channel is
                        // disconnected or full.
                        subscription.messages.send(msg).await.unwrap();
                    }
                }

                ServerOp::Err(msg) => {
                    let si = self.server_info().await;
                    connector
                        .get_options()
                        .error_callback
                        .call(si, Error::new(ErrorKind::Other, msg))
                        .await;
                }

                ServerOp::Unknown(line) => {
                    log::warn!("unknown op: {}", line);
                }
            }
        }
        // The stream of operation is broken, meaning the connection was lost.
        Err(ErrorKind::ConnectionReset.into())
    }
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_struct("Client").finish()
    }
}

/// Reconnect buffer.
///
/// If the connection was broken and the client is currently reconnecting, PUB
/// messages get stored in this buffer of limited size. As soon as the
/// connection is then re-established, buffered messages will be sent to the
/// server.
struct Buffer {
    /// Bytes in the buffer.
    ///
    /// There are three interesting ranges in this slice:
    ///
    /// - `..flushed` contains buffered PUB messages.
    /// - `flushed..written` contains a partial PUB message at the end.
    /// - `written..` is empty space in the buffer.
    bytes: Box<[u8]>,

    /// Number of written bytes.
    written: usize,

    /// Number of bytes marked as "flushed".
    flushed: usize,
}

impl Buffer {
    /// Creates a new buffer with the given size.
    fn new(size: usize) -> Buffer {
        Buffer {
            bytes: vec![0_u8; size].into_boxed_slice(),
            written: 0,
            flushed: 0,
        }
    }

    /// Clears the buffer and returns buffered bytes.
    fn clear(&mut self) -> &[u8] {
        let buffered = &self.bytes[..self.flushed];
        self.written = 0;
        self.flushed = 0;
        buffered
    }
}

impl AsyncWrite for Buffer {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        let n = buf.len();
        // Check if `buf` will fit into this `Buffer`.
        if self.bytes.len() - self.written < n {
            // Fill the buffer to prevent subsequent smaller writes.
            self.written = self.bytes.len();
            Poll::Ready(Err(Error::new(
                ErrorKind::Other,
                "the disconnect buffer is full",
            )))
        } else {
            // Append `buf` into the buffer.
            let range = self.written..self.written + n;
            self.bytes[range].copy_from_slice(&buf[..n]);
            self.written += n;
            Poll::Ready(Ok(n))
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.flushed = self.written;
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }
}
