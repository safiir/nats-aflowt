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

//! Support for the `JetStream` at-least-once messaging system.
//!
//! # Examples
//!
//! Create a new stream with default options:
//!
//! ```no_run
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let nc = nats_aflowt::connect("my_server::4222").await?;
//! let js = nats_aflowt::jetstream::new(nc);
//!
//! // add_stream converts a str into a
//! // default `StreamConfig`.
//! js.add_stream("my_stream").await?;
//!
//! # Ok(()) }
//! ```
//!
//! Create a new stream with configuration:
//!
//! ```no_run
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use nats_aflowt::jetstream::{StreamConfig, StorageType};
//!
//! let nc = nats_aflowt::connect("my_server::4222").await?;
//! let js = nats_aflowt::jetstream::new(nc);
//!
//! js.add_stream(StreamConfig {
//!     name: "my_memory_stream".to_string(),
//!     max_bytes: 5 * 1024 * 1024 * 1024,
//!     storage: StorageType::Memory,
//!     ..Default::default()
//! }).await?;
//!
//! # Ok(()) }
//! ```
//!
//! Create a new consumer:
//!
//! ```no_run
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let nc = nats_aflowt::connect("my_server::4222").await?;
//! let js = nats_aflowt::jetstream::new(nc);
//!
//! js.add_stream("my_stream").await?;
//! js.add_consumer("my_stream", "my_consumer").await?;
//!
//! # Ok(()) }
//! ```
//!
//! Create a new consumer with configuration:
//!
//! ```no_run
//! use nats_aflowt::jetstream::{ConsumerConfig};
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let nc = nats_aflowt::connect("my_server::4222").await?;
//! let js = nats_aflowt::jetstream::new(nc);
//!
//! js.add_stream("my_stream").await?;
//! js.add_consumer("my_stream", ConsumerConfig {
//!   deliver_subject: Some("my_deliver_subject".to_string()),
//!   durable_name: Some("my_durable_consumer".to_string()),
//!   ..Default::default()
//! }).await?;
//!
//! # Ok(()) }
//! ```
//!
//! Create a new subscription:
//!
//! ```no_run
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!
//! let nc = nats_aflowt::connect("my_server::4222").await?;
//! let js = nats_aflowt::jetstream::new(nc);
//!
//! js.add_stream("my_stream").await?;
//! let subscription = js.subscribe("my_stream").await?;
//!
//! # Ok(()) }
//! ```
//!

use crate::{BoxFuture, Stream};
use pin_project::pin_project;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::{
    collections::{HashSet, VecDeque},
    convert::TryFrom,
    error, fmt,
    fmt::Debug,
    io::{self, ErrorKind},
    pin::Pin,
    time::Duration,
};
use tokio::sync::Mutex;

const ORDERED_IDLE_HEARTBEAT: Duration = Duration::from_nanos(5_000_000_000);

mod push_subscription;
mod types;

pub use push_subscription::PushSubscription;
pub use types::*;

use crate::{
    header::{self, HeaderMap},
    Connection, Message,
};

/// `JetStream` options
#[derive(Clone)]
pub struct JetStreamOptions {
    pub(crate) api_prefix: String,
}

impl Debug for JetStreamOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_map()
            .entry(&"api_prefix", &self.api_prefix)
            .finish()
    }
}

impl Default for JetStreamOptions {
    fn default() -> JetStreamOptions {
        JetStreamOptions {
            api_prefix: "$JS.API.".to_string(),
        }
    }
}

impl JetStreamOptions {
    /// `Options` for `JetStream` operations.
    ///
    /// # Example
    ///
    /// ```
    /// let options = nats_aflowt::jetstream::JetStreamOptions::new();
    /// ```
    pub fn new() -> JetStreamOptions {
        JetStreamOptions::default()
    }

    /// Set a custom `JetStream` API prefix.
    ///
    /// # Example
    ///
    /// ```
    /// let options = nats_aflowt::jetstream::JetStreamOptions::new()
    ///     .api_prefix("some_exported_prefix".to_string());
    /// ```
    #[must_use]
    pub fn api_prefix(mut self, mut api_prefix: String) -> Self {
        if !api_prefix.ends_with('.') {
            api_prefix.push('.');
        }

        self.api_prefix = api_prefix;
        self
    }

    /// Set a custom `JetStream` API prefix from a domain.
    ///
    /// # Example
    ///
    /// ```
    /// let options = nats_aflowt::jetstream::JetStreamOptions::new()
    ///   .domain("some_domain");
    /// ```
    #[must_use]
    pub fn domain(self, domain: &str) -> Self {
        if domain.is_empty() {
            self.api_prefix("".to_string())
        } else {
            self.api_prefix(format!("$JS.{}.API", domain))
        }
    }
}

/// `ApiResponse` is a standard response from the `JetStream` JSON Api
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum ApiResponse<T> {
    // Note:
    // Serde will try to match the data against each variant in order and the first one that
    // deserializes successfully is the one returned.
    //
    // Therefore the error case must come first, otherwise it can be ignored.
    Err { error: Error },
    Ok(T),
}

/// `ErrorCode` which can be returned from a server an a response when an error occurs.
#[derive(Debug, PartialEq, Eq, Serialize_repr, Deserialize_repr, Clone, Copy)]
#[repr(u64)]
pub enum ErrorCode {
    /// Peer not a member
    ClusterPeerNotMember = 10040,
    /// Consumer expected to be ephemeral but detected a durable name set in subject
    ConsumerEphemeralWithDurableInSubject = 10019,
    /// Stream external delivery prefix {prefix} overlaps with stream subject {subject}
    StreamExternalDelPrefixOverlaps = 10022,
    /// Resource limits exceeded for account
    AccountResourcesExceeded = 10002,
    /// JetStream system temporarily unavailable
    ClusterNotAvail = 10008,
    /// Subjects overlap with an existing stream
    StreamSubjectOverlap = 10065,
    /// Wrong last sequence: {seq}
    StreamWrongLastSequence = 10071,
    /// Template name in subject does not match request
    TemplateNameNotMatchSubject = 10073,
    /// No suitable peers for placement
    ClusterNoPeers = 10005,
    /// Consumer expected to be ephemeral but a durable name was set in request
    ConsumerEphemeralWithDurableName = 10020,
    /// Insufficient resources
    InsufficientResources = 10023,
    /// Stream mirror must have max message size >= source
    MirrorMaxMessageSizeTooBig = 10030,
    /// Generic stream template deletion failed error string
    StreamTemplateDelete = 10067,
    /// Bad request
    BadRequest = 10003,
    /// Not currently supported in clustered mode
    ClusterUnSupportFeature = 10036,
    /// Consumer not found
    ConsumerNotFound = 10014,
    /// Stream source must have max message size >= target
    SourceMaxMessageSizeTooBig = 10046,
    /// Generic stream assignment error string
    StreamAssignment = 10048,
    /// Message size exceeds maximum allowed
    StreamMessageExceedsMaximum = 10054,
    /// Generic template creation failed string
    StreamTemplateCreate = 10066,
    /// Invalid JSON
    InvalidJSON = 10025,
    /// Stream external delivery prefix {prefix} must not contain wildcards
    StreamInvalidExternalDeliverySubject = 10024,
    /// Restore failed: {err}
    StreamRestore = 10062,
    /// Incomplete results
    ClusterIncomplete = 10004,
    /// Account not found
    NoAccount = 10035,
    /// General RAFT error string
    RaftGeneral = 10041,
    /// JetStream unable to subscribe to restore snapshot {subject}: {err}
    RestoreSubscribeFailed = 10042,
    /// General stream deletion error string
    StreamDelete = 10050,
    /// Stream external api prefix {prefix} must not overlap with {subject}
    StreamExternalApiOverlap = 10021,
    /// Stream mirrors can not also contain subjects
    MirrorWithSubjects = 10034,
    /// JetStream not enabled
    NotEnabled = 10076,
    /// JetStream not enabled for account
    NotEnabledForAccount = 10039,
    /// Sequence {seq} not found
    SequenceNotFound = 10043,
    /// Mirror configuration can not be updated
    StreamMirrorNotUpdatable = 10055,
    /// Expected stream sequence does not match
    StreamSequenceNotMatch = 10063,
    /// Wrong last msg Id: {id}
    StreamWrongLastMsgId = 10070,
    /// JetStream unable to open temp storage for restore
    TempStorageFailed = 10072,
    /// Insufficient storage resources available
    StorageResourcesExceeded = 10047,
    /// Stream name in subject does not match request
    StreamMismatch = 10056,
    /// Expected stream does not match
    StreamNotMatch = 10060,
    /// Generic mirror consumer setup failure string
    MirrorConsumerSetupFailed = 10029,
    /// Expected an empty request payload
    NotEmptyRequest = 10038,
    /// Stream name already in use
    StreamNameExist = 10058,
    /// Tags placement not supported for operation
    ClusterTags = 10011,
    /// Maximum consumers limit reached
    MaximumConsumersLimit = 10026,
    /// General source consumer setup failure string
    SourceConsumerSetupFailed = 10045,
    /// General consumer creation failure string
    ConsumerCreate = 10012,
    /// Consumer expected to be durable but no durable name set in subject
    ConsumerDurableNameNotInSubject = 10016,
    /// General stream limits exceeded error string
    StreamLimits = 10053,
    /// Replicas configuration can not be updated
    StreamReplicasNotUpdatable = 10061,
    /// Template not found
    StreamTemplateNotFound = 10068,
    /// JetStream cluster not assigned to this server
    ClusterNotAssigned = 10007,
    /// JetStream cluster can not handle request
    ClusterNotLeader = 10009,
    /// Consumer name already in use
    ConsumerNameExist = 10013,
    /// Stream mirrors can not also contain other sources
    MirrorWithSources = 10031,
    /// Stream not found
    StreamNotFound = 10059,
    /// JetStream clustering support required
    ClusterRequired = 10010,
    /// Consumer expected to be durable but a durable name was not set
    ConsumerDurableNameNotSet = 10018,
    /// Maximum number of streams reached
    MaximumStreamsLimit = 10027,
    /// Stream mirrors can not have both start seq and start time configured
    MirrorWithStartSeqAndTime = 10032,
    /// Snapshot failed: {err}
    StreamSnapshot = 10064,
    /// Generic stream update error string
    StreamUpdate = 10069,
    /// JetStream not in clustered mode
    ClusterNotActive = 10006,
    /// Consumer name in subject does not match durable name in request
    ConsumerDurableNameNotMatchSubject = 10017,
    /// Insufficient memory resources available
    MemoryResourcesExceeded = 10028,
    /// Stream mirrors can not contain filtered subjects
    MirrorWithSubjectFilters = 10033,
    /// Generic stream creation error string
    StreamCreate = 10049,
    /// Server is not a member of the cluster
    ClusterServerNotMember = 10044,
    /// No message found
    NoMessageFound = 10037,
    /// Deliver subject not valid
    SnapshotDeliverSubjectInvalid = 10015,
    /// General stream failure string
    StreamGeneralErrorF = 10051,
    /// Stream configuration validation error string
    StreamInvalidConfigF = 10052,
    /// Replicas > 1 not supported in non-clustered mode
    StreamReplicasNotSupported = 10074,
    /// Generic message deletion failure error string
    StreamMsgDeleteFailedF = 10057,
    /// Peer remap failed
    PeerRemap = 10075,
    /// Generic error when storing a message failed
    StreamStoreFailedF = 10077,
    /// Consumer config required
    ConsumerConfigRequired = 10078,
    /// Consumer deliver subject has wildcards
    ConsumerDeliverToWildcards = 10079,
    /// Consumer in push mode can not set max waiting
    ConsumerPushMaxWaiting = 10080,
    /// Consumer deliver subject forms a cycle
    ConsumerDeliverCycle = 10081,
    /// Consumer requires ack policy for max ack pending
    ConsumerMaxPendingAckPolicyRequired = 10082,
    /// Consumer idle heartbeat needs to be >= 100ms
    ConsumerSmallHeartbeat = 10083,
    /// Consumer in pull mode requires explicit ack policy
    ConsumerPullRequiresAck = 10084,
    /// Consumer in pull mode requires a durable name
    ConsumerPullNotDurable = 10085,
    /// Consumer in pull mode can not have rate limit set
    ConsumerPullWithRateLimit = 10086,
    /// Consumer max waiting needs to be positive
    ConsumerMaxWaitingNegative = 10087,
    /// Consumer idle heartbeat requires a push based consumer
    ConsumerHBRequiresPush = 10088,
    /// Consumer flow control requires a push based consumer
    ConsumerFCRequiresPush = 10089,
    /// Consumer direct requires a push based consumer
    ConsumerDirectRequiresPush = 10090,
    /// Consumer direct requires an ephemeral consumer
    ConsumerDirectRequiresEphemeral = 10091,
    /// Consumer direct on a mapped consumer
    ConsumerOnMapped = 10092,
    /// Consumer filter subject is not a valid subset of the interest subjects
    ConsumerFilterNotSubset = 10093,
    /// Generic delivery policy error
    ConsumerInvalidPolicy = 10094,
    /// Failed to parse consumer sampling configuration: {err}
    ConsumerInvalidSampling = 10095,
    /// Stream not valid
    StreamInvalid = 10096,
    /// Workqueue stream requires explicit ack
    ConsumerWQRequiresExplicitAck = 10098,
    /// Multiple non-filtered consumers not allowed on workqueue stream
    ConsumerWQMultipleUnfiltered = 10099,
    /// Filtered consumer not unique on workqueue stream
    ConsumerWQConsumerNotUnique = 10100,
    /// Consumer must be deliver all on workqueue stream
    ConsumerWQConsumerNotDeliverAll = 10101,
    /// Consumer name is too long, maximum allowed is {max}
    ConsumerNameTooLong = 10102,
    /// Durable name can not contain '.', '*', '>'
    ConsumerBadDurableName = 10103,
    /// Error creating store for consumer: {err}
    ConsumerStoreFailed = 10104,
    /// Consumer already exists and is still active
    ConsumerExistingActive = 10105,
    /// Consumer replacement durable config not the same
    ConsumerReplacementWithDifferentName = 10106,
    /// Consumer description is too long, maximum allowed is {max}
    ConsumerDescriptionTooLong = 10107,
    /// Header size exceeds maximum allowed of 64k
    StreamHeaderExceedsMaximum = 10097,
}

/// `Error` type returned from an API response when an error occurs.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Error {
    code: usize,
    err_code: ErrorCode,
    description: Option<String>,
}

impl Error {
    /// Returns the status code assosciated with this error
    pub fn code(&self) -> usize {
        self.code
    }

    /// Returns the server side error code associated with this error.
    pub fn error_code(&self) -> ErrorCode {
        self.err_code
    }
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            fmt,
            "{} (code {}, error code {})",
            self.code,
            self.description.as_ref().unwrap_or(&"unknown".to_string()),
            self.err_code as u64,
        )
    }
}

impl error::Error for Error {}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
struct PagedRequest {
    offset: i64,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
struct PagedResponse<T> {
    pub r#type: String,

    #[serde(alias = "streams", alias = "consumers")]
    pub items: Option<VecDeque<T>>,

    // related to paging
    pub total: usize,
    pub offset: usize,
    pub limit: usize,
}

/// An iterator over paged `JetStream` API operations.
#[derive(Debug)]
#[pin_project]
pub struct PagedIterator<'a, T> {
    #[pin]
    manager: &'a JetStream,
    subject: String,
    offset: i64,
    items: VecDeque<T>,
    done: bool,
}

impl<'x, 'y, 'z: 'x, T: 'x> PagedIterator<'z, T>
where
    T: DeserializeOwned + Debug,
{
    fn to_stream(mut self: Pin<Box<Self>>) -> impl Stream<Item = io::Result<T>> + 'x {
        async_stream::try_stream! {
            if !self.done {
                if !self.items.is_empty() {
                    yield self.items.pop_front().unwrap();
                }
                let req = serde_json::ser::to_vec(&PagedRequest {
                    offset: self.offset,
                })
                .unwrap();
                let mut page: PagedResponse<T> =
                    match self.manager.js_request(&self.subject, &req).await {
                        Ok(page) => page,
                        Err(e) => {
                            (*self).done = true;
                            Err(e)?
                        }
                    };
                if page.items.is_none() {
                    (*self).done = true;
                } else {
                    let items = page.items.take().unwrap();
                    (*self).offset += i64::try_from(items.len()).unwrap();
                    (*self).items = items;
                    if self.items.is_empty() {
                        (*self).done = true;
                    } else {
                        yield self.items.pop_front().unwrap()
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
struct SubscriptionPreprocessor {
    sequence_pair: Arc<Mutex<SequencePair>>,
    context: JetStream,
    consumer_config: ConsumerConfig,
    stream_name: String,
    shared_sid: Arc<AtomicU64>,
}

impl crate::client::Preprocessor for SubscriptionPreprocessor {
    fn process<'proc>(&'proc self, sid: u64, message: &'proc Message) -> BoxFuture<'proc, bool> {
        Box::pin(async move {
            if message.is_flow_control() {
                return false;
            }

            // Track and respond to idle heartbeats
            if message.is_idle_heartbeat() {
                let maybe_consumer_stalled = message
                    .headers
                    .as_ref()
                    .and_then(|headers| {
                        headers
                            .get(header::NATS_CONSUMER_STALLED)
                            .map(|set| set.iter().cloned().next())
                    })
                    .flatten();

                if let Some(consumer_stalled) = maybe_consumer_stalled {
                    self.context
                        .connection
                        .try_publish_with_reply_or_headers(&consumer_stalled, None, None, b"")
                        .await;
                }

                let maybe_consumer_seq = message
                    .headers
                    .as_ref()
                    .and_then(|headers| {
                        headers
                            .get(header::NATS_LAST_CONSUMER)
                            .map(|set| set.iter().cloned().next())
                    })
                    .flatten();

                if let Some(consumer_seq) = maybe_consumer_seq {
                    let consumer_seq = consumer_seq.parse::<u64>().unwrap();
                    let sequence_info = self.sequence_pair.lock().await;
                    if consumer_seq != sequence_info.consumer_seq {
                        return self
                            .context
                            .handle_sequence_mismatch(
                                self.consumer_config.clone(),
                                self.sequence_pair.clone(),
                                self.stream_name.clone(),
                                self.shared_sid.clone(),
                                sid,
                                sequence_info.stream_seq + 1,
                            )
                            .await;
                    }
                }
                return false;
            }

            // Track messages for sequence mismatches.
            if let Some(message_info) = message.jetstream_message_info() {
                let mut sequence_info = self.sequence_pair.lock().await;
                if message_info.consumer_seq != sequence_info.consumer_seq + 1 {
                    return self
                        .context
                        .handle_sequence_mismatch(
                            self.consumer_config.clone(),
                            self.sequence_pair.clone(),
                            self.stream_name.clone(),
                            self.shared_sid.clone(),
                            sid,
                            sequence_info.stream_seq + 1,
                        )
                        .await;
                }

                sequence_info.stream_seq = message_info.stream_seq;
                sequence_info.consumer_seq = message_info.consumer_seq;
            }
            false
        })
    }
}

/// A context for performing `JetStream` operations.
#[derive(Clone, Debug)]
pub struct JetStream {
    pub(crate) connection: Connection,
    pub(crate) options: JetStreamOptions,
}

impl JetStream {
    /// Create a new `JetStream` context.
    pub fn new(connection: Connection, options: JetStreamOptions) -> Self {
        Self {
            connection,
            options,
        }
    }

    /// Publishes a message to `JetStream`
    pub async fn publish(&self, subject: &str, data: impl AsRef<[u8]>) -> io::Result<PublishAck> {
        self.publish_with_options_or_headers(subject, None, None, data)
            .await
    }

    /// Publishes a message to `JetStream` with the given options.
    pub async fn publish_with_options(
        &self,
        subject: &str,
        data: impl AsRef<[u8]>,
        options: &PublishOptions,
    ) -> io::Result<PublishAck> {
        self.publish_with_options_or_headers(subject, Some(options), None, data)
            .await
    }

    /// Publishes a `Message` to `JetStream`.
    pub async fn publish_message(&self, message: &Message) -> io::Result<PublishAck> {
        self.publish_with_options_or_headers(
            &message.subject,
            None,
            message.headers.as_ref(),
            &message.data,
        )
        .await
    }

    /// Publishes a `Message` to `JetStream` with the given options.
    pub async fn publish_message_with_options(
        &self,
        message: &Message,
        options: &PublishOptions,
    ) -> io::Result<PublishAck> {
        self.publish_with_options_or_headers(
            &message.subject,
            Some(options),
            message.headers.as_ref(),
            &message.data,
        )
        .await
    }

    /// Publishes a message to `JetStream` with the given options and/or headers.
    pub(crate) async fn publish_with_options_or_headers(
        &self,
        subject: &str,
        maybe_options: Option<&PublishOptions>,
        maybe_headers: Option<&HeaderMap>,
        msg: impl AsRef<[u8]>,
    ) -> io::Result<PublishAck> {
        let maybe_headers = if let Some(options) = maybe_options {
            let mut headers = maybe_headers.map_or_else(HeaderMap::default, HeaderMap::clone);

            if let Some(v) = options.id.as_ref() {
                let entry = headers
                    .inner
                    .entry(header::NATS_MSG_ID.to_string())
                    .or_insert_with(HashSet::default);

                entry.insert(v.to_string());
            }

            if let Some(v) = options.expected_last_msg_id.as_ref() {
                let entry = headers
                    .inner
                    .entry(header::NATS_EXPECTED_LAST_MSG_ID.to_string())
                    .or_insert_with(HashSet::default);

                entry.insert(v.to_string());
            }

            if let Some(v) = options.expected_stream.as_ref() {
                let entry = headers
                    .inner
                    .entry(header::NATS_EXPECTED_STREAM.to_string())
                    .or_insert_with(HashSet::default);

                entry.insert(v.to_string());
            }

            if let Some(v) = options.expected_last_sequence.as_ref() {
                let entry = headers
                    .inner
                    .entry(header::NATS_EXPECTED_LAST_SEQUENCE.to_string())
                    .or_insert_with(HashSet::default);

                entry.insert(v.to_string());
            }

            if let Some(v) = options.expected_last_subject_sequence.as_ref() {
                let entry = headers
                    .inner
                    .entry(header::NATS_EXPECTED_LAST_SUBJECT_SEQUENCE.to_string())
                    .or_insert_with(HashSet::default);

                entry.insert(v.to_string());
            }

            Some(headers)
        } else {
            maybe_headers.cloned()
        };

        let maybe_timeout = maybe_options.and_then(|options| options.timeout);

        let res_msg = self
            .connection
            .request_with_headers_or_timeout(subject, maybe_headers.as_ref(), maybe_timeout, msg)
            .await?;

        let res: ApiResponse<PublishAck> = serde_json::de::from_slice(&res_msg.data)?;
        match res {
            ApiResponse::Ok(pub_ack) => Ok(pub_ack),
            ApiResponse::Err { error, .. } => {
                log::error!(
                    "failed to parse API response: {:?}",
                    std::str::from_utf8(&res_msg.data)
                );

                Err(io::Error::new(ErrorKind::Other, error))
            }
        }
    }

    /// Create an ephemeral push consumer subscription.
    ///
    /// # Example
    ///
    /// ```
    /// use nats_aflowt::Stream;
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// # let client = nats_aflowt::connect("demo.nats.io").await?;
    /// # let context = nats_aflowt::jetstream::new(client);
    /// # let sub_name = format!("sub_{}", rand::random::<u64>());
    /// # context.add_stream(sub_name.as_str()).await?;
    /// # context.publish(&sub_name, "hello").await?;
    /// #    
    /// let subscription = context.subscribe(&sub_name).await?;
    /// println!("Received message {:?}", subscription.next().await);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn subscribe(&self, subject: &str) -> io::Result<PushSubscription> {
        self.do_push_subscribe(subject, None, None).await
    }

    /// Creates a push consumer subscription with options.
    ///
    /// If said consumer is named and already exists, this will attempt to bind this consumer to
    /// that one, else will attempt to create a new internally managed consumer resource.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use nats_aflowt::jetstream::{ SubscribeOptions };
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// # let nc = nats_aflowt::connect("demo.nats.io").await?;
    /// # let js = nats_aflowt::jetstream::new(nc);
    /// let sub = js.subscribe_with_options("foo",
    ///       &SubscribeOptions::bind("existing_stream".to_string(),
    ///       "existing_consumer".to_string())).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn subscribe_with_options(
        &self,
        subject: &str,
        options: &SubscribeOptions,
    ) -> io::Result<PushSubscription> {
        self.do_push_subscribe(subject, None, Some(options)).await
    }

    /// Creates a push-based consumer subscription with a queue group.
    /// The queue group will be used as the durable name.
    ///
    /// # Example
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// # let client = nats_aflowt::connect("demo.nats.io").await?;
    /// # let context = nats_aflowt::jetstream::new(client);
    /// # context.add_stream("queue");
    /// let subscription = context.queue_subscribe("queue", "queue_group").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn queue_subscribe(
        &self,
        subject: &str,
        queue: &str,
    ) -> io::Result<PushSubscription> {
        self.do_push_subscribe(subject, Some(queue), None).await
    }

    /// Creates a push-based consumer subscription with a queue group and options.
    ///
    /// If a durable name is not set within the options provided options then the queue group will
    /// be used as the durable name.
    ///
    pub async fn queue_subscribe_with_options(
        &self,
        subject: &str,
        queue: &str,
        options: &SubscribeOptions,
    ) -> io::Result<PushSubscription> {
        self.do_push_subscribe(subject, Some(queue), Some(options))
            .await
    }

    async fn handle_sequence_mismatch(
        &self,
        consumer_config: ConsumerConfig,
        sequence_pair: Arc<Mutex<SequencePair>>,
        stream_name: String,
        shared_sid: Arc<AtomicU64>,
        sid: u64,
        start_seq: u64,
    ) -> bool {
        // Immediately mute the subscription so that messages no longer get delivered to it.
        //
        // If we are already muted then the triggering slipped through and we can
        // return early as a resubscription will already be on the way.
        if !self.connection.0.client.mute(sid).await.unwrap() {
            return true;
        }

        let context = self.clone();
        tokio::spawn(async move {
            let new_deliver_subject = context.connection.new_inbox();
            let result = context
                .connection
                .0
                .client
                .resubscribe(sid, &new_deliver_subject)
                .await;

            if let Ok(new_sid) = result {
                shared_sid.store(new_sid, Ordering::Relaxed);

                let mut consumer_config = consumer_config.clone();
                consumer_config.deliver_subject = Some(new_deliver_subject);
                consumer_config.deliver_policy = DeliverPolicy::ByStartSeq;
                consumer_config.opt_start_seq = Some(start_seq);
                context
                    .add_consumer(stream_name, consumer_config)
                    .await
                    .ok();
            }

            let mut sequence_info = sequence_pair.lock().await;
            sequence_info.consumer_seq = 0;
        });

        true
    }

    async fn do_push_subscribe(
        &self,
        subject: &str,
        maybe_queue: Option<&str>,
        maybe_options: Option<&SubscribeOptions>,
    ) -> io::Result<PushSubscription> {
        // If no stream name is specified the subject cannot be empty.
        if subject.is_empty()
            && maybe_options
                .map(|options| options.stream_name.as_ref())
                .is_none()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Subject required",
            ));
        }

        let wants_idle_heartbeat =
            maybe_options.map_or(false, |options| options.idle_heartbeat.is_some());

        let wants_flow_control =
            maybe_options.map_or(false, |options| options.flow_control.is_some());

        if maybe_queue.is_some() {
            // Queue subscriber cannot have idle heartbeats
            if wants_idle_heartbeat {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "queue subscription doesn't support idle heartbeat",
                ));
            }

            // Nor flow control since messages will randomly dispatched to members.
            if wants_flow_control {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "queue subscription doesn't support flow control",
                ));
            }
        };

        // Checks specific to ordered consumers.
        if let Some(options) = maybe_options.filter(|options| options.ordered) {
            // Check for queue subscription.
            if maybe_queue.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "queues not be set for an ordered consumer",
                ));
            }

            // Check for durable name
            if options.durable_name.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "durable name can not be set for an ordered consumer",
                ));
            }

            // Check for bound consumers.
            if options.consumer_name.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "can not bind existing consumer for an ordered consumer",
                ));
            }

            // Check for ack policy.
            if options.ack_policy.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "ack policy can not be set for an ordered consumer",
                ));
            }

            // Check for max deliver.
            if options.max_deliver.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "max deliver can not be set for an ordered consumer",
                ));
            }

            // Check for deliver subject (we pick our own).
            if options.deliver_subject.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "deliver subject can not be set for an ordered consumer",
                ));
            }
        }

        let process_consumer_info = |info: ConsumerInfo| {
            // Make sure this new subject matches or is a subset.
            if !info.config.filter_subject.is_empty() && subject != info.config.filter_subject {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "subject does not match consumer",
                ));
            }

            if let Some(deliver_group) = info.config.deliver_group.as_ref() {
                if let Some(queue) = maybe_queue {
                    if deliver_group != queue {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "cannot create a queue subscription {} for a consumer with a deliver group {}",
                                queue, deliver_group
                            ),
                        ));
                    }
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!(
                            "cannot create a subscription for a consumer with a deliver group {}",
                            deliver_group
                        ),
                    ));
                }
            } else {
                // Prevent a user from attempting to create a queue subscription on
                // a consumer that was not created with a deliver group.
                if maybe_queue.is_some() {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "cannot create a queue subscription for a consumer without a deliver group",
                    ));
                }

                // Need to reject a non queue subscription to a non queue consumer if the consumer
                // is already bound.
                if maybe_queue.is_some() && info.push_bound {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "consumer is already bound to a subscription",
                    ));
                }
            }

            // Check for configuration mismatches.
            if let Some(options) = maybe_options {
                if options.durable_name.is_some()
                    && options.durable_name != info.config.durable_name
                {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("configuration requests durable name to be {:?}, but consumer's value is {:?}", options.durable_name, info.config.durable_name
                        )));
                }

                if options.description.is_some() && options.description != info.config.description {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("configuration requests description to be {:?}, but consumer's value is {:?}", options.description, info.config.description
                        )));
                }

                if options.deliver_policy.is_some()
                    && options.deliver_policy.unwrap() != info.config.deliver_policy
                {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("configuration requests deliver policy to be {:?}, but consumer's value is {:?}", options.deliver_policy, info.config.deliver_policy
                        )));
                }

                if options.opt_start_seq.is_some()
                    && options.opt_start_seq != info.config.opt_start_seq
                {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("configuration requests optional start sequence to be {:?}, but consumer's value is {:?}", options.opt_start_seq, info.config.opt_start_seq
                        )));
                }

                if options.opt_start_time.is_some()
                    && options.opt_start_time != info.config.opt_start_time
                {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("configuration requests optional start time to be {:?}, but consumer's value is {:?}", options.opt_start_time, info.config.opt_start_time
                        )));
                }

                if options.ack_policy.is_some()
                    && options.ack_policy.unwrap() != info.config.ack_policy
                {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("configuration requests ack policy to be {:?}, but consumer's value is {:?}", options.ack_policy, info.config.ack_policy
                        )));
                }

                if options.ack_wait.is_some() && options.ack_wait.unwrap() != info.config.ack_wait {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!(
                            "configuration requests ack wait to be {:?}, but consumer's value is {:?}",
                            options.ack_wait, info.config.ack_wait
                        ),
                    ));
                }

                if options.max_deliver.is_some()
                    && options.max_deliver.unwrap() != info.config.max_deliver
                {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("configuration requests max deliver to be {:?}, but consumer's value is {:?}", options.max_deliver, info.config.max_deliver
                        )));
                }

                if options.replay_policy.is_some()
                    && options.replay_policy.unwrap() != info.config.replay_policy
                {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("configuration requests replay policy to be {:?}, but consumer's value is {:?}", options.replay_policy, info.config.replay_policy
                        )));
                }

                if options.rate_limit.is_some()
                    && options.rate_limit.unwrap() != info.config.rate_limit
                {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("configuration requests rate limit to be {:?}, but consumer's value is {:?}", options.rate_limit, info.config.rate_limit
                        )));
                }

                if options.sample_frequency.is_some()
                    && options.sample_frequency.unwrap() != info.config.sample_frequency
                {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("configuration requests sample frequency to be {:?}, but consumer's value is {:?}", options.sample_frequency, info.config.sample_frequency
                        )));
                }

                if options.max_waiting.is_some()
                    && options.max_waiting.unwrap() != info.config.max_waiting
                {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("configuration requests max waiting to be {:?}, but consumer's value is {:?}", options.max_waiting, info.config.max_waiting
                        )));
                }

                if options.max_ack_pending.is_some()
                    && options.max_ack_pending.unwrap() != info.config.max_ack_pending
                {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("configuration requests max ack pending to be {:?}, but consumer's value is {:?}", options.max_ack_pending, info.config.max_ack_pending
                        )));
                }

                // For flow control, we want to fail if the user explicit wanted it, but
                // it is not set in the existing consumer. If it is not asked by the user,
                // the library still handles it and so no reason to fail.
                if options.flow_control.is_some() && !info.config.flow_control {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("configuration requests flow control to be {:?}, but consumer's value is {:?}", options.flow_control, info.config.flow_control
                        )));
                }

                if options.idle_heartbeat.is_some()
                    && options.idle_heartbeat.unwrap() != info.config.idle_heartbeat
                {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!(
                            "configuration requests heartbeat to be {:?}, but consumer's value is {:?}",
                            options.idle_heartbeat, info.config.idle_heartbeat
                        ),
                    ));
                }
            }

            Ok(info)
        };

        // Find the stream mapped to the subject if not bound to a stream already.
        let stream_name = match maybe_options.and_then(|options| options.stream_name.to_owned()) {
            Some(sub) => Ok(sub),
            None => self.stream_name_by_subject(subject).await,
        }?;

        // Figure out if we have a consumer name
        let maybe_durable_name = maybe_options
            .and_then(|options| options.durable_name.as_deref())
            .or(maybe_queue);

        let maybe_consumer_name = maybe_options
            .and_then(|options| options.consumer_name.as_deref())
            .or(maybe_durable_name);

        // If we do have a consumer name, we can lookup the consumer we should attach to first
        let bind_only = maybe_options.map_or(false, |options| options.bind_only);
        let maybe_consumer_info = if let Some(consumer_name) = maybe_consumer_name {
            let consumer_info_result = self
                .consumer_info(&stream_name, &consumer_name)
                .await
                .and_then(process_consumer_info);

            match consumer_info_result {
                Ok(info) => Some(info),
                Err(err) => {
                    // We only care about protocol errors
                    if err.kind() != io::ErrorKind::Other {
                        return Err(err);
                    }

                    // If we're binding we can't recover from this we so return early.
                    if bind_only {
                        if let Some(inner) = err.into_inner() {
                            if let Ok(err) = inner.downcast::<Error>() {
                                if err.error_code() == ErrorCode::ConsumerNotFound {
                                    return Err(io::Error::new(io::ErrorKind::Other, err));
                                }
                            }
                        }
                    }

                    None
                }
            }
        } else {
            None
        };

        // Create our consumer configuration out here so it can be re-used for matching
        let is_ordered = maybe_options.map_or(false, |options| options.ordered);
        let consumer_config = {
            let mut config = if let Some(options) = maybe_options {
                ConsumerConfig {
                    ack_policy: options.ack_policy.unwrap_or_default(),
                    ack_wait: options.ack_wait.unwrap_or_default(),
                    deliver_policy: options.deliver_policy.unwrap_or_default(),
                    deliver_subject: options.deliver_subject.clone(),
                    description: options.description.clone(),
                    durable_name: options.durable_name.clone(),
                    flow_control: options.flow_control.unwrap_or_default(),
                    headers_only: options.headers_only.unwrap_or_default(),
                    idle_heartbeat: options.idle_heartbeat.unwrap_or_default(),
                    max_ack_pending: options.max_ack_pending.unwrap_or_default(),
                    max_deliver: options.max_deliver.unwrap_or_default(),
                    max_waiting: options.max_waiting.unwrap_or_default(),
                    opt_start_seq: options.opt_start_seq,
                    opt_start_time: options.opt_start_time,
                    rate_limit: options.rate_limit.unwrap_or_default(),
                    replay_policy: options.replay_policy.unwrap_or_default(),
                    sample_frequency: options.sample_frequency.unwrap_or_default(),
                    ..Default::default()
                }
            } else {
                ConsumerConfig::default()
            };

            // Do filtering always, server will clear as needed.
            config.filter_subject = subject.to_string();

            // Pass the queue to the consumer config
            if let Some(queue) = maybe_queue {
                if config.durable_name.is_none() {
                    config.durable_name = Some(queue.to_owned());
                }

                config.deliver_group = Some(queue.to_owned());
            }

            // Create a subject if there is not one.
            if config.deliver_subject.is_none() {
                config.deliver_subject = Some(self.connection.new_inbox());
            }

            // If we're ordered, configuration must be a certain way.
            if is_ordered {
                config.flow_control = true;
                config.ack_policy = AckPolicy::None;
                config.max_deliver = 1;
                config.ack_wait = Duration::from_nanos(1_000_000);

                if config.idle_heartbeat.is_zero() {
                    config.idle_heartbeat = ORDERED_IDLE_HEARTBEAT;
                }
            }

            config
        };

        // We need a shared sid between the preprocessor and the actual subscription object so that
        // the subscription can be redirected as needed.
        let shared_sid = Arc::new(AtomicU64::new(0));

        // Sequences for gap detection
        let sequence_pair = Arc::new(Mutex::new(SequencePair {
            consumer_seq: 0,
            stream_seq: 0,
        }));

        // Figure out our deliver subject
        let deliver_subject = maybe_consumer_info
            .as_ref()
            .and_then(|consumer_info| consumer_info.config.deliver_subject.clone())
            .or_else(|| consumer_config.deliver_subject.clone())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Other,
                    "must use pull subscribe to bind to pull based consumer",
                )
            })?;

        let preprocessor = SubscriptionPreprocessor {
            sequence_pair: sequence_pair.clone(),
            context: self.clone(),
            consumer_config: consumer_config.clone(),
            stream_name: stream_name.clone(),
            shared_sid: shared_sid.clone(),
        };
        // Create a subscription with that subject.
        let (mut sid, mut receiver) = self
            .connection
            .0
            .client
            .subscribe_with_preprocessor(
                &deliver_subject,
                maybe_queue,
                Box::pin(preprocessor.clone()),
            )
            .await?;

        // If we don't have a consumer yet we try to create one here.
        // If that fails try to bind once before giving up.
        let (consumer_info, consumer_ownership) = match maybe_consumer_info {
            Some(consumer_info) => (consumer_info, ConsumerOwnership::No),
            None => match self.add_consumer(&stream_name, &consumer_config).await {
                Ok(consumer_info) => (consumer_info, ConsumerOwnership::Yes),
                Err(err) => {
                    // We won't be using this subscription, remove it.
                    self.connection.0.client.unsubscribe(sid).await?;

                    // If this isn't a protocol error we can return without trying anything else.
                    if err.kind() != io::ErrorKind::Other {
                        return Err(err);
                    }

                    if let Some(inner) = err.into_inner() {
                        if let Ok(err) = inner.downcast::<Error>() {
                            if err.error_code() != ErrorCode::ConsumerNameExist {
                                return Err(io::Error::new(io::ErrorKind::Other, err));
                            }
                        }
                    }

                    let consumer_name = maybe_consumer_name
                        .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "stream not found"))?;

                    let consumer_info = self
                        .consumer_info(&stream_name, &consumer_name)
                        .await
                        .and_then(process_consumer_info)?;

                    let deliver_subject = consumer_info
                        .config
                        .deliver_subject
                        .as_ref()
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::Other,
                                "must use pull subscribe to bind to pull based consumer",
                            )
                        })?;

                    // Create a new subscription with the new subject.
                    let (new_sid, new_receiver) = self
                        .connection
                        .0
                        .client
                        .subscribe_with_preprocessor(
                            deliver_subject,
                            maybe_queue,
                            Box::pin(preprocessor),
                        )
                        .await?;

                    sid = new_sid;
                    receiver = new_receiver;

                    (consumer_info, ConsumerOwnership::No)
                }
            },
        };

        shared_sid.store(sid, Ordering::Relaxed);

        Ok(PushSubscription::new(
            shared_sid,
            consumer_info,
            consumer_ownership,
            receiver,
            self.clone(),
        ))
    }

    /// Create a `JetStream` stream.
    pub async fn add_stream<S>(&self, stream_config: S) -> io::Result<StreamInfo>
    where
        StreamConfig: From<S>,
    {
        let config: StreamConfig = stream_config.into();
        if config.name.is_empty() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            ));
        }
        let subject: String = format!("{}STREAM.CREATE.{}", self.api_prefix(), config.name);
        let req = serde_json::ser::to_vec(&config)?;
        self.js_request(&subject, &req).await
    }

    /// Update a `JetStream` stream.
    pub async fn update_stream(&self, config: &StreamConfig) -> io::Result<StreamInfo> {
        if config.name.is_empty() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            ));
        }
        let subject: String = format!("{}STREAM.UPDATE.{}", self.api_prefix(), config.name);
        let req = serde_json::ser::to_vec(&config)?;
        self.js_request(&subject, &req).await
    }

    /// List all `JetStream` stream names. If you also want stream information,
    /// use the `list_streams` method instead.
    pub fn stream_names(&self) -> impl Stream<Item = io::Result<String>> + '_ {
        Box::pin(PagedIterator {
            subject: format!("{}STREAM.NAMES", self.api_prefix()),
            manager: self,
            offset: 0,
            items: Default::default(),
            done: false,
        })
        .to_stream()
    }

    async fn stream_name_by_subject(&self, subject: &str) -> io::Result<String> {
        let req = serde_json::ser::to_vec(&StreamNamesRequest {
            subject: subject.to_string(),
        })?;

        let request_subject = format!("{}STREAM.NAMES", self.api_prefix());
        self.js_request::<StreamNamesResponse>(&request_subject, &req)
            .await
            .map(|res| res.streams.first().unwrap().to_string())
    }

    /// List all `JetStream` streams.
    pub fn list_streams(&self) -> impl Stream<Item = io::Result<StreamInfo>> + '_ {
        Box::pin(PagedIterator {
            subject: format!("{}STREAM.LIST", self.api_prefix()),
            manager: self,
            offset: 0,
            items: Default::default(),
            done: false,
        })
        .to_stream()
    }

    /// List `JetStream` consumers for a stream.
    pub fn list_consumers<S>(
        &self,
        stream: S,
    ) -> io::Result<impl Stream<Item = io::Result<ConsumerInfo>> + '_>
    where
        S: AsRef<str>,
    {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            ));
        }
        let subject: String = format!("{}CONSUMER.LIST.{}", self.api_prefix(), stream);

        Ok(Box::pin(PagedIterator {
            subject,
            manager: self,
            offset: 0,
            items: Default::default(),
            done: false,
        })
        .to_stream())
    }

    /// Query `JetStream` stream information.
    pub async fn stream_info<S: AsRef<str>>(&self, stream: S) -> io::Result<StreamInfo> {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            ));
        }
        let subject: String = format!("{}STREAM.INFO.{}", self.api_prefix(), stream);
        self.js_request(&subject, b"").await
    }

    /// Purge `JetStream` stream messages.
    pub async fn purge_stream<S: AsRef<str>>(&self, stream: S) -> io::Result<PurgeResponse> {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            ));
        }
        let subject = format!("{}STREAM.PURGE.{}", self.api_prefix(), stream);
        self.js_request(&subject, b"").await
    }

    /// Purge stream messages matching a subject.
    pub async fn purge_stream_subject<S: AsRef<str>>(
        &self,
        stream: S,
        filter_subject: &str,
    ) -> io::Result<PurgeResponse> {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            ));
        }

        let subject = format!("{}STREAM.PURGE.{}", self.api_prefix(), stream);
        let request = serde_json::to_vec(&PurgeRequest {
            filter: Some(filter_subject.to_string()),
            ..Default::default()
        })?;

        self.js_request(&subject, &request).await
    }

    /// Get a message from a stream.
    pub async fn get_message<S: AsRef<str>>(
        &self,
        stream: S,
        seq: u64,
    ) -> io::Result<StreamMessage> {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            ));
        }

        let subject = format!("{}STREAM.MSG.GET.{}", self.api_prefix(), stream);
        let request = serde_json::ser::to_vec(&StreamMessageGetRequest {
            seq: Some(seq),
            last_by_subject: None,
        })?;

        let raw_message = self
            .js_request::<StreamMessageGetResponse>(&subject, &request)
            .await
            .map(|response| response.message)?;

        let message = StreamMessage::try_from(raw_message)?;

        Ok(message)
    }

    /// Get the last message from a stream by subject
    pub async fn get_last_message<S: AsRef<str>>(
        &self,
        stream_name: S,
        stream_subject: &str,
    ) -> io::Result<StreamMessage> {
        let stream_name: &str = stream_name.as_ref();
        if stream_name.is_empty() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            ));
        }

        let subject = format!("{}STREAM.MSG.GET.{}", self.api_prefix(), stream_name);
        let request = serde_json::ser::to_vec(&StreamMessageGetRequest {
            seq: None,
            last_by_subject: Some(stream_subject.to_string()),
        })?;

        let raw_message = self
            .js_request::<StreamMessageGetResponse>(&subject, &request)
            .await
            .map(|response| response.message)?;

        let message = StreamMessage::try_from(raw_message)?;

        Ok(message)
    }

    /// Delete message in a `JetStream` stream.
    pub async fn delete_message<S: AsRef<str>>(
        &self,
        stream: S,
        sequence_number: u64,
    ) -> io::Result<bool> {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            ));
        }

        let req = serde_json::ser::to_vec(&DeleteRequest {
            seq: sequence_number,
        })
        .unwrap();

        let subject = format!("{}STREAM.MSG.DELETE.{}", self.api_prefix(), stream);

        self.js_request::<DeleteResponse>(&subject, &req)
            .await
            .map(|dr| dr.success)
    }

    /// Delete `JetStream` stream.
    pub async fn delete_stream<S: AsRef<str>>(&self, stream: S) -> io::Result<bool> {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            ));
        }

        let subject = format!("{}STREAM.DELETE.{}", self.api_prefix(), stream);
        self.js_request::<DeleteResponse>(&subject, b"")
            .await
            .map(|dr| dr.success)
    }

    /// Create a `JetStream` consumer.
    pub async fn add_consumer<S, C>(&self, stream: S, config: C) -> io::Result<ConsumerInfo>
    where
        S: AsRef<str>,
        ConsumerConfig: From<C>,
    {
        let config = ConsumerConfig::from(config);
        let stream = stream.as_ref();
        if stream.is_empty() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            ));
        }

        let subject = if let Some(ref durable_name) = config.durable_name {
            format!(
                "{}CONSUMER.DURABLE.CREATE.{}.{}",
                self.api_prefix(),
                stream,
                durable_name
            )
        } else {
            format!("{}CONSUMER.CREATE.{}", self.api_prefix(), stream)
        };

        let req = CreateConsumerRequest {
            stream_name: stream.into(),
            config,
        };

        let ser_req = serde_json::ser::to_vec(&req)?;
        self.js_request(&subject, &ser_req).await
    }

    /// Delete a `JetStream` consumer.
    pub async fn delete_consumer<S, C>(&self, stream: S, consumer: C) -> io::Result<bool>
    where
        S: AsRef<str>,
        C: AsRef<str>,
    {
        let stream = stream.as_ref();
        if stream.is_empty() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            ));
        }
        let consumer = consumer.as_ref();
        if consumer.is_empty() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "the consumer name must not be empty",
            ));
        }

        let subject = format!(
            "{}CONSUMER.DELETE.{}.{}",
            self.api_prefix(),
            stream,
            consumer
        );

        self.js_request::<DeleteResponse>(&subject, b"")
            .await
            .map(|dr| dr.success)
    }

    /// Query `JetStream` consumer information.
    pub async fn consumer_info<S, C>(&self, stream: S, consumer: C) -> io::Result<ConsumerInfo>
    where
        S: AsRef<str>,
        C: AsRef<str>,
    {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            ));
        }
        let consumer: &str = consumer.as_ref();
        let subject: String = format!("{}CONSUMER.INFO.{}.{}", self.api_prefix(), stream, consumer);
        self.js_request(&subject, b"").await
    }

    /// Query `JetStream` account information.
    pub async fn account_info(&self) -> io::Result<AccountInfo> {
        self.js_request(&format!("{}INFO", self.api_prefix()), b"")
            .await
    }

    async fn js_request<Res>(&self, subject: &str, req: &[u8]) -> io::Result<Res>
    where
        Res: DeserializeOwned,
    {
        let res_msg = self.connection.request(subject, req).await?;
        let res: ApiResponse<Res> = serde_json::de::from_slice(&res_msg.data)?;
        match res {
            ApiResponse::Ok(stream_info) => Ok(stream_info),
            ApiResponse::Err { error, .. } => {
                log::error!(
                    "failed to parse API response: {:?}",
                    std::str::from_utf8(&res_msg.data)
                );

                Err(io::Error::new(io::ErrorKind::Other, error))
            }
        }
    }

    fn api_prefix(&self) -> &str {
        &self.options.api_prefix
    }
}

/// Creates a new `JetStream` context using the given `Connection` and default options.
///
pub fn new(nc: Connection) -> JetStream {
    JetStream::new(nc, JetStreamOptions::default())
}
