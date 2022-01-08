## Experimental port of rust NATS client to rust async

_Don't get blocked by asynk - stay aflowt_

The nats::asynk module is an async veneer over blocking io
and consumes a thread-per-connection, and, depending on how you use
subscriptions, an additional thread per subscription handler.
This package uses real async throughout the client. All connections
and subscriptions are non-blocking and may safely share the same thread,
or, optionally, a thread pool.

This package is not endorsed by the Nats team.
I wrote it as an experiment after running into some limitations with
`ratsio`, another nats async client.

### :loudspeaker: Warning

> :warning: This should only be used in environments where software is expected to hang, consume all CPU, destroy data, and explode at random intervals.

### Status (Jan 8, 2022)

  - Feature parity with rust nats client (including jetstream, tls,
  etc.) as of commit d22329c (main branch, jan 7, 2022)

  - Tested on linux x86_64. 
    All tests pass except for two: (jetstream:kv and jetstream:object_store),
    which are currently disabled by a feature flag.

  - apis are subject to change

### Changes from nats.rs

  - can run all async tasks in a single thread (subscriptions and 
    connections do not spawn new threads)
  - in the port to async, many apis have changed:
    - most public functions have been changed to be `async`
    - all functions that used to return an `Iterator` now return `futures::Stream`
    - wherever there was an explicit `iter(..)` method to create an
      iterator, it's been replaced with `stream(..)`.
    - jetstream `PagedIterator` replaced with `Stream`
    - error callbacks must now implement a trait, instead of being a
      simple closure. See doc comments and examples folder for examples.
    - added `Subscription.with_async_handler` to take async closure
      - (`Subscription.with_handler` still takes sync closure)
  - jetstream kv and object_store functions are enabled in this crate
    in the default feature set to support testing. 
    (In `nats.rs` they are behind an "unstable" feature flag
    and should be considered unstable here).
  - some tests have been added
  - upgraded to rust 2021 edition


This crate currently depends on the tokio runtime. I suspect
if the Nats team does develop a pure async rust client, they will
attempt to make it runtime agnostic.



