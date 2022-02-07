This is an unofficial fork and is Alpha status

# 0.16.104

- updated nkeys dependency to 0.2.0
- prevent panic if stream shutdown called while there are holders of the connection

# 0.16.103

- Now builds and passes clippy on stable (1.57) and nightly (1.60).

### Fixes

 - got rid of all async closures which prevented builds in stable.
   When using `with_async_handler`, the syntax is:
      with_async_handler( move |m| async move { .... } )
 - worked around rustc compiler bug in 1.57 (trait lifetimes);
 - removed unwrap() from PushSubscription::process

### changed most tests to use local nats-server instead of demo.nats.io

  - tests on `demo.nats.io` started failing with an error about too many streams,
    so switched to running tests locally.
  - The script `./run-tests.sh` starts a nats-server (in docker) with jetstream
    on 127.0.0.1:14222, runs all tests, then stops the container.
    It doesn't use port 4222 because some of the tests start a nats-server on that
    port.
  - This adds a dependency on `nc` and `docker`
  - Accordingly, most occurrences of 'demo.nats.io' in the
  tests have been changed to 127.0.0.1:14222. 

### test cleanup and fixes

  - minor cleanup for some tests in subscription, and added some
  assertions
  - fixed bug in doc test for PushSubscription::next_timeout
  (wasn't adding stream before calling subscribe)
  - fixed hang in subscription.drain() test by changing next() to
  next_timeout()

### test failures

  - I'm seeing intermittent test hangs in some jetstream doc tests for
  ObjectStore, KV, and PushSubscription.
  - On most runs, all tests pass. Sometimes there's a single failure,
  but each time the test that fails is different. These are the ones
  I've seen:
    - jststream::PushSubscription::process
    - jststream::PushSubscription::close
    - jststream::PushSubscription::unsubscribe
    - jststream::PushSubscription::with_process_handler
    - jetstream::ObjectStore::delete_object_store
    - jetstream::ObjectStore::get
    - jetstream::ObjectStore::put
    - jetstream::kv::Store::history
  - since kv and object_store are marked 'unstable' in the original crate,
  I don't know how much to worry about these.

# 0.16.102

  - changed crate references in tests and examples from nats:: to nats_aflowt::
  - fixed paths in Cargo.toml and README.md

# 0.16.101 

  - merged prs 287-292
  - all functionality current as of nats.rs d22329c (main branch, jan 7, 2022)
  - added tests/request_multi.rs

# 0.16.x

- ported to full async
  - obsoletes asynk package
  - currently has dependencies on tokio runtime
  - can run all async tasks in a single thread (subscriptions and 
    connections do not spawn new threads)
- in the port to async, some apis have changed:
  - most public functions have been changed to be `async`
  - all functions that used to return an `Iterator` now return `Stream`
    - wherever there was an explicit `iter(..)` method to create an
    iterator, it's been replaced with `stream(..)`.
  - jetstream `PagedIterator` replaced with `Stream`
  - error callbacks must now implement a trait, instead of being a
  simple closure. See doc comments and examples folder for examples.
  - added `Subscription.with_async_handler` to take async closure
    - (`Subscription.with_handler` still takes sync closure)
- some tests added
- upgraded to rust 2021 edition

- Status notes
  - two known test failures: in jetstream:kv and jetstream:object_store

