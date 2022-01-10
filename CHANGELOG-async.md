This is an unofficial fork and is Alpha status

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

