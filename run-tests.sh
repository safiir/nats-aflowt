#!/bin/sh

# Run tests
# 
# Usage examples:
#    ./run-tests.sh                # run all tests
#    ./run-tests.sh tls            # run unit tests with 'tls' in their name
#    ./run-tests.sh --doc          # run all doc tests
#    ./run-tests.sh --doc Option   # run all doc tests for Option

# if a nats-server is not running on 127.0.0.1:14222, start one in docker

TEST_NAME="${@:---all}"

if nc -czt -w1 127.0.0.1 14222; then
  cargo test $TEST_NAME -- --nocapture --color always  --show-output
else
  docker rm -f nats-local-test
  docker run --pull=always --rm -d --name nats-local-test -p 127.0.0.1:14222:4222 nats:2.6 -js
  cargo test $TEST_NAME -- --nocapture --color always
  docker stop nats-local-test
fi

