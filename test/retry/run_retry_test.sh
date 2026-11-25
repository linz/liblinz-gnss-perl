#!/bin/bash
# Tests retrying data center requests, reused HTTP connections and the transfer statistics,
# in a Docker image with liblinz-gnss-perl's dependencies and python3, using the modules in
# this source tree.  A stub HTTP server (stub_server.py) runs in the same container, so no
# network access is needed.
#
# Usage: run_retry_test.sh image

set -e
image=${1:?Usage: run_retry_test.sh image}
srcdir=$(cd "$(dirname "$0")/../.." && pwd)

docker run --rm --network none -e PERL5LIB=/src/lib -e TEST_LOG_LEVEL -v "$srcdir:/src:ro" \
    --entrypoint sh "$image" -c '
        python3 -u /src/test/retry/stub_server.py > /tmp/stub.port &
        for i in $(seq 50); do [ -s /tmp/stub.port ] && break; sleep 0.1; done
        perl /src/test/retry/retry_test.pl "$(cat /tmp/stub.port)"'
