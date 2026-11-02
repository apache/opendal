#!/usr/bin/env bash
set -euo pipefail

# Task-only evidence collection; this driver is not part of a product PR.
task_base=3f76ae484ff32e57051497641fb0f63aea5081db
task_log=${1:?Pass a new evidence directory}
task_project=opendal-redis-readiness-20261102
task_repo=$(git rev-parse --show-toplevel)
test ! -e "$task_log"
mkdir -p "$task_log"
exec 9>/tmp/opendal-pr-delivery-20261031-build.lock
flock 9
test -z "$(git status --porcelain)"
git rev-parse HEAD >"$task_log/transport.head"
sha256sum "$0" >"$task_log/driver.sha256"
git checkout --detach "$task_base"
git rev-parse HEAD HEAD^{tree} >"$task_log/product.head-tree"
test -z "$(git status --porcelain)"
cd "$task_repo/core"
export CARGO_BUILD_JOBS=4 CARGO_TERM_COLOR=never
unset CARGO_TARGET_DIR RUST_TEST_THREADS
rustc --version >"$task_log/rust.version"
cargo --version >"$task_log/cargo.version"
nproc >"$task_log/available-cpus"
cargo test --locked --test behavior --features tests,services-redis --no-run --message-format=json \
    >"$task_log/build.jsonl" 2>"$task_log/build.log"
task_test_bin=$(python3 - "$task_log/build.jsonl" <<'PY'
import json, sys
bins = []
for line in open(sys.argv[1]):
    item = json.loads(line)
    if (item.get('target') or {}).get('name') == 'behavior' and item.get('executable'):
        bins.append(item['executable'])
assert len(set(bins)) == 1, bins
print(bins[-1])
PY
)
cd "$task_repo"
task_compose=(docker compose -p "$task_project" -f fixtures/redis/docker-compose-redis-cluster-tls.yml)
test -z "$(docker ps -aq --filter "label=com.docker.compose.project=$task_project")"
docker network ls -q | xargs -r docker network inspect >"$task_log/networks-before.json"
python3 - "$task_log/networks-before.json" <<'PY'
import ipaddress, json, socket, sys
target = ipaddress.ip_network('172.30.0.0/16')
for network in json.load(open(sys.argv[1])):
    for config in (network.get('IPAM') or {}).get('Config') or []:
        if config.get('Subnet') and ':' not in config['Subnet']:
            assert not target.overlaps(ipaddress.ip_network(config['Subnet'])), network['Name']
for port in range(6380, 6386):
    with socket.socket() as probe:
        probe.bind(('0.0.0.0', port))
PY
task_resources_created=false
cleanup() {
    if "$task_resources_created"; then
        "${task_compose[@]}" down --volumes >"$task_log/cleanup.log" 2>&1
    fi
}
trap cleanup EXIT
redis_node() {
    local task_node=$1
    shift
    "${task_compose[@]}" exec -T -e REDISCLI_AUTH=opendal "redis-node-$task_node" \
        redis-cli --tls --cacert /etc/redis/ssl/ca.crt --raw "$@"
}
snapshot() {
    local task_phase=$1 task_dir=$2 task_node
    for task_node in {0..5}; do
        {
            date -u +%FT%T.%NZ
            redis_node "$task_node" CLUSTER INFO
            redis_node "$task_node" CLUSTER NODES
            redis_node "$task_node" CLUSTER SLOTS
            redis_node "$task_node" INFO replication
            redis_node "$task_node" INFO stats
            redis_node "$task_node" INFO keyspace
        } >"$task_dir/$task_phase-node-$task_node.log" 2>&1
    done
}
export OPENDAL_TEST=redis
export OPENDAL_REDIS_CLUSTER_ENDPOINTS=rediss://127.0.0.1:6380/,rediss://127.0.0.1:6381/,rediss://127.0.0.1:6382/,rediss://127.0.0.1:6383/,rediss://127.0.0.1:6384/,rediss://127.0.0.1:6385/
export OPENDAL_REDIS_ROOT=/test/opendal OPENDAL_REDIS_DB=0 OPENDAL_REDIS_PASSWORD=opendal
unset OPENDAL_DISABLE_RANDOM_ROOT OPENDAL_TEST_CAPABILITY_OVERRIDES OPENDAL_TEST_IO_TIMEOUT
export SSL_CERT_FILE="$task_repo/fixtures/redis/ssl/ca.crt"
export RUST_LOG=opendal=debug,redis=trace RUST_BACKTRACE=1
for task_round in 1 2 3; do
    task_round_log="$task_log/cold-$task_round"
    mkdir -p "$task_round_log"
    task_resources_created=true
    "${task_compose[@]}" up -d --wait --wait-timeout 60 >"$task_round_log/up.log" 2>&1
    docker image inspect docker.io/bitnamilegacy/redis-cluster:7.4.3-debian-12-r0 \
        --format '{{json .RepoDigests}} {{json .Config.Healthcheck}}' >"$task_round_log/image.txt"
    for task_node in {0..5}; do
        task_container=$("${task_compose[@]}" ps -q "redis-node-$task_node")
        docker inspect --format '{{.State.Health.Status}}' "$task_container" \
            >"$task_round_log/node-$task_node.health"
    done
    # Observe startup concurrently so topology inspection does not delay the test.
    (trap - EXIT; snapshot startup "$task_round_log" || true) &
    task_snapshot_pid=$!
    date -u +%FT%T.%NZ >"$task_round_log/test-start.txt"
    set +e
    "$task_test_bin" >"$task_round_log/tests.log" 2>&1
    task_test_exit=$?
    set -e
    printf '%s\n' "$task_test_exit" >"$task_round_log/tests.exit"
    wait "$task_snapshot_pid" || true
    snapshot after "$task_round_log" || true
    "${task_compose[@]}" logs --no-color --timestamps >"$task_round_log/server.log" 2>&1
    for task_node in {0..5}; do
        redis_node "$task_node" --scan >"$task_round_log/node-$task_node.keys" 2>&1 || true
    done
    "${task_compose[@]}" down --volumes >"$task_round_log/down.log" 2>&1
    task_resources_created=false
done
git rev-parse HEAD HEAD^{tree} >"$task_log/post.head-tree"
git status --porcelain >"$task_log/post.status"
printf 'Evidence collection completed; inspect each test footer and exit file.\n'
