## Capabilities

Depending on its configuration and the backing system, this service can expose:

- [ ] ~~create_dir~~
- [x] stat
- [x] read
- [x] write
- [x] delete
- [x] list (standalone connections)
- [ ] ~~copy~~
- [ ] ~~rename~~
- [ ] ~~presign~~

Inspect the effective capability set with [`opendal_core::Operator::info`] and
[`opendal_core::OperatorInfo::capability`] after building an operator.

## Configuration

Use [`crate::RedisConfig`] for serializable configuration and this builder's
methods for direct construction. The field and method documentation defines
accepted values, defaults, and environment interaction.

## Listing

Standalone connections support prefix and recursive listing through `SCAN`.
Non-recursive listing groups matching nested keys into directories. Recursive
listing returns stored files and directory markers. The list `limit` is a scan
count hint, and Redis may return more or fewer keys per request. Listing does
not provide a snapshot when keys change during the scan.

Use `op.list_with("/").glob("movreg_*_832917")` to find matching keys with
OpenDAL's default `GlobLayer`. A Redis account with restricted command access
needs `SCAN`, `TYPE`, and `STRLEN` to list entries.

Redis Cluster connections do not support listing because scan cursors are
local to each node.

## Redis Cluster replica reads

With `cluster_endpoints` configured, set `read_from_replicas` to `true` to route
read-only commands for `read` and `stat` to replicas when available. The default
is `false`, which routes these commands to the primary. Writes go to the primary.

Replication lag can cause stale results. For example, `read` or `stat` can
briefly return `ErrorKind::NotFound` for a just-written key. OpenDAL returns
a replica miss without retrying on the primary.

## Expiration

A write with `expires` overrides `default_ttl` for that key. Redis applies
the expiration in the same command that stores the value, rounding positive
durations up to a whole millisecond. Omit `default_ttl` to disable expiration
for writes without an explicit `expires`. Both durations must be positive.

## Example

### Via Builder

```rust,no_run
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_redis::Redis;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Redis::default();

    // this will build a Operator accessing Redis which runs on tcp://localhost:6379
    let op: Operator = Operator::new(builder)?;
    Ok(())
}
```
