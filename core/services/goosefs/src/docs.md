## Capabilities

Depending on its configuration and the backing system, this service can expose:

- [x] create_dir
- [x] stat
- [x] read
- [x] write
- [x] delete
- [x] list
- [ ] copy
- [x] rename
- [x] rename (if_not_exists)
- [ ] presign

Inspect the effective capability set with [`opendal_core::Operator::info`] and
[`opendal_core::OperatorInfo::capability`] after building an operator.

## Notes

GooseFS service uses native gRPC protocol (not REST API like Alluxio),
which means it connects directly to GooseFS Master (port 9200) and
Worker (port 9203) without requiring a Proxy component.

Features:
- **HA support**: Comma-separated master addresses for automatic Primary Master discovery.
- **Block-level I/O**: Data reads/writes go through block-level gRPC bidirectional streaming.
- **Consistent hash routing**: Worker selection uses consistent hashing on block IDs.
- **All WriteTypes**: Supports MUST_CACHE, CACHE_THROUGH, THROUGH, and ASYNC_THROUGH.
- **Conditional Create**: `write_with_if_not_exists` publishes via Master no-replace
  rename (`rename_with_if_not_exists`); destination is never deleted on the Create path.
- **Rename parent directories**: `rename` creates a missing destination parent only
  when Master reports it missing. Write-via-temp publish does not call
  `CreateDirectory` for a parent that `CreateFile(recursive)` already created.

## Configuration

Use [`crate::GoosefsConfig`] for serializable configuration and this builder's
methods for direct construction. The field and method documentation defines
accepted values, defaults, and environment interaction.

### Master address resolution

`build()` resolves the master addresses from three sources, highest priority
first:

1. the `GOOSEFS_MASTER_ADDR` environment variable, either a comma-separated
   list or the SDK's `gfs://h1:9200,h2:9200/root` URI form;
2. `goosefs.master.rpc.addresses` or `goosefs.master.hostname` in
   `goosefs-site.properties`, discovered through `$GOOSEFS_CONFIG_FILE`,
   `$GOOSEFS_HOME/conf`, `~/.goosefs`, and `/etc/goosefs`. `goosefs-sdk` 0.1.9
   documents `$GOOSEFS_CONF_DIR` as a search path but does not read it, so use
   `$GOOSEFS_CONFIG_FILE` to point at a file outside those directories;
3. the `master_addr` config key, which also receives the URI authority of
   `goosefs://host:port/path`.

A site file that declares masters outranks `master_addr` because the file
carries a deployment's whole HA master list, which a single URI authority
cannot express. Set `GOOSEFS_MASTER_ADDR` to override a deployed site file for
one process. `build()` fails with `ConfigInvalid` when no source supplies an
address; it never falls back to `127.0.0.1:9200`.

| `goosefs-site.properties` | `GOOSEFS_MASTER_ADDR` | `master_addr` / URI authority | Master addresses used |
| --- | --- | --- | --- |
| declares masters | set | any | `GOOSEFS_MASTER_ADDR` |
| declares masters | unset | any or absent | site file |
| absent, or no master keys | set | any | `GOOSEFS_MASTER_ADDR` |
| absent, or no master keys | unset | set | `master_addr` |
| absent, or no master keys | unset | absent | none — `ConfigInvalid` |

## Example

### Via Builder

```rust,no_run
use opendal::Operator;
use opendal::Result;
use opendal::services::GooseFs;

#[tokio::main]
async fn main() -> Result<()> {
    // Single master
    let builder = GooseFs::default()
        .root("/data")
        .master_addr("10.0.0.1:9200");

    let op: Operator = Operator::new(builder)?;

    Ok(())
}
```

### Via URI

```rust,no_run
use opendal::Operator;
use opendal::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let op = Operator::from_uri("goosefs://10.0.0.1:9200/data")?;
    Ok(())
}
```

### HA Mode

```rust,no_run
use opendal::Operator;
use opendal::Result;
use opendal::services::GooseFs;

#[tokio::main]
async fn main() -> Result<()> {
    let builder = GooseFs::default()
        .root("/data")
        .master_addr("10.0.0.1:9200,10.0.0.2:9200,10.0.0.3:9200")
        .write_type("cache_through");

    let op: Operator = Operator::new(builder)?;

    Ok(())
}
```

### With Authentication

```rust,no_run
use opendal::Operator;
use opendal::Result;
use opendal::services::GooseFs;

#[tokio::main]
async fn main() -> Result<()> {
    // SIMPLE authentication (default) with custom username
    let builder = GooseFs::default()
        .root("/data")
        .master_addr("10.0.0.1:9200")
        .auth_type("simple")
        .auth_username("myuser");

    let op: Operator = Operator::new(builder)?;

    // No authentication (NOSASL mode)
    let builder = GooseFs::default()
        .root("/data")
        .master_addr("10.0.0.1:9200")
        .auth_type("nosasl");

    let op: Operator = Operator::new(builder)?;

    Ok(())
}
```

## Testing

This service is covered by all three OpenDAL test layers:

1. **Unit tests** (no cluster required) — exercise `Config`/`Builder`/error-mapping
   boundaries. Run:

   ```shell
   cargo test -p opendal-service-goosefs
   ```

2. **Behavior tests** (require a running GooseFS cluster) — the shared
   `core/tests/behavior` suite (`read`/`write`/`list`/`stat`/`rename`/`delete`/
   `create_dir`). Start the fixture and point the harness at it:

   ```shell
   # Start a single-container GooseFS (master + worker; see start-default.sh)
   docker compose -f fixtures/goosefs/docker-compose-goosefs.yml up -d --wait

   OPENDAL_TEST=goosefs \
   OPENDAL_GOOSEFS_MASTER_ADDR=127.0.0.1:9200 \
   OPENDAL_GOOSEFS_ROOT=/ \
   cargo test behavior --features tests,services-goosefs

   docker compose -f fixtures/goosefs/docker-compose-goosefs.yml down
   ```

   If `OPENDAL_TEST` is unset the behavior suite automatically skips, so
   missing a cluster is safe.

3. **GitHub CI** — `.github/services/goosefs/goosefs/action.yml` is picked up
   automatically by `.github/scripts/test_behavior/plan.py::provided_cases()`
   and runs the fixture + behavior suite on every PR. The fixture image
   (`ghcr.io/tencent/tencent-goosefs-rust-sdk/goosefs:v2.0.0`) is public so no secrets
   are needed.

The fixture also exposes a `distributed` compose profile (separate master /
worker / job_master / job_worker containers) for multi-node diagnostics;
opt in via `--profile distributed`.

