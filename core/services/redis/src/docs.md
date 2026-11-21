## Capabilities

Depending on its configuration and the backing system, this service can expose:

- [ ] ~~create_dir~~
- [x] stat
- [x] read
- [x] write
- [x] delete
- [ ] ~~list~~
- [ ] ~~copy~~
- [ ] ~~rename~~
- [ ] ~~presign~~

Inspect the effective capability set with [`opendal_core::Operator::info`] and
[`opendal_core::OperatorInfo::capability`] after building an operator.

## Configuration

Use [`crate::RedisConfig`] for serializable configuration and this builder's
methods for direct construction. The field and method documentation defines
accepted values, defaults, and environment interaction.

## Per-write expiration

A write with `expires` overrides `default_ttl` for that key. Redis applies
the expiration in the same command that stores the value, rounding positive
durations up to a whole millisecond.

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
