- Proposal Name: `retryable_error`
- Start Date: 2022-04-12
- RFC PR: [datafuselabs/opendal#0000](https://github.com/datafuselabs/opendal/pull/0000)
- Tracking Issue: [datafuselabs/opendal#0000](https://github.com/datafuselabs/opendal/issues/0000)

# Summary

Treat `io::ErrorKind::Interrupt` as retryable error.

# Motivation

Supports retry make our users' lives easier:

> [Feature request: Custom retries for the s3 backend](https://github.com/datafuselabs/opendal/issues/196)
>
> While the reading/writing from/to s3, AWS occasionally returns errors that could be retried (at least 5xx?). Currently, in the databend, this will fail the whole execution of the statement (which may have been running for an extended time).

Most users may need this retry feature, like `decompress`. Implementing it in OpenDAL will make users no bother on backoff logic.

# Guide-level explanation

With `retry` feature enabled:

```toml
opendal = {version="0.5.2", features=["retry"]}
```

Users can configure the retry behavior easily:

```rust
let backoff = ExponentialBackoff::default();
let op = op.with_backoff(backoff);
```

All requests sent by `op` will be automatically retried.

# Reference-level explanation

TBD

# Drawbacks

TBD

# Rationale and alternatives

TBD

# Prior art

None

# Unresolved questions

- `write` can't be retried.

# Future possibilities

None
