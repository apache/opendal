# Enable Server Side Encryption

OpenDAL has native support for server side encryption.

## S3

NOTE: they can't be enabled at the same time.

Enable `SSE-KMS` with aws managed KMS key:

```rust
{{#include ../../examples/s3-sse-kms-aws.rs:15:}}
```

Enable `SSE-KMS` with customer managed KMS key:

```rust
{{#include ../../examples/s3-sse-kms-customer.rs:15:}}
```

Enable `SSE-S3`:

```rust
{{#include ../../examples/s3-sse-s3.rs:15:}}
```

Enable `SSE-C`:

```rust
{{#include ../../examples/s3-sse-c.rs:15:}}
```
