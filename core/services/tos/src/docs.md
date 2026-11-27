## Capabilities

Depending on its configuration and the backing system, this service can expose:

- [ ] create_dir
- [x] stat
- [x] read
- [x] write
- [x] delete
- [x] list
- [x] copy
- [ ] rename
- [x] presign

Inspect the effective capability set with [`opendal_core::Operator::info`] and
[`opendal_core::OperatorInfo::capability`] after building an operator.

## Configuration

Use [`crate::TosConfig`] for serializable configuration and this builder's
methods for direct construction. The field and method documentation defines
accepted values, defaults, and environment interaction.

## Restore

Enable versioning on the TOS bucket before deleting objects you intend to
recover. `Operator::restore` leaves a live object as is. It lists versions of
the requested key and removes its current delete marker by version ID when a
recoverable data version exists. Each call removes one marker. A key with no
live or recoverable data version returns `NotFound`. The caller needs permission
to list object versions and delete the selected marker.

`Operator::restore_with(...).version(...)` uses TOS CopyObject to create a new
current version from the selected source version. The caller needs permission
to read the source version and write the destination. TOS CopyObject limits
apply. A restore with a version and `if_not_exists(true)` returns `Unsupported`.

If TOS fails to confirm the mutation, check the current state before retrying.
The request may already have removed a marker or created a version.

## Write checksums

Signed non-append writes accept `WriteChecksum::Any` and `WriteChecksum::Fast`.
OpenDAL verifies the CRC-64/XZ reported by TOS in `x-tos-hash-crc64ecma`
and returns it as `ChecksumAlgo::Crc64Ecma`. Multipart writes verify each part
and compare the CRC of the ordered parts with the completion receipt.

If OpenDAL receives a successful write status and cannot confirm the receipt,
it returns a non-temporary `Unexpected` error. The object may already be
visible. `Algo`, `Expected`, and checksum verification for presigned writes are
unsupported.

## Example

### Via Builder

```rust,no_run
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_tos::Tos;

fn main() -> Result<()> {
    let builder = Tos::default()
        // Set the root for TOS, all operations will happen under this root.
        .root("/path/to/dir")
        // Set the bucket name. This is required.
        .bucket("test")
        // Set the endpoint.
        //
        // For example:
        // - "https://tos-cn-beijing.volces.com"
        // - "https://tos-cn-shanghai.volces.com"
        .endpoint("https://tos-cn-beijing.volces.com")
        // Set the region.
        .region("cn-beijing")
        // Set the access_key_id and secret_access_key.
        //
        // OpenDAL will try to load credentials from the environment if
        // credentials are not set explicitly.
        .access_key_id("access_key_id")
        .secret_access_key("secret_access_key");

    let _op: Operator = Operator::new(builder)?;

    Ok(())
}
```
