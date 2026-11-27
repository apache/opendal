## Capabilities

Depending on its configuration and the backing system, this service can expose:

- [ ] create_dir
- [x] stat
- [x] read
- [x] write
- [x] delete
- [x] restore
- [x] list
- [x] copy
- [ ] rename
- [x] presign

Inspect the effective capability set with [`opendal_core::Operator::info`] and
[`opendal_core::OperatorInfo::capability`] after building an operator.

## Checksum format

Tencent Cloud COS reports an unsigned decimal checksum named CRC64-ECMA in
`x-cos-hash-crc64ecma`. Alibaba Cloud OSS uses the same checksum in
`x-oss-hash-crc64ecma`. OpenDAL represents it as `ChecksumAlgo::Crc64Ecma`.

CRC-64/XZ uses the canonical polynomial `0x42F0E1EBA9EA3693`, with
`refin = true`, `refout = true`, and `init` and `xorout` both set to
`0xFFFFFFFFFFFFFFFF`. The check value for the ASCII bytes `123456789` is
`0x995DC9BBDF1939FA`. CRC-64/ECMA-182 (for example, `crc::CRC_64_ECMA_182`)
yields `0x6C40DF5F0B497347` for the same input and is incompatible with
these service digests.

## Configuration

Use [`crate::CosConfig`] for serializable configuration and this builder's
methods for direct construction. The field and method documentation defines
accepted values, defaults, and environment interaction.

## Example

### Via Builder

```rust,no_run
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_cos::Cos;

#[tokio::main]
async fn main() -> Result<()> {
    // create backend builder
    let mut builder = Cos::default()
        // set the storage bucket for OpenDAL
        .bucket("test")
        // set the endpoint for OpenDAL
        .endpoint("https://cos.ap-singapore.myqcloud.com")
        // Set the access_key_id and secret_access_key.
        //
        // OpenDAL will try load credential from the env.
        // If credential not set and no valid credential in env, OpenDAL will
        // send request without signing like anonymous user.
        .secret_id("secret_id")
        .secret_key("secret_access_key");

    let op: Operator = Operator::new(builder)?;

    Ok(())
}
```

## Copy

COS can copy large files in server-side parts. Set `chunk` on
`Operator::copy_with` or `Operator::copier_with` to select a part size. COS
accepts parts from 1 MiB to 5 GiB, with a smaller final part, and allows up to
10,000 parts. Smaller files use Copy Object. A multipart copy fails if the
source ETag changes between parts. Multipart copy needs permission to read the
source object's tags (`cos:GetObjectTagging`), which COS stores on the destination
when the upload completes.

Copies with `if_not_exists` use Copy Object, which supports this condition in
buckets that have never enabled versioning.

## Restore

Restore requires a bucket with versioning enabled. `Operator::restore` lists
object versions and removes the current delete marker, requiring permission to
list versions and delete specific versions. It succeeds without changing an
already-live object. Each call removes only one current marker; repeated
deletions can require repeated restore calls. An unknown path returns `NotFound`.

`Operator::restore_with(...).version(...)` copies the selected historical version
to the same path as a new current version. It requires read access to the source
version and write access to the destination. Conditional restore with
`if_not_exists` is not supported.

COS version listings can briefly retain a deleted marker. If the listed marker
no longer exists, restore returns a temporary error rather than reporting that
another deletion was restored. Use `RetryLayer` to retry after the listing updates.
